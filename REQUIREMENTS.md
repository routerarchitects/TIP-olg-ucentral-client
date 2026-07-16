# Requirements Specification: uCentral Client Daemon

This document lists the strict, numbered requirements for the Go-based uCentral Client daemon (`TIP-olg-ucentral-client`). All architectural designs, specifications, code implementations, and test suites must map directly back to these requirements.

---

## 1. Network Connectivity & Lifecycles

*   **REQ-001 (Concurrent Startup Loops):** The daemon must launch separate, independent, concurrent connection loops to NATS and the Cloud WebSocket at boot. A failure or delay in NATS connection must not block the Cloud connection, and vice versa.
*   **REQ-002 (Decoupled Connection State Machine):** The daemon must manage the Cloud and NATS connection lifecycles entirely independently. Their individual connection states (`Offline` $\rightarrow$ `Connecting` $\rightarrow$ `Connected`) must be evaluated continuously to form a composite Global State:
    *   `ProtocolNegotiating`: Cloud is `Connected`, NATS is `Connected`, but protocol negotiation is `NotStarted` or `InProgress`.
    *   `Operational`: Cloud is `Connected`, NATS is `Connected`, and protocol negotiation is `Ready`.
    *   `CloudDegraded`: Cloud is `Offline`/`Connecting`, but NATS is `Connected`. (Daemon safely buffers telemetry locally; reconnects to Cloud with randomized exponential backoff of 2s-300s).
    *   `NATSDegraded`: NATS is `Offline`/`Connecting`, but Cloud is `Connected`. (Daemon fast-fails incoming Cloud commands with `local_service_unavailable`).
    *   `Offline`: Both Cloud and NATS are `Offline` or `Connecting`.
    *   `ProtocolFailure`: Cloud is connected but protocol negotiation failed (Version Mismatch). This state takes strict precedence over `NATSDegraded`; if negotiation fails, the state is `ProtocolFailure` regardless of NATS connection status.
    *   No daemon restart is allowed for connection recovery on either network layer.
*   **REQ-003 (Version Negotiation Fallback):** During connection handshake, if the Cloud and client share no common major protocol version (e.g., Cloud is v2-only, client is v1-only), the client must fall back to the `ProtocolFailure` global state. In this state, it remains connected for health reporting only and rejects all other commands with `local_service_unavailable` (JSON-RPC code -32603, application_code 3).

---

## 2. NATS & JetStream Schema

*   **REQ-004 (Subject Schema Versioning):** All NATS subjects used by the client must be versioned with a `v1` prefix and follow target-serial isolation boundaries:
    *   `ucentral.v1.device.<own-serial>.config.apply` (Request-Reply)
    *   `ucentral.v1.device.<own-serial>.action.<command>` (Request-Reply)
    *   `ucentral.v1.device.<own-serial>.state` (Pub-Sub)
    *   `ucentral.v1.device.<own-serial>.telemetry` (Pub-Sub)
    *   `ucentral.v1.device.<own-serial>.log` (Pub-Sub)
    *   `ucentral.v1.device.<own-serial>.health` (Pub-Sub)
    *   `ucentral.v1.device.<own-serial>.capabilities.get` (Request-Reply)
    *   `ucentral.v1.device.<own-serial>.status.get` (Request-Reply, downstream device/local-agent status query)
*   **REQ-005 (Permissive Parameter Validation):** The client must validate incoming configuration schemas using a compiled-in schema validator. It must enforce **permissive validation**: known schema parameters are strictly validated, but unknown future parameters must be passed through to NATS to preserve forward compatibility.
*   **REQ-006 (JetStream KV Consistency Contract):** The client must write configurations to JetStream KV (`cfg_desired` bucket) under key `desired.<serial>`. The NATS configure trigger must carry the target `uuid`, `kv_key`, and the NATS `kv_revision` to allow the downstream agent to fetch the configuration and verify ordering. Downstream agents must abort if the KV revision is higher than the trigger revision, and must not apply without a matching trigger revision.

---

## 3. Transaction & Request Management

*   **REQ-007 (Transaction Lifecycle):** Every incoming Cloud command must be tracked by an active in-memory transaction transitioning through: `Created` $\rightarrow$ `PendingNATS` $\rightarrow$ `InFlight` $\rightarrow$ `Completed` / `Failed` / `TimedOut`.
*   **REQ-008 (Concurrency Serialization):** The Request Manager must serialize state-changing commands (`configure`, `reboot`, `factory`, `upgrade`) for the device. If a state-changing transaction is active (`PendingNATS` or `InFlight`), new state-changing commands must be immediately rejected with a `busy` status (Error Code -32603). Read-only downstream queries (`capabilities.get`, `status.get`) must run in parallel and must not acquire the device state lock. The `status.get` subject is owned by the downstream device/local agent; the uCentral client must only publish requests to it and must not respond on it.
*   **REQ-009 (Duplicate Active Request Rejection):** If a Cloud request arrives with a canonical Cloud JSON-RPC `id` matching any currently active transaction (`Created`, `PendingNATS`, or `InFlight`), the request must be rejected immediately, including after WebSocket reconnection, with a standard JSON-RPC busy/internal error (`-32603`) instead of attempting to run it or attach it to the running transaction.
*   **REQ-010 (Operation-Specific Caching & TTL):** The transaction cache must persist results in-memory with TTLs categorized by command type to support duplicate request replay and reconnection recovery:
    *   `configure`: 5 minutes
    *   `reboot`: 10 minutes
    *   `factory`: 30 minutes
    *   `upgrade` (Firmware): 60 minutes from completion
*   **REQ-011 (Asynchronous Upgrade Tracking & Crash Recovery):** Firmware upgrades must run as a background operation tracked by a persistent `operation_id` independent of the initial JSON-RPC `id`.
    *   **Generation and Storage:** The uCentral client MUST generate a globally unique `operation_id` (e.g., a UUID), store the upgrade record durably (in a separate `OperationStore`), and acquire the state-changing lock *before* dispatching the long-running upgrade command via NATS. Upgrade operation records remain in `OperationStore` while the operation is active and are marked terminal or removed after terminal processing is durably completed. The final Cloud-facing upgrade response remains in `TransactionCache` for 60 minutes from completion.
    *   **Agent Preservation:** The downstream upgrade agent MUST persist the received `operation_id` with its upgrade state before starting the upgrade. Every status response and upgrade result MUST return the same `operation_id`. If `active=true` or a long-running status is returned, the agent MUST include the `operation_id`.
    *   **Restart Recovery:** On startup, the uCentral client must load the active operation from the `OperationStore`, restore the state-changing lock, call downstream `status.get`, and correlate the response using `operation_id`. Each recovery `status.get` query MUST generate a fresh internal `correlation_id`; the persisted `operation_id` remains the durable identity used to associate the status response with the original upgrade operation. If the agent reports `active=true` but omits the `operation_id`, the client MUST treat it as an indeterminate recovery error, retain the state lock to prevent new commands, and MUST NOT generate a replacement ID.
    *   **Cloud Re-association:** The durable record maps the original Cloud JSON-RPC `id` to the `operation_id`. After a restart and WebSocket reconnection, the client uses this mapping to send operation-status notifications back to the Cloud containing both the JSON-RPC `id` and the `operation_id`.

---

## 4. Queues & Outbound Traffic Scheduling

*   **REQ-012 (Command Dispatch Buffer):** The client must use a configurable, short-lived NATS dispatch buffer (default size: 100). If NATS is down or the buffer is full, incoming commands must fail fast with `local_service_unavailable` (JSON-RPC code -32603, application_code 3).
*   **REQ-013 (Command Result Priority Queue):** NATS command execution results must be processed through a bounded result queue (default size: 50). Because state-changing commands are serialized per device, this queue is not expected to fill during normal operation. The queue must never block core network or NATS subscriber loops. If it nears capacity, telemetry/log forwarding must be throttled. If the queue overflows, the daemon must treat it as an exceptional local result-delivery failure: record `command_result_overflow`, log the `correlation_id`, command type, and subject, and complete any matching Cloud transaction with an indeterminate local delivery error. The daemon must not silently drop correlated command results, must not wait for timeout when overflow is already known, and must not report that the downstream operation itself failed unless the downstream result explicitly says so.
*   **REQ-014 (WebSocket Outbound Priority Scheduler):** Outbound WebSocket traffic must be written via a priority scheduler:
    *   `Priority 0 (Highest)`: JSON-RPC responses. Bypasses lower-priority backlog but uses a dedicated bounded emergency queue. If exhausted, it triggers path recovery, fails affected transactions, and records an overflow metric.
    *   `Priority 1`: Audits, system crash logs, and health snapshots. To prevent blocking core NATS handler execution, `Push()` operations to Priority 1 must be non-blocking. If the queue reaches capacity, it must return a fast error and record an `audit_delivery_failure` metric instead of blocking the caller.
    *   `Priority 2`: Coalesced state metrics. If the scheduler queue is full, `Push()` must fail-fast. The producer must handle this by doing nothing; the state remains safely in the upstream `StateCoalescer` (via `Peek()`) so it can be re-attempted on the next tick with any newer state natively applied.
    *   `Priority 3 (Lowest)`: Telemetry events and standard logs. If the scheduler queue is full, `Push()` must fail-fast. The producer must drop the popped telemetry event and record a `dropped_by_reason.scheduler_full` metric, relying on the upstream `TelemetryRingBuffer` to accumulate new data.
    *   **Anti-Starvation Rule:** To prevent a flood of Priority 0 traffic from permanently starving telemetry and health, the scheduler must enforce a consecutive yield limit. After a maximum of 10 consecutive Priority 0 messages are yielded, the scheduler must forcefully yield at least one available message from the next highest populated queue (Priority 1, 2, or 3) before returning to Priority 0.
*   **REQ-015 (State Coalescer & Telemetry Ring Buffer):** 
    *   State statistics must be rate-limited to 1 message per 10s using a last-write-wins coalescer (newer reports overwrite older un-sent reports).
    *   Telemetry events must be rate-limited to 50/sec. On buffer overflow, the oldest events must be dropped (FIFO drop).
    *   The client must track drop counters via `dropped_by_reason` metrics.

---

## 5. Security & Observability

*   **REQ-016 (NATS Security & Target Isolation):** The daemon must connect using NKeys or JWT credentials and restrict its publish/subscribe permissions to subjects containing its `<own-serial>` only, and must explicitly include subscribe permissions for reply-inboxes (`_INBOX.>`) to support NATS request-reply flows.
*   **REQ-017 (Local Management Signal Security):** The local capability refresh trigger must be exposed as a Unix domain socket. Access must be restricted to root-only file permissions, and must be rate-limited and audit logged.
*   **REQ-018 (Audit Logging & Loop Prevention):** Every sensitive action (`reboot`, `factory`, `upgrade`) must generate a high-severity audit log forwarded to the Cloud. If forwarding fails, the client must increment `audit_delivery_failure` but must not generate another log, preventing recursive logging loops.
*   **REQ-019 (NATS-Native Health Reporting):** To maintain security and efficiency, the client must not expose HTTP ports. It must subscribe to device health snapshots on `ucentral.v1.device.<own-serial>.health` for Cloud forwarding. The uCentral client must not serve daemon liveness/readiness as a NATS responder on `ucentral.v1.device.<own-serial>.status.get`; that subject is reserved for downstream device/local-agent status queries. The daemon's own liveness, readiness, Cloud connectivity, NATS connectivity, queue depth, uptime, and local metrics must be tracked internally and reported to the Cloud through the WebSocket control path.

---

## 6. Sizing & Error Mapping

*   **REQ-020 (Sizing Constraints & Memory Protection):** Maximum uncompressed payload sizes must be strictly enforced: Configuration (10MB), State (1MB), Telemetry (256KB), and Logs (64KB). To prevent Out-Of-Memory (OOM) denial-of-service, limits must be enforced on the network read stream, using bounded readers or equivalent mechanisms, before full memory allocation or JSON unmarshalling occurs wherever possible. For compressed payloads, limits must apply to the decompressed byte stream, and decompression must stop once the applicable uncompressed limit is exceeded. Inbound and outbound compression handling must be treated separately; outbound compression must not allow payloads to bypass uncompressed size limits. If an inbound Cloud JSON-RPC request exceeds the limit before a valid JSON-RPC request object and `id` can be parsed, the daemon must close or reject the request and record an error metric. If a valid request object and `id` can be parsed but the payload exceeds the applicable method limit, the daemon must return JSON-RPC `-32602` (Invalid Params) with `error.data.application_code = 4` (Validation Failed).
*   **REQ-021 (JSON-RPC Error Mapping):** Internal and application errors must map to standard JSON-RPC 2.0 error codes. For application execution errors, the top-level error code must be `-32603` (Internal Error) and the specific application-level code must be returned in the `error.data` object under the `application_code` key:
    *   Standard JSON-RPC wire codes:
        *   `-32700` (Parse Error)
        *   `-32600` (Invalid Request)
        *   `-32601` (Method Not Found)
        *   `-32602` (Invalid Params)
        *   `-32603` (Internal / Busy Error)
    *   Application-specific subcodes carried in `error.data.application_code`:
        *   `1` (Application Error)
        *   `2` (Timeout)
        *   `3` (Local Service Unavailable)
        *   `4` (Validation Failed)
        *   `5` (Rollback Completed)
        *   `6` (Rollback Failed)
        *   `7` (Result Delivery Failed): The downstream operation may have completed, but the uCentral client could not process or deliver the result due to a local queue or transport failure. The operation outcome is indeterminate from the Cloud perspective.
*   **REQ-022 (Capability Caching & Lifecycle):** The daemon must populate the capability cache once after the first successful NATS connection and downstream capability responder availability. If initial retrieval fails due to broker/responder unavailability or timeout, the daemon must retry with bounded exponential backoff (e.g., base 2s, max 300s) until the initial cache is populated. After successful initialization, capabilities must not be automatically re-fetched on later NATS reconnect events. The cache must only be refreshed upon detecting a firmware version change, receiving a specific upgrade reboot log, or receiving a valid local management signal.
*   **REQ-023 (TLS v1.3 Security):** All NATS broker connections must enforce TLS v1.3 encryption with strict CA certificate verification configured using local CA paths. Plain text or insecure NATS connections must be rejected.
*   **REQ-024 (Payload Compression):** Outbound payloads exceeding a configurable compression threshold specified by the configuration file property `compression_threshold_bytes` (default: 2048 bytes / 2KB) must be compressed using gzip prior to WebSocket transmission.
*   **REQ-025 (Transaction Retry Policy):** The Request Manager must implement a strict transaction retry policy: only idempotent, read-only downstream queries (e.g., `capabilities.get`, `status.get`) are retryable for transient failures, using a randomized exponential backoff (base 2s, max 3 attempts). State-changing actions (`configure`, `reboot`, `factory`, `upgrade`) must fail fast without automatic retries. The `status.get` retry policy applies only to requests sent by the uCentral client to the downstream device/local agent.
*   **REQ-026 (Desired/Applied Cloud Reconciliation Contract):** If a desired configuration write to JetStream KV succeeds but publication of the corresponding `config.apply` trigger fails, the daemon must retain the desired configuration in KV and return a failure to the Cloud. Recovery and reconciliation of desired versus applied configuration state are owned by the Cloud control plane, not by the uCentral client.
*   **REQ-027 (JSON-RPC ID Preservation & Edge Cases):** The daemon must support both string and numeric types for JSON-RPC `id` fields per the JSON-RPC 2.0 specification. The exact Cloud JSON-RPC ID must be preserved in the Request Manager and final WebSocket response. NATS commands and replies must use only the generated internal `correlation_id`. The daemon must enforce the following edge cases:
    *   **Notifications:** Requests without an `id` must be executed, but the daemon must not send a JSON-RPC response. To prevent transaction collisions and ensure proper NATS correlation, locking, and timeout tracking, the Request Manager must generate a unique internal `correlation_id` (e.g., a UUID) for notifications while retaining a flag indicating `respondToCloud=false`. Null or empty strings must never be used as a transaction key.
    *   **Invalid/Malformed IDs:** Requests where `id` is an object, array, or malformed must be rejected immediately with `-32600` (Invalid Request) or `-32700` (Parse Error), and the error response must return `id: null`.
    *   **Reused IDs:** If a valid `id` matches a completed transaction in the Request Manager cache, the daemon must immediately replay the cached response without re-executing the command downstream.
*   **REQ-028 (NATS Envelope Serialization Contract):** Internal structures representing NATS commands and triggers must strictly serialize into the documented JSON schema keys (e.g., `version`, `correlation_id`, `command_type`, `action`) without mutation or field loss before transmission.
*   **REQ-029 (Graceful Teardown):** Upon receiving a SIGTERM or SIGINT signal, the daemon must attempt a graceful teardown with a strict bounded deadline of 5 seconds. During this window, Priority-0 transaction responses must receive preferential flushing. Remaining lower-priority messages are discarded if the deadline expires, or earlier if necessary to preserve the deadline for Priority-0 delivery. Upon successful queue depletion or expiration of the deadline, the daemon must force-close all WebSocket and NATS connections and terminate the process with exit code 0.
*   **REQ-030 (Startup Configuration Validation):** The daemon must strictly parse and validate all provided configuration parameters (including device serial, Cloud URL, NATS credentials, and queue capacities) prior to attempting any network connections. Missing mandatory fields, unsupported protocols (e.g., non-WSS or non-TLS URLs), missing TLS certificates, or zero/negative queue capacities must be treated as fatal validation errors. If validation fails, the daemon must log the exact validation failure, avoid starting any background network connection loops, and exit immediately with a non-zero exit status.
