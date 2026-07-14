# Requirements Specification: uCentral Client Daemon

This document lists the strict, numbered requirements for the Go-based uCentral Client daemon (`olg-ucentral-client`). All architectural designs, specifications, code implementations, and test suites must map directly back to these requirements.

---

## 1. Network Connectivity & Lifecycles

*   **REQ-001 (Concurrent Startup Loops):** The daemon must launch separate, independent, concurrent connection loops to NATS and the Cloud WebSocket at boot. A failure or delay in NATS connection must not block the Cloud connection, and vice versa.
*   **REQ-002 (Reconnection State Machine):** The daemon must manage connection lifecycles through four states: `Offline`, `ConnectingBoth`, `Operational`, and `Degraded`. 
    *   If the Cloud connection is lost, it must transition to `ConnectingBoth` and retry in the background with randomized exponential backoff (2s to 300s).
    *   No daemon restart is allowed for connection recovery.
*   **REQ-003 (Version Negotiation Fallback):** During connection handshake, if the Cloud and client share no common major protocol version (e.g., Cloud is v2-only, client is v1-only), the client must fall back to a Degraded state. In this state, it remains connected for health reporting only and rejects all other commands with `local_service_unavailable` (JSON-RPC code -32603, application_code 3).

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
    *   `ucentral.v1.device.<own-serial>.status.get` (Request-Reply)
*   **REQ-005 (Permissive Parameter Validation):** The client must validate incoming configuration schemas using a compiled-in schema validator. It must enforce **permissive validation**: known schema parameters are strictly validated, but unknown future parameters must be passed through to NATS to preserve forward compatibility.
*   **REQ-006 (JetStream KV Consistency Contract):** The client must write configurations to JetStream KV (`cfg_desired` bucket) under key `desired.<serial>`. The NATS configure trigger must carry the target `uuid`, `kv_key`, and the NATS `kv_revision` to allow the downstream agent to fetch the configuration and verify ordering. Downstream agents must abort if the KV revision is higher than the trigger revision, and must not apply without a matching trigger revision.

---

## 3. Transaction & Request Management

*   **REQ-007 (Transaction Lifecycle):** Every incoming Cloud command must be tracked by an active in-memory transaction transitioning through: `Created` $\rightarrow$ `PendingNATS` $\rightarrow$ `InFlight` $\rightarrow$ `Completed` / `Failed` / `TimedOut`.
*   **REQ-008 (Concurrency Serialization):** The Request Manager must serialize state-changing commands (`configure`, `reboot`, `factory`, `upgrade`) for the device. If a state-changing transaction is active (`PendingNATS` or `InFlight`), new state-changing commands must be immediately rejected with a `busy` status (Error Code -32603). Read-only commands (`capabilities.get`, `status.get`) must run in parallel.
*   **REQ-009 (Duplicate Active Request Rejection):** If a Cloud request arrives with an `rpc_id` that is already in-progress (`InFlight`), the Request Manager must reject the new request immediately with a standard JSON-RPC busy/internal error (`-32603`) instead of attempting to run it or attach it to the running transaction.
*   **REQ-010 (Operation-Specific Caching & TTL):** The transaction cache must persist results in-memory with TTLs categorized by command type:
    *   `configure`: 5 minutes
    *   `reboot`: 10 minutes
    *   `factory`: 30 minutes
    *   `upgrade` (Firmware): 60 minutes
*   **REQ-011 (Asynchronous Upgrade Tracking & Crash Recovery):** Firmware upgrades must run as a background operation. The initial `"started"` response must close the initial JSON-RPC request-reply exchange. The state-changing lock (`activeStateTx`) must remain held until terminal completion or failure of the upgrade. Upon WebSocket reconnection, the daemon must resume reporting the current upgrade state to the Cloud. To survive a daemon crash/restart, the daemon must query the downstream device's status on boot. If an upgrade is active, it must immediately re-acquire the `activeStateTx` lock. Any duplicate upgrade requests received while the background upgrade is active must be rejected immediately as busy.

---

## 4. Queues & Outbound Traffic Scheduling

*   **REQ-012 (Command Dispatch Buffer):** The client must use a configurable, short-lived NATS dispatch buffer (default size: 100). If NATS is down or the buffer is full, incoming commands must fail fast with `local_service_unavailable` (JSON-RPC code -32603, application_code 3).
*   **REQ-013 (Command Result Priority Queue):** NATS command execution results must be processed through a result queue (default size: 50). This queue must never block core network loops. If it nears capacity, telemetry/log forwarding must be throttled.
*   **REQ-014 (WebSocket Outbound Priority Scheduler):** Outbound WebSocket traffic must be written via a priority scheduler:
    *   `Priority 0 (Highest)`: JSON-RPC responses. Bypasses lower-priority backlog but uses a dedicated bounded emergency queue. If exhausted, it triggers path recovery, fails affected transactions, and records an overflow metric.
    *   `Priority 1`: Audits, system crash logs, and health snapshots. To prevent blocking core NATS handler execution, `Push()` operations to Priority 1 must be non-blocking. If the queue reaches capacity, it must return a fast error and record an `audit_delivery_failure` metric instead of blocking the caller.
    *   `Priority 2`: Coalesced state metrics.
    *   `Priority 3 (Lowest)`: Telemetry events and standard logs.
*   **REQ-015 (State Coalescer & Telemetry Ring Buffer):** 
    *   State statistics must be rate-limited to 1 message per 10s using a last-write-wins coalescer (newer reports overwrite older un-sent reports).
    *   Telemetry events must be rate-limited to 50/sec. On buffer overflow, the oldest events must be dropped (FIFO drop).
    *   The client must track drop counters via `dropped_by_reason` metrics.

---

## 5. Security & Observability

*   **REQ-016 (NATS Security & Target Isolation):** The daemon must connect using NKeys or JWT credentials and restrict its publish/subscribe permissions to subjects containing its `<own-serial>` only.
*   **REQ-017 (Local Management Signal Security):** The local capability refresh trigger must be exposed as a Unix domain socket. Access must be restricted to root-only file permissions, and must be rate-limited and audit logged.
*   **REQ-018 (Audit Logging & Loop Prevention):** Every sensitive action (`reboot`, `factory`, `upgrade`) must generate a high-severity audit log forwarded to the Cloud. If forwarding fails, the client must increment `audit_delivery_failure` but must not generate another log, preventing recursive logging loops.
*   **REQ-019 (NATS-Native Health Reporting):** To maintain security and efficiency, the client must not expose HTTP ports. It must publish health snapshots to `ucentral.v1.device.<own-serial>.health` and reply to readiness/liveness status queries on `ucentral.v1.device.<own-serial>.status.get`.

---

## 6. Sizing & Error Mapping

*   **REQ-020 (Sizing Constraints):** Maximum uncompressed JSON payload sizes must be strictly enforced: Configuration (10MB), State (1MB), Telemetry (256KB), Logs (64KB). Payloads exceeding these limits must be discarded.
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
*   **REQ-022 (Capability Caching & Lifecycle):** The daemon must populate the capability cache once after the first successful NATS connection and downstream capability responder availability. If initial retrieval fails due to broker/responder unavailability or timeout, the daemon must retry with bounded exponential backoff (e.g., base 2s, max 300s) until the initial cache is populated. After successful initialization, capabilities must not be automatically re-fetched on later NATS reconnect events. The cache must only be refreshed upon detecting a firmware version change, receiving a specific upgrade reboot log, or receiving a valid local management signal.
*   **REQ-023 (TLS v1.3 Security):** All NATS broker connections must enforce TLS v1.3 encryption with strict CA certificate verification configured using local CA paths. Plain text or insecure NATS connections must be rejected.
*   **REQ-024 (Payload Compression):** Outbound payloads exceeding a configurable compression threshold specified by the configuration file property `compression_threshold_bytes` (default: 2048 bytes / 2KB) must be compressed using gzip prior to WebSocket transmission.
*   **REQ-025 (Transaction Retry Policy):** The Request Manager must implement a strict transaction retry policy: only idempotent, read-only queries (e.g., `capabilities.get`, `status.get`) are retryable for transient failures, using a randomized exponential backoff (base 2s, max 3 attempts). State-changing actions (`configure`, `reboot`, `factory`, `upgrade`) must fail fast without automatic retries.
*   **REQ-026 (Desired/Applied Cloud Reconciliation Contract):** If a desired configuration write to JetStream KV succeeds but publication of the corresponding `config.apply` trigger fails, the daemon must retain the desired configuration in KV and return a failure to the Cloud. Recovery and reconciliation of desired versus applied configuration state are owned by the Cloud control plane, not by the uCentral client.
*   **REQ-027 (JSON-RPC ID Preservation):** The daemon must support both string and numeric types for JSON-RPC `id` fields per the JSON-RPC 2.0 specification. The exact raw type and representation of the `id` must be preserved across internal parsing, transaction manager mapping, NATS correlation, and final WebSocket response rendering.
*   **REQ-028 (NATS Envelope Serialization Contract):** Internal structures representing NATS commands and triggers must strictly serialize into the documented JSON schema keys (e.g., `version`, `rpc_id`, `command_type`, `action`) without mutation or field loss before transmission.

