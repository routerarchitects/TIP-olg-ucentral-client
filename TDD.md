# Test-Driven Development (TDD) Specification

This document details the test plans, test cases, and verification strategies for each phase of development of the uCentral Client (`olg-ucentral-client`).

---

## Epic 1: Scaffold & Base Types

### PR 1.1: Shared Contracts & Serialization Tests
*   **TC-CON-001 (Envelope Serialization):**
    *   *Requirement Mapping:* `REQ-004` (Subject Schema Versioning)
    *   *Setup:* Create instances of `ConfigureCommand`, `ActionCommand`, and `ResultEnvelope`.
    *   *Input:* `ActionCommand` with `Action = "reboot"`, `RPCID = "123"`.
    *   *Assert:* Marshalling to JSON must produce exact keys `version`, `rpc_id`, `target`, `command_type`, `action`, `payload`, `timestamp`.
*   **TC-CON-002 (Error Mappings):**
    *   *Requirement Mapping:* `REQ-021` (JSON-RPC Error Mapping)
    *   *Setup:* Pass internal error enum `ErrServiceUnavailable` to JSON-RPC error encoder helper.
    *   *Assert:* Encoder must output JSON-RPC error payload with `code = -32603` (Internal Error) and `data.application_code` equal to `3` (Local Service Unavailable).
*   **TC-CON-003 (Version Negotiation Fallback):**
    *   *Requirement Mapping:* `REQ-003` (Version Negotiation Fallback)
    *   *Setup:* Initiate a mock WebSocket connection offering only `v2` protocol, while the client is configured for `v1` only.
    *   *Assert:* Client must transition to `Degraded` state, remain connected for health reporting, and return `local_service_unavailable` (JSON-RPC code -32603, application_code 3) for configuration/action commands.
*   **TC-VAL-001 (Permissive Parameter Validation):**
    *   *Requirement Mapping:* `REQ-005` (Permissive Parameter Validation)
    *   *Setup:* Submit a configuration payload containing a known schema property with an invalid type, and an unknown future schema property.
    *   *Assert:* Schema validator must reject the request due to the invalid known parameter. Submit another payload containing only valid known parameters and unknown future parameters; the validator must pass the unknown parameters through to NATS unmodified.
*   **TC-CON-004 (Payload Sizing Constraints Checks):**
    *   *Requirement Mapping:* `REQ-020` (Sizing Constraints)
    *   *Setup:* Generate payloads of varying sizes for Configuration, State, Telemetry, and Logs. Send payloads exceeding their respective limits (10MB, 1MB, 256KB, 64KB).
    *   *Assert:* Client must reject/discard the oversized payloads and increment corresponding drop/error metrics. Payloads within limits must be successfully processed.
*   **TC-CON-005 (JSON-RPC ID Preservation):**
    *   *Requirement Mapping:* `REQ-027` (JSON-RPC ID Preservation)
    *   *Setup:* Send valid JSON-RPC requests containing `ID` as an integer (e.g., `42`) and `ID` as a string (e.g., `"42"`).
    *   *Assert:* The client must successfully parse both formats, track them through the transaction manager, and return the exact matching original format (numeric or string) in the JSON-RPC response without mutation.

---

## Epic 2: Traffic Queues & Priority Scheduler

### PR 2.1: Priority Outbound Scheduler Tests
*   **TC-SCH-001 (Priority Outbound Ordering):**
    *   *Requirement Mapping:* `REQ-014` (Outbound Priority Scheduler)
    *   *Setup:* Instantiate `PriorityScheduler` with a capacity of 10 and an emergency capacity of 100. Push 5 messages of `PriorityLow` (Priority 3). Push 1 message of `PriorityHighest` (Priority 0).
    *   *Assert:* Calling `Next()` must return the `PriorityHighest` message first. Subsequent calls must return `PriorityLow` messages in FIFO order.
*   **TC-SCH-002 (Scheduler Blocking and Wakeup):**
    *   *Requirement Mapping:* `REQ-014` (Outbound Priority Scheduler)
    *   *Setup:* Call `Next()` on an empty `PriorityScheduler` in a separate goroutine.
    *   *Assert:* Goroutine must block. Push a message into the scheduler from the main thread; goroutine must unblock and receive the message.
*   **TC-SCH-003 (Priority 0 Bounded Emergency Queue):**
    *   *Requirement Mapping:* `REQ-014`
    *   *Setup:* Instantiate `PriorityScheduler` with per-priority capacity and a bounded emergency limit for Priority 0. Block the consumer to simulate a stalled WebSocket writer. Push Priority 0 messages until the emergency limit is reached.
    *   *Assert:* `Push()` must return an explicit overflow error once the Priority 0 emergency limit is exhausted. Queue growth must remain bounded.
*   **TC-SCH-004 (Non-Blocking Priority 1 Overflow):**
    *   *Requirement Mapping:* `REQ-014`
    *   *Setup:* Instantiate `PriorityScheduler`. Fill the Priority 1 queue to maximum capacity. Attempt to push one more Priority 1 message from a separate goroutine.
    *   *Assert:* The `Push()` call must return immediately with a fast error and must **not** block the calling goroutine.

### PR 2.2: Buffers, Coalescers & Ring Buffer Tests
*   **TC-BUF-001 (Telemetry Ring Buffer FIFO Drop):**
    *   *Requirement Mapping:* `REQ-015` (State Coalescer & Telemetry Ring Buffer)
    *   *Setup:* Instantiate `TelemetryRingBuffer` with capacity = 5. Push 5 messages. Push 6th message.
    *   *Assert:* The 6th push must return `dropped = true`. The 1st pushed message must be discarded. The buffer size must remain 5.
*   **TC-BUF-002 (State Coalescing last-write-wins):**
    *   *Requirement Mapping:* `REQ-015` (State Coalescer & Telemetry Ring Buffer)
    *   *Setup:* Write State Report A (`"uptime": 10`). Write State Report B (`"uptime": 20`) to `StateCoalescer`.
    *   *Assert:* `Flush()` must return State Report B. The coalescer must be empty after flush.
*   **TC-BUF-003 (NATS Dispatch Buffer Busy Rejection):**
    *   *Requirement Mapping:* `REQ-012` (Command Dispatch Buffer)
    *   *Setup:* Instantiate `NATSDispatchBuffer` with capacity = 2. Push 2 messages. Push 3rd message.
    *   *Assert:* The 3rd push must return a queue full error immediately (does not block).
*   **TC-QUE-001 (Telemetry Throttling on Full Results Queue):**
    *   *Requirement Mapping:* `REQ-013` (Command Result Priority Queue)
    *   *Setup:* Fill the NATS command result queue (capacity 50) to 90% capacity. Send telemetry events.
    *   *Assert:* The client must throttle/delay telemetry forwarding to prioritize command results, ensuring core loops do not block.
*   **TC-BUF-004 (Gzip Compression Trigger Threshold):**
    *   *Requirement Mapping:* `REQ-024` (Payload Compression)
    *   *Setup:* Set `compression_threshold_bytes` to 2048. Generate a payload of size 1024 bytes and another of size 3072 bytes.
    *   *Assert:* The 1024-byte payload must be sent uncompressed. The 3072-byte payload must be gzipped before WebSocket transmission.
*   **TC-BUF-006 (Command Result Queue Non-Blocking):**
    *   *Requirement Mapping:* `REQ-013` (Command Result Priority Queue)
    *   *Setup:* Fill the NATS command result queue (capacity 50) to maximum capacity. Attempt to publish execution results from downstream agent loops.
    *   *Assert:* Outbound WebSocket writes or telemetry delays must not block the core NATS listener loops, ensuring execution results are processed asynchronously and independently.
*   **TC-BUF-007 (Outbound Rate Limiting, Drop Metrics & Coalescing):**
    *   *Requirement Mapping:* `REQ-015` (State Coalescer & Telemetry Ring Buffer)
    *   *Setup:* Push 60 telemetry events within 1 second. Push 2 state updates within 5 seconds.
    *   *Assert:* Outbound scheduler must rate-limit telemetry to 50 events/second (dropping 10 events) and state reports to 1 per 10 seconds. Verify that dropped events correctly increment the `dropped_by_reason` metric map using the standardized keys `rate_limited`, `queue_full`, and `cloud_disconnected` as applicable.

---

## Epic 3: Request Manager & Caching

### PR 3.1: Transaction State Machine & Manager Tests
*   **TC-RM-001 (State Machine Transitions):**
    *   *Requirement Mapping:* `REQ-007` (Transaction Lifecycle)
    *   *Setup:* Create a transaction using `CreateTransaction(rpcID = "tx-1", timeout = 10s)`.
    *   *Assert:* Initial state must be `TxCreated`. Manually advance state to `TxPendingNATS`, then `TxInFlight`. Verify correct enum states.
*   **TC-RM-002 (Concurrency Rejection):**
    *   *Requirement Mapping:* `REQ-008` (Concurrency Serialization)
    *   *Setup:* Start a transaction with `isStateChanging = true` for `rpc_id = "tx-1"`. Submit another transaction with `rpc_id = "tx-2"`, `isStateChanging = true`.
    *   *Assert:* The second transaction request must return a `busy` error immediately.
*   **TC-RM-003 (Parallel Read Operations):**
    *   *Requirement Mapping:* `REQ-008` (Concurrency Serialization)
    *   *Setup:* Start state-changing transaction `rpc_id = "tx-1"`. Submit read-only command transaction `rpc_id = "query-1"`, `isStateChanging = false`.
    *   *Assert:* Transaction `query-1` must succeed and run in parallel (no busy error).
*   **TC-UPG-001 (Asynchronous Upgrade Progress Stream):**
    *   *Requirement Mapping:* `REQ-011` (Asynchronous Upgrade Tracking)
    *   *Setup:* Start an upgrade transaction.
    *   *Assert:* Client must immediately return an initial "started" status response and close the initial JSON-RPC request-reply exchange, while the background upgrade operation remains active and logs continue to flow over NATS to the Cloud until a terminal state is reached.
*   **TC-UPG-002 (Upgrade Crash Recovery via Startup Query):**
    *   *Requirement Mapping:* `REQ-011`
    *   *Setup:* Simulate a daemon crash/restart while an upgrade is active downstream. On boot, the daemon queries the downstream device status.
    *   *Assert:* The daemon must detect the active upgrade from the status report, immediately re-acquire the in-memory `activeStateTx` lock, and correctly reject any new state-changing commands (e.g., `reboot`) until the upgrade completes.

### PR 3.2: Duplicate Attachment & Cache TTL Tests
*   **TC-RM-004 (Duplicate Active Request Rejection):**
    *   *Requirement Mapping:* `REQ-009` (Duplicate Active Request Rejection)
    *   *Setup:* Start transaction `rpc_id = "tx-1"`. Submit another request with matching `rpc_id = "tx-1"`.
    *   *Assert:* The second request must fail immediately and return a busy error (`-32603`). The original transaction must continue execution unaffected.
*   **TC-RM-005 (Operation-Specific Cache TTLs):**
    *   *Requirement Mapping:* `REQ-010` (Operation-Specific Caching & TTL)
    *   *Setup:* Write `configure` (TTL 5 mins), `reboot` (TTL 10 mins), `factory` (TTL 30 mins), and `upgrade` (TTL 60 mins) results to `TransactionCache`. Mock clock time to advance 15 minutes.
    *   *Assert:* Cache lookups for `configure` and `reboot` must return `false` (expired). Lookups for `factory` and `upgrade` must return `true` (cached).
*   **TC-RM-006 (Transaction Retry Policy & Backoff):**
    *   *Requirement Mapping:* `REQ-025` (Transaction Retry Policy)
    *   *Setup:* Mock the NATS responder to return transient errors. Submit a read-only request (`capabilities.get`) and a state-changing request (`configure`).
    *   *Assert:* The read-only request must be retried up to 3 times with exponential backoff (e.g., first retry after ~2s, second retry after ~4s) before failing. The state-changing request must fail fast on the first error with no retries.

---

## Epic 4: Network & Transport Clients

### PR 4.1: WebSocket Client Tests
*   **TC-NET-001 (Randomized Reconnect Backoff):**
    *   *Requirement Mapping:* `REQ-002` (Reconnection State Machine)
    *   *Setup:* Instantiate reconnection backoff loops. Simulate connection drops.
    *   *Assert:* Reconnect delays must fall within exponential bounds (e.g. attempt 2 delay is between `3.6s` and `4.8s` given base `4s` and `10-20%` randomized jitter).

### PR 4.2: NATS Integration Client Tests
*   **TC-NET-003 (JetStream KV Revision Guard & Trigger Contract):**
    *   *Requirement Mapping:* `REQ-006` (JetStream KV Consistency Contract)
    *   *Setup:* Write config payload to JetStream KV. Retrieve the sequence revision and publish the `config.apply` NATS trigger. Intercept the serialized trigger. Then, simulate a downstream agent processing the trigger under two conditions: (A) when the KV store revision exactly matches the trigger `kv_revision`, and (B) when the KV store contains a newer, higher revision payload.
    *   *Assert:* The intercepted trigger must contain `uuid`, `kv_key`, `kv_revision`, `target`, and `rpc_id` while strictly omitting the full configuration `payload`. In condition A (exact match), the simulated agent must successfully download and apply the configuration. In condition B (mismatch), the agent must explicitly abort the apply process, completely fulfilling the consistency contract.
*   **TC-SEC-001 (Target Subject Isolation Constraints):**
    *   *Requirement Mapping:* `REQ-016` (NATS Security & Target Isolation)
    *   *Setup:* Attempt to publish or subscribe to a subject with a different target serial (e.g. `ucentral.v1.device.different-serial.state`).
    *   *Assert:* Connection/authorization must block or reject the operation, ensuring target-serial isolation.
*   **TC-NET-007 (NATS-Native Health Check & Status Endpoints):**
    *   *Requirement Mapping:* `REQ-019` (NATS-Native Health Reporting)
    *   *Setup:* Publish a query to the `status.get` subject.
    *   *Assert:* The client must respond with a health snapshot containing daemon uptime, metrics, liveness ("ok"), and readiness status ("ready" when connected, "degraded" if NATS or Cloud is down).
*   **TC-SEC-002 (TLS v1.3 and CA Verification):**
    *   *Requirement Mapping:* `REQ-023` (TLS v1.3 Security)
    *   *Setup:* Configure the NATS client to connect to a broker without TLS or with an invalid CA cert.
    *   *Assert:* Client must fail to connect and reject the connection attempt. Configure with a valid CA cert and TLS v1.3; the connection must succeed.


### PR 4.3: Dynamic Capabilities & Sockets Tests
*   **TC-NET-005 (Unix Socket Refresh Trigger):**
    *   *Requirement Mapping:* `REQ-017` (Local Management Signal Security)
    *   *Setup:* Run Unix socket listener. Write a refresh command into the socket.
    *   *Assert:* Capabilities cache callback must be invoked. Socket file permission must be validated as root-only.
*   **TC-NET-006 (Audit Log Loop Prevention):**
    *   *Requirement Mapping:* `REQ-018` (Audit Logging & Loop Prevention)
    *   *Setup:* Force NATS publish failures during audit log writes.
    *   *Assert:* Client increments `audit_delivery_failure` but does not trigger recursive log writes.
*   **TC-NET-008 (Capability Retrieval & Caching Lifecycle):**
    *   *Requirement Mapping:* `REQ-022` (Capability Caching & Lifecycle)
    *   *Setup:* Start the client with NATS and the downstream responder initially unavailable. Verify retry backoff. Bring NATS and the responder online. Trigger a subsequent NATS reconnect event.
    *   *Assert:* The client must retry capability retrieval with bounded backoff until successful. Once the cache is successfully populated, no new fetch must be triggered on subsequent NATS reconnect events. Simulate a local Unix socket capabilities refresh command; the capabilities must be updated.
*   **TC-NET-010 (Connection State Machine Transitions):**
    *   *Requirement Mapping:* `REQ-002` (Reconnection State Machine)
    *   *Setup:* Instantiate connection state machine. Simulate transitions: `Offline` -> `ConnectingBoth` -> `Operational` -> `Degraded`.
    *   *Assert:* State machine must compile and transition correctly through all four connection states, invoking status callbacks and reporting correct degraded statuses.
*   **TC-NET-011 (Unix Socket Rate Limiting & Auditing):**
    *   *Requirement Mapping:* `REQ-017` (Local Management Signal Security), `REQ-018` (Audit Logging & Loop Prevention)
    *   *Setup:* Send 10 capability refresh requests to the Unix socket in 1 second. Trigger sensitive actions (`reboot`, `factory`, `upgrade`).
    *   *Assert:* The Unix socket listener must rate-limit and reject excess refresh requests. Sensitive actions must successfully emit high-severity audit logs to the Cloud.
*   **TC-NET-012 (Syslog-Triggered Capability Refreshes):**
    *   *Requirement Mapping:* `REQ-022` (Capability Caching & Lifecycle)
    *   *Setup:* Input a syslog message indicating a firmware version change, and a NATS message indicating an upgrade reboot log.
    *   *Assert:* Both trigger events must invalidate the capability cache and launch a new downstream NATS capability discovery query.

---

## Epic 5: Main Entry Point & Assembly

### PR 5.1: Main Loop Tests
*   **TC-INT-001 (Graceful Teardown):**
    *   *Requirement Mapping:* `REQ-001` (Concurrent Startup Loops)
    *   *Setup:* Boot main client. Send `SIGTERM` signal.
    *   *Assert:* Client must gracefully flush scheduler queues, close WebSocket connections, close NATS, and terminate process with exit code 0.

### PR 5.2: End-to-End Integration Tests
*   **TC-INT-002 (Config Sync and Rollback Flow):**
    *   *Requirement Mapping:* `REQ-006` (KV Consistency), `REQ-021` (Error Mapping)
    *   *Setup:* Push configuration update from mock WebSocket server. Downstream agent returns rollback result.
    *   *Assert:* Client writes KV config, triggers `config.apply` NATS command, receives `rolled_back` reply, and returns JSON-RPC error `code = -32603` (Internal Error) with `error.data.application_code = 5` (Rollback Completed) containing the active config UUID inside the `error.data` payload.
*   **TC-INT-003 (Concurrent Startup Loops and Independent Connections):**
    *   *Requirement Mapping:* `REQ-001` (Concurrent Startup Loops)
    *   *Setup:* Start the daemon with NATS connection blocked (unreachable broker) but Cloud WebSocket reachable.
    *   *Assert:* Daemon must successfully establish connection to the Cloud WebSocket and report status as "Degraded" (due to NATS being offline) without hanging or blocking on the NATS connection loop.
*   **TC-INT-004 (Priority 0 Overflow Recovery):**
    *   *Requirement Mapping:* `REQ-013`, `REQ-014`
    *   *Setup:* Simulate a stalled WebSocket writer while the daemon continues generating Priority 0 responses.
    *   *Assert:* The daemon treats the WebSocket writer path as unhealthy and triggers recovery, fails affected transactions, and increments the overflow metric instead of allowing unbounded memory growth.

---

## Epic 6: Requirements Traceability Matrix

| Requirement ID | Requirement Name | Mapping Test Case(s) |
| :--- | :--- | :--- |
| **REQ-001** | Concurrent Startup Loops | `TC-INT-001`, `TC-INT-003` |
| **REQ-002** | Reconnection State Machine | `TC-NET-001`, `TC-NET-010` |
| **REQ-003** | Version Negotiation Fallback | `TC-CON-003` |
| **REQ-004** | Subject Schema Versioning | `TC-CON-001` |
| **REQ-005** | Permissive Parameter Validation | `TC-VAL-001` |
| **REQ-006** | JetStream KV Consistency Contract | `TC-NET-003`, `TC-INT-002` |
| **REQ-007** | Transaction Lifecycle | `TC-RM-001` |
| **REQ-008** | Concurrency Serialization | `TC-RM-002`, `TC-RM-003` |
| **REQ-009** | Duplicate Active Request Rejection | `TC-RM-004` |
| **REQ-010** | Operation-Specific Caching & TTL | `TC-RM-005` |
| **REQ-011** | Asynchronous Upgrade Tracking & Crash Recovery | `TC-UPG-001`, `TC-UPG-002` |
| **REQ-012** | Command Dispatch Buffer | `TC-BUF-003` |
| **REQ-013** | Command Result Priority Queue | `TC-QUE-001`, `TC-BUF-006`, `TC-INT-004` |
| **REQ-014** | WebSocket Outbound Priority Scheduler | `TC-SCH-001`, `TC-SCH-002`, `TC-SCH-003`, `TC-SCH-004`, `TC-INT-004` |
| **REQ-015** | State Coalescer & Telemetry Ring Buffer | `TC-BUF-001`, `TC-BUF-002`, `TC-BUF-007` |
| **REQ-016** | NATS Security & Target Isolation | `TC-SEC-001` |
| **REQ-017** | Local Management Signal Security | `TC-NET-005`, `TC-NET-011` |
| **REQ-018** | Audit Logging & Loop Prevention | `TC-NET-006`, `TC-NET-011` |
| **REQ-019** | NATS-Native Health Reporting | `TC-NET-007` |
| **REQ-020** | Sizing Constraints | `TC-CON-004` |
| **REQ-021** | JSON-RPC Error Mapping | `TC-CON-002`, `TC-INT-002` |
| **REQ-022** | Capability Caching & Lifecycle | `TC-NET-008`, `TC-NET-012` |
| **REQ-023** | TLS v1.3 Security | `TC-SEC-002` |
| **REQ-024** | Payload Compression | `TC-BUF-004` |
| **REQ-025** | Request Manager Retry Policy | `TC-RM-006` |
| **REQ-026** | Desired/Applied Cloud Reconciliation Contract | `N/A` |
| **REQ-027** | JSON-RPC ID Preservation | `TC-CON-005` |
