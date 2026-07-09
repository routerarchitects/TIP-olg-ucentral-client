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
    *   *Assert:* Encoder must output JSON-RPC error payload with `code = -32603` (Internal Error) and data code payload equal to `3` (Local Service Unavailable).
*   **TC-CON-003 (Version Negotiation Fallback):**
    *   *Requirement Mapping:* `REQ-003` (Version Negotiation Fallback)
    *   *Setup:* Initiate a mock WebSocket connection offering only `v2` protocol, while the client is configured for `v1` only.
    *   *Assert:* Client must transition to `Degraded` state, remain connected for health reporting, and return `local_service_unavailable` (Error Code 3) for configuration/action commands.
*   **TC-VAL-001 (Permissive Parameter Validation):**
    *   *Requirement Mapping:* `REQ-005` (Permissive Parameter Validation)
    *   *Setup:* Submit a configuration payload containing a known schema property with an invalid type, and an unknown future schema property.
    *   *Assert:* Schema validator must reject the request due to the invalid known parameter. Submit another payload containing only valid known parameters and unknown future parameters; the validator must pass the unknown parameters through to NATS unmodified.
*   **TC-CON-004 (Payload Sizing Constraints Checks):**
    *   *Requirement Mapping:* `REQ-020` (Sizing Constraints)
    *   *Setup:* Generate payloads of varying sizes for Configuration, State, Telemetry, and Logs. Send payloads exceeding their respective limits (10MB, 1MB, 256KB, 64KB).
    *   *Assert:* Client must reject/discard the oversized payloads and increment corresponding drop/error metrics. Payloads within limits must be successfully processed.

---

## Epic 2: Traffic Queues & Priority Scheduler

### PR 2.1: Priority Outbound Scheduler Tests
*   **TC-SCH-001 (Priority Outbound Ordering):**
    *   *Requirement Mapping:* `REQ-014` (Outbound Priority Scheduler)
    *   *Setup:* Instantiate `PriorityScheduler` with a capacity of 10. Push 5 messages of `PriorityLow` (Priority 3). Push 1 message of `PriorityHighest` (Priority 0).
    *   *Assert:* Calling `Next()` must return the `PriorityHighest` message first. Subsequent calls must return `PriorityLow` messages in FIFO order.
*   **TC-SCH-002 (Scheduler Blocking and Wakeup):**
    *   *Requirement Mapping:* `REQ-014` (Outbound Priority Scheduler)
    *   *Setup:* Call `Next()` on an empty `PriorityScheduler` in a separate goroutine.
    *   *Assert:* Goroutine must block. Push a message into the scheduler from the main thread; goroutine must unblock and receive the message.

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
    *   *Assert:* Client must immediately return an initial "started" status response, keep the transaction active in memory, and stream progress logs over NATS to the Cloud until a terminal state is reached.

### PR 3.2: Duplicate Attachment & Cache TTL Tests
*   **TC-RM-004 (Duplicate Request Attachment):**
    *   *Requirement Mapping:* `REQ-009` (Duplicate Transaction Attachment)
    *   *Setup:* Start transaction `rpc_id = "tx-1"`. Call `AttachDuplicate("tx-1")` from a second thread.
    *   *Assert:* `AttachDuplicate` must return `true` and the exact same `ResultCh` channel as the original transaction.
*   **TC-RM-005 (Operation-Specific Cache TTLs):**
    *   *Requirement Mapping:* `REQ-010` (Operation-Specific Caching & TTL)
    *   *Setup:* Write `configure` result (TTL 5 mins) and `upgrade` result (TTL 60 mins) to `TransactionCache`. Mock clock time to advance 10 minutes.
    *   *Assert:* Cache lookup for `configure` must return `false` (expired). Lookup for `upgrade` must return `true` (cached).
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
*   **TC-NET-003 (JetStream KV Revision Guard):**
    *   *Requirement Mapping:* `REQ-006` (JetStream KV Consistency Contract)
    *   *Setup:* Write config payload to JetStream KV. Verify sequence metadata.
    *   *Assert:* The client must retrieve the KV sequence revision and append it to the NATS trigger notification payload as `kv_revision`.
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
    *   *Setup:* Start the client, observe capabilities fetch on boot. Trigger a NATS reconnect event.
    *   *Assert:* Capabilities must be fetched exactly once on boot and cached. No new fetch must be triggered on NATS reconnect. Simulate a local Unix socket capabilities refresh command; the capabilities must be updated.

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
    *   *Assert:* Client writes KV config, triggers `config.apply` NATS command, receives `rolled_back` reply, and returns JSON-RPC error `1` containing the active config UUID.
