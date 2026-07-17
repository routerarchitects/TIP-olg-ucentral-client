# Test-Driven Development (TDD) Specification

This document details the test plans, test cases, and verification strategies for each phase of development of the uCentral Client (`TIP-olg-ucentral-client`).

---

## Epic 1: Scaffold & Base Types

### PR 1.1: Shared Contracts & Serialization Tests
*   **TC-CON-001 (Envelope Serialization):**
    *   *Requirement Mapping:* `REQ-028` (NATS Envelope Serialization Contract)
    *   *Setup:* Create instances of `ConfigureCommand`, `ActionCommand`, and `ResultEnvelope`.
    *   *Assert:* Marshalling to JSON must produce exact keys for each envelope type:
        *   `ActionCommand`: `version`, `correlation_id`, `target`, `command_type`, `action`, `payload`, `timestamp`. Must assert that if `payload` is nil, it is correctly omitted or formatted. Furthermore, calling `Validate()` must explicitly fail if `action` is `upgrade` and `operation_id` is empty before attempting serialization or dispatch.
        *   `ConfigureCommand`: `version`, `correlation_id`, `target`, `uuid`, `kv_key`, `kv_revision`, `timestamp`. Must assert that the raw `payload` is absent.
        *   `ResultEnvelope`: `version`, `correlation_id`, `target`, `command_type`, `result`, `message`, `timestamp`. Verify payload is serialized for command-specific results such as script, certupdate, and ping, and omitted when empty. Additionally verify that `operation_id` is serialized for upgrade operations, `uuid` is serialized for configure operations, and omitted fields are absent from the JSON.
*   **TC-CON-002 (Error Mappings):**
    *   *Requirement Mapping:* `REQ-021` (JSON-RPC Error Mapping)
    *   *Setup:* Pass internal error enum `ErrServiceUnavailable` to JSON-RPC error encoder helper.
    *   *Assert:* Encoder must output JSON-RPC error payload with `code = -32603` (Internal Error) and `data.application_code` equal to `3` (Local Service Unavailable).
*   **TC-CON-003 (Version Verification Fallback & Protocol State):**
    *   *Requirement Mapping:* `REQ-003` (Version Verification Fallback)
    *   *Setup:* (1) Initiate a mock WebSocket connection with NATS offline, transmit `connect.capabilities`, and simulate the Cloud returning a successful `connect` response (error=0). (2) Simulate the Cloud returning an explicitly defined fatal version-rejection response.
    *   *Assert:* (1) Client must mark protocol verification as successful but remain in `NATSDegraded` state because NATS is offline. It must not enter `Operational` until NATS connects. (2) Client must transition to `ProtocolFailure` state, remain connected for health reporting, and return `local_service_unavailable` (JSON-RPC code -32603, application_code 3) for configuration/action commands.
*   **TC-VAL-001 (Permissive Parameter Validation):**
    *   *Requirement Mapping:* `REQ-005` (Permissive Parameter Validation)
    *   *Setup:* Submit a configuration payload containing a known schema property with an invalid type, and an unknown future schema property.
    *   *Assert:* Schema validator must reject the request due to the invalid known parameter. Submit another payload containing only valid known parameters and unknown future parameters; the validator must pass the unknown parameters through to NATS unmodified.
*   **TC-CON-004 (Sizing Constraints, Bounded Readers, and Decompression Bombs):**
    *   *Requirement Mapping:* `REQ-020` (Sizing Constraints & Memory Protection)
    *   *Setup:* Send the following payloads to the client over WebSocket:
        1. A small compressed payload that decompresses into a massive 15MB configuration string (Decompression bomb).
        2. A continuous, unbounded stream of garbage JSON where no `id` is present within the first 10MB.
        3. A valid JSON-RPC request containing a valid `id`, but where the `params` payload exceeds 10MB.
    *   *Assert:* The client must use a bounded reader that terminates decompression and reading exactly at the uncompressed limit, before full memory allocation or JSON unmarshalling.
        * For Scenario 1 and 2, the client must forcefully reject/close the request without attempting to return a JSON-RPC error, and emit a metric.
        * For Scenario 3, the client must successfully parse the `id`, abort the rest of the payload, and return a JSON-RPC `-32602` (Invalid Params) with `error.data.application_code = 4` (Validation Failed).
*   **TC-CON-005 (JSON-RPC ID Preservation & Edge Cases):**
    *   *Requirement Mapping:* `REQ-027` (JSON-RPC ID Preservation & Edge Cases)
    *   *Setup:* Send a series of JSON-RPC requests containing:
        1. `id` as an integer (`42`) and string (`"42"`).
        2. No `id` field for a read-only command (e.g. `ping`).
        2b. No `id` field for a state-changing command (e.g. `reboot`).
        3. `id` as an object (`{"id": 1}`) and array (`[1, 2]`).
        4. A previously completed valid `id`.
    *   *Assert:* 
        * For (1), the client must successfully parse, track, and return the exact matching original format in the response.
        * For (2), the client must execute the command but send no WebSocket response, internally generating a unique correlation ID for NATS correlation.
        * For (2b), the client must immediately reject and drop the request before transaction creation or lock acquisition.
        * For (3), the client must reject the request with `-32600` or `-32700` and return `id: null`.
        * For (4), the client must not re-execute the command and must replay the cached JSON-RPC response.
*   **TC-CON-006 (OWGW Configure Request and Response Contract):**
    *   *Requirement Mapping:* `REQ-031` (OWGW Configure Protocol Compatibility)
    *   *Setup:* Send various JSON-RPC `configure` requests: (1) Numeric vs string `uuid`, (2) valid uncompressed `config` vs valid `compress_64`, (3) simultaneous `config` and `compress_64`, (4) neither `config` nor `compress_64` present, (5) valid `compress_64` with mismatched `compress_sz`, (6) valid `compress_64` that decompresses to >10MB, (7) invalid base64/zlib data, (8) any non-zero `when` value. Then, mock internal `ResultEnvelope` statuses (success, substitution, full rejection) to trigger response formatting.
    *   *Assert:* (1) String `uuid` is rejected, numeric is accepted. (2) Standard and compressed configs are properly parsed, and the decompressed `compress_64` content is successfully reparsed as the complete configure `params` object containing `serial`, `uuid`, `when`, and `config`. (3) Simultaneous fields yield Invalid Params. (4) Neither field yields Invalid Params. (5) `compress_sz` mismatch yields Invalid Params. (6) >10MB decompressed output is rejected before full allocation. (7) Invalid compression yields Invalid Params. (8) Any non-zero `when` yields Invalid Params. For responses: Internal success maps to `status.error = 0`, substitutions map to `1`, full rejection maps to `2`, `status.when` is present, `uuid` is numeric, and the original JSON-RPC `id` is preserved.
*   **TC-ACT-001 (OWGW Reboot Request and Response Contract):**
    *   *Requirement Mapping:* `REQ-033` (OWGW Reboot Protocol Compatibility)
    *   *Setup:* Send JSON-RPC `reboot` requests: (1) Missing/zero `when`, (2) non-zero `when`. Then mock internal NATS `ResultEnvelope` statuses (success, busy, rejected).
    *   *Assert:* (1) Missing/zero `when` is accepted. (2) Non-zero `when` is rejected with Invalid Params. For responses: Internal success maps to `status.error = 0`, busy maps to `1`, rejection maps to `2`. Ensure the response contains nested `status.when` and preserves the original JSON-RPC `id`.
*   **TC-ACT-002 (OWGW Factory Request and Response Contract):**
    *   *Requirement Mapping:* `REQ-034` (OWGW Factory Protocol Compatibility)
    *   *Setup:* Send JSON-RPC `factory` requests: (1) Missing/zero `when`, (2) non-zero `when`, (3) missing `keep_redirector`, (4) `keep_redirector` = 0, (5) `keep_redirector` = 1, (6) invalid `keep_redirector` (e.g. 2). Then mock internal NATS `ResultEnvelope` statuses (success, busy, rejected).
    *   *Assert:* (1) Missing/zero `when` is accepted. (2) Non-zero `when` yields Invalid Params. (3) Missing `keep_redirector` yields Invalid Params. (4,5) `keep_redirector` values 0 and 1 are accepted. (6) Invalid `keep_redirector` yields Invalid Params. The `keep_redirector` field must be preserved in the `action: "factory"` NATS payload. For responses: Internal success maps to `status.error = 0`, busy maps to `1`, rejection maps to `2`. Ensure the response preserves the original JSON-RPC `id` and serial.
*   **TC-ACT-005 (OWGW Trace Request Contract):**
    *   *Requirement Mapping:* `REQ-035`
    *   *Setup:* Send JSON-RPC `trace` requests with `duration`, `packets`, `network`, `interface`, and `uri`.
    *   *Assert:* Request maps exactly to NATS payload. Returns trace status object.
*   **TC-ACT-006 (OWGW Ping Request Contract):**
    *   *Requirement Mapping:* `REQ-036`
    *   *Setup:* Send JSON-RPC `ping` requests. Mock NATS response with ping info.
    *   *Assert:* Request maps to NATS payload. Response properly translates `serial`, `uuid`, and `deviceUTCTime` without mapping arbitrary internal strings.
*   **TC-ACT-007 (OWGW LEDs Request Contract):**
    *   *Requirement Mapping:* `REQ-037`
    *   *Setup:* Send JSON-RPC `leds` requests with varied `pattern` and `duration`.
    *   *Assert:* Request maps exactly to NATS payload. Returns LED status object.
*   **TC-ACT-008 (OWGW Telemetry Request Contract):**
    *   *Requirement Mapping:* `REQ-038`
    *   *Setup:* Send JSON-RPC `telemetry` requests with various `interval` and `types`.
    *   *Assert:* Validation must strictly enforce `0 <= interval <= 60`, `types` length of 1, exact match for "dhcp", and no duplicates. Valid requests map exactly to NATS payload. Returns telemetry status object.
*   **TC-ACT-009 (OWGW Remote Access / RTTY Request Contract):**
    *   *Requirement Mapping:* `REQ-039`
    *   *Setup:* Send JSON-RPC `remote_access` requests testing `method="rtty"`, exact `token`, `server`, `port` fields. Mock response with optional `meta`.
    *   *Assert:* Non-"rtty" methods or missing mandatory fields return Invalid Params. Valid requests map exactly to NATS `action: "rtty"` payload. Returns remote_access status object while correctly preserving optional `meta`.
*   **TC-ACT-010 (OWGW Certupdate Request and Response Contract):**
    *   *Requirement Mapping:* `REQ-040`
    *   *Setup:* Send JSON-RPC `certupdate` request containing base64 encoded certificates payload.
    *   *Assert:* Request maps exactly to NATS `action: "certupdate"`. Response must translate NATS result to `error` and `txt` properties. Validates: (1) malformed base64 returns Invalid Params, (2) decoded bundle over 2 MB returns Invalid Params, (3) valid base64 remains unchanged in `ActionCommand.payload`, and (4) certificate data never appears in logs.
*   **TC-ACT-011 (OWGW Reenroll Request and Response Contract):**
    *   *Requirement Mapping:* `REQ-041`
    *   *Setup:* Send JSON-RPC `reenroll` requests in parallel with other state-changing requests like `reboot`.
    *   *Assert:* missing/0 `when` is accepted, nonzero `when` returns Invalid Params. Reenroll must strictly acquire the serialization state lock. Assert correct conversion into `ActionCommand` and accurate translation of NATS `error` / `txt` responses.
*   **TC-ACT-012 (OWGW Script Request and Response Contract):**
    *   *Requirement Mapping:* `REQ-042`
    *   *Setup:* Send JSON-RPC `script` requests with combinations: (1) valid base64 script, (2) invalid base64 script, (3) `scriptId` included (must be rejected as non-standard), (4) invalid type, (5) invalid URI, (6) oversized script. Mock various results (timeout, gzipped result).
    *   *Assert:* Must enforce strict base64 decoding validation and forbid `scriptId`. Scenarios 2, 3, 4, 5, and 6 return Invalid Params. Scenario 1 maps exactly to NATS payload. Validate execution timeout errors. Translate NATS response formats properly into `error`, `result_64`, `result_sz`, or `result`. Verify sensitive script contents or execution logs are not printed to audit stream.
*   **TC-ACT-013 (Unsupported Commands Rejection):**
    *   *Requirement Mapping:* `REQ-032`
    *   *Setup:* Send JSON-RPC requests for out-of-scope features: `rrm`, `wifiscan`, `fixedconfig`, `powercycle`, `request`, event buffer retrieval, and `transfer`.
    *   *Assert:* The daemon must immediately reject the requests and return JSON-RPC `-32601 Method Not Found`.
---

## Epic 2: Traffic Queues & Priority Scheduler

### PR 2.1: Priority Outbound Scheduler Tests
*   **TC-SCH-001 (Priority Outbound Ordering):**
    *   *Requirement Mapping:* `REQ-014` (Outbound Priority Scheduler)
    *   *Setup:* Instantiate `PriorityScheduler` with a capacity of 10 and an emergency capacity of 100. Push 5 messages of `PriorityLow` (Priority 3). Push 1 message of `PriorityHighest` (Priority 0).
    *   *Assert:* Calling `Next()` must return the `PriorityHighest` message first. Subsequent calls must return `PriorityLow` messages in FIFO order.
*   **TC-SCH-005 (Anti-Starvation Yield Limit):**
    *   *Requirement Mapping:* `REQ-014` (Outbound Priority Scheduler)
    *   *Setup:* Pre-fill the scheduler with 20 `PriorityHighest` (Priority 0) messages and 5 `PriorityLow` (Priority 3) messages.
    *   *Assert:* Calling `Next()` repeatedly must yield exactly 10 consecutive `PriorityHighest` messages, followed by 1 `PriorityLow` message, followed by the remaining 10 `PriorityHighest` messages, before draining the rest of `PriorityLow`.
*   **TC-SCH-002 (Scheduler Blocking and Wakeup):**
    *   *Requirement Mapping:* `REQ-014` (Outbound Priority Scheduler)
    *   *Setup:* Call `Next()` on an empty `PriorityScheduler` in a separate goroutine.
    *   *Assert:* Goroutine must block. Push a message into the scheduler from the main thread; goroutine must unblock and receive the message.
*   **TC-SCH-003 (Priority 0 Bounded Emergency Queue):**
    *   *Requirement Mapping:* `REQ-014`
    *   *Setup:* Instantiate `PriorityScheduler` with per-priority capacity and a bounded emergency limit for Priority 0. Block the consumer to simulate a stalled WebSocket writer. Push Priority 0 messages until the emergency limit is reached.
    *   *Assert:* `Push()` must return an explicit overflow error once the Priority 0 emergency limit is exhausted. Queue growth must remain bounded.
*   **TC-SCH-006 (Non-Blocking Priority 2 and 3 Overflow Policies):**
    *   *Requirement Mapping:* `REQ-014`
    *   *Setup:* Fill Priority 2 and Priority 3 queues to their capacity. Have the upstream producer goroutines attempt to `Push` a peeked state and telemetry payloads.
    *   *Assert:* `Push()` must return `ErrQueueFull` immediately without blocking. For Priority 2, the test must verify the producer does nothing, skipping `Commit()`, leaving the state safely in the upstream coalescer. For Priority 3, the test must verify the payload is dropped and the `dropped_by_reason.scheduler_full` metric is incremented.
*   **TC-SCH-004 (Non-Blocking Priority 1 Overflow):**
    *   *Requirement Mapping:* `REQ-014`
    *   *Setup:* Instantiate `PriorityScheduler`. Fill the Priority 1 queue to maximum capacity. Attempt to push one more Priority 1 message (e.g. an audit log) from a separate goroutine.
    *   *Assert:* The `Push()` call must return immediately with a fast error, must **not** block the calling goroutine, must increment the `audit_delivery_failure` metric, and must avoid generating a recursive audit log.

### PR 2.2: Buffers, Coalescers & Ring Buffer Tests
*   **TC-BUF-001 (Telemetry Ring Buffer FIFO Drop):**
    *   *Requirement Mapping:* `REQ-015` (State Coalescer & Telemetry Ring Buffer)
    *   *Setup:* Instantiate `TelemetryRingBuffer` with capacity = 5. Push 5 messages. Push 6th message.
    *   *Assert:* The 6th push must return `dropped = true`. The 1st pushed message must be discarded. The buffer size must remain 5.
*   **TC-BUF-002 (State Coalescing last-write-wins and Generation Tracking):**
    *   *Requirement Mapping:* `REQ-015` (State Coalescer & Telemetry Ring Buffer)
    *   *Setup:* Write State Report A (`"uptime": 10`). Write State Report B (`"uptime": 20`) to `StateCoalescer`. Call `Peek()`. While holding the returned generation, write State C (`"uptime": 30`). Call `Commit()` with State B's generation.
    *   *Assert:* `Peek()` must return State B and its generation. `Commit()` must return `false` because a newer update exists (State C). State C must remain available in the coalescer for the next `Peek()`.
*   **TC-BUF-003 (NATS Dispatch Buffer Busy Rejection):**
    *   *Requirement Mapping:* `REQ-012` (Command Dispatch Buffer)
    *   *Setup:* (A) Instantiate `NATSDispatchBuffer` with capacity = 2. Push 2 messages, then push a 3rd. (B) Push a message when the underlying NATS connection state is offline.
    *   *Assert:* In case (A), the 3rd push must return a queue full error immediately. In case (B), the caller must return a fast error (`local_service_unavailable`) without blocking or relying solely on buffer capacity.
*   **TC-QUE-001 (Telemetry Throttling on Full Results Queue):**
    *   *Requirement Mapping:* `REQ-013` (Command Result Priority Queue)
    *   *Setup:* Fill the NATS command result queue (capacity 50) to 90% capacity. Send telemetry events.
    *   *Assert:* The client must throttle/delay telemetry forwarding to prioritize command results, ensuring core loops do not block.
*   **TC-QUE-002 (Command Result Queue Overflow Preserves Downstream Result):**
    *   *Requirement Mapping:* `REQ-013`, `REQ-021`
    *   *Setup:* Fill the Command Result Priority Queue to capacity. Simulate a correlated downstream command result arriving with a known `correlation_id`.
    *   *Assert:* `Push()` must return `ErrQueueFull`; the daemon must log the `correlation_id`, command type, and subject and increment `command_result_overflow`. The Request Manager MUST NOT rewrite the transaction state to Failed and MUST NOT cache a generated `-32603` failure. The exact original downstream response must be processed directly and preserved in the `TransactionCache`. If subsequent delivery to the Priority-0 WebSocket scheduler also fails, the daemon must trigger WebSocket path recovery. When the Cloud reconnects and retries, it must receive the exact original cached response.
*   **TC-BUF-004 (WebSocket permessage-deflate Negotiation & Threshold):**
    *   *Requirement Mapping:* `REQ-024` (Payload Compression)
    *   *Setup:* Set `compression_threshold_bytes` to 2048. Perform WebSocket handshake simulating a controller that accepts `permessage-deflate`. Generate a payload of size 1024 bytes and another of size 3072 bytes.
    *   *Assert:* The client must successfully negotiate the `permessage-deflate` extension during handshake. The 1024-byte payload must be sent as an uncompressed WebSocket frame. The 3072-byte payload must be sent as a compressed WebSocket frame managed transparently by the WebSocket layer. Unconditional application-level gzip mapping to a binary or non-standard format is prohibited.
*   **TC-QUE-003 (Priority 3 Read Hysteresis for Telemetry and Logs):**
    *   *Requirement Mapping:* `REQ-013` (Command Result Priority Queue)
    *   *Setup:* Fill the Command Result Priority queue beyond its critical threshold. While in this state, attempt to poll `TelemetryRingBuffer` for both telemetry and log events. Drain the Command Result queue below its resume threshold and poll again.
    *   *Assert:* Reads from `TelemetryRingBuffer` must return empty/block while the result queue is critical. Reads must resume yielding events once the result queue drains below the hysteresis threshold. This throttling must apply equally to telemetry and logs.
*   **TC-BUF-006 (Command Result Queue Non-Blocking):**
    *   *Requirement Mapping:* `REQ-013` (Command Result Priority Queue)
    *   *Setup:* Fill the NATS command result queue (capacity 50) to maximum capacity. Attempt to publish execution results from downstream agent loops.
    *   *Assert:* Outbound WebSocket writes or telemetry delays must not block the core NATS listener loops, ensuring execution results are processed asynchronously and independently.
*   **TC-BUF-007 (Outbound Rate Limiting, Drop Metrics & Coalescing):**
    *   *Requirement Mapping:* `REQ-015` (State Coalescer & Telemetry Ring Buffer)
    *   *Setup:* Rapidly push 60 telemetry events within 1 second into the `TelemetryRingBuffer` and 2 state updates within 5 seconds into the `StateCoalescer`.
    *   *Assert:* The producer/drain layer must rate-limit telemetry to 50 events/second (dropping 10 events) and state reports to 1 per 10 seconds before submitting them to the `OutboundScheduler`. The scheduler itself must not enforce these limits. Verify that dropped events correctly increment the `dropped_by_reason` metric map.

---

## Epic 3: Request Manager & Caching

### PR 3.1: Transaction State Machine & Manager Tests
*   **TC-RM-001 (State Machine Transitions):**
    *   *Requirement Mapping:* `REQ-007` (Transaction Lifecycle)
    *   *Setup:* Create a transaction using `CreateTransaction(cloudRPCID = "tx-1", respondToCloud = true, method = "action", timeout = 10s, isStateChanging = false)`.
    *   *Assert:* Initial state must be `TxCreated`, and the Request Manager must generate and assign a valid internal `CorrelationID`. Manually advance the transaction through `TxPreparingDispatch`, `TxPendingPublish`, and `TxInFlight`. Verify every enum state. Verify that KV/preparation failure $\rightarrow$ `TxFailed`, dispatch-buffer full $\rightarrow$ `TxFailed`, publish/request failure $\rightarrow$ `TxFailed`. Ensure timeout is valid only after `TxInFlight`.
*   **TC-RM-002 (Concurrency Rejection):**
    *   *Requirement Mapping:* `REQ-008` (Concurrency Serialization)
    *   *Setup:* Start a transaction with `isStateChanging = true` for `Cloud ID = "tx-1"` and hold it in the `TxCreated` state. While transaction A is in `TxCreated`, concurrently submit another transaction request with `Cloud ID = "tx-2"`, `isStateChanging = true`.
    *   *Assert:* The Request Manager must guarantee that transaction A atomically reserves the state lock during creation. The second transaction request must return a `busy` error immediately and must not be created.
*   **TC-RM-003 (Parallel Read Operations):**
    *   *Requirement Mapping:* `REQ-008` (Concurrency Serialization)
    *   *Setup:* Start state-changing transaction `Cloud ID = "tx-1"`. Submit read-only command transaction `Cloud ID = "query-1"`, `isStateChanging = false`.
    *   *Assert:* Transaction `query-1` must succeed and run in parallel (no busy error).
*   **TC-UPG-001 (Asynchronous Upgrade Progress Stream):**
    *   *Requirement Mapping:* `REQ-011` (Asynchronous Upgrade Tracking)
    *   *Setup:* Start an upgrade transaction. Test two cases: (A) Gateway does not advertise `upgrade_progress` support, (B) Gateway advertises `upgrade_progress` support.
    *   *Assert:* In both cases, the client must immediately return an initial "started" status response matching the `CloudUpgradeResponse` schema (with `status.error = 0`). The initial JSON-RPC exchange must be closed while the background upgrade operation remains active. For case A, the client must NOT send any `upgrade_progress` notifications. For case B, optional progress notifications matching the `CloudUpgradeProgressNotification` schema may be emitted.
*   **TC-UPG-002 (Upgrade Crash Recovery via Durable Store and Status Query):**
    *   *Requirement Mapping:* `REQ-011`
    *   *Setup:* Simulate a daemon crash/restart while an upgrade is active downstream. Populate `OperationStore` with an active operation record. Mock a downstream device/local-agent responder on `ucentral.v1.device.<own-serial>.status.get`.
    *   *Assert:* On boot, the daemon must load the `OperationStore` to recover the Cloud JSON-RPC `id` and immediately re-acquire the in-memory `activeStateTx` lock. It must then publish a request to `status.get` generating a **fresh internal `correlation_id`**, receive the downstream status response, correlate it using the persisted `operation_id`, and release the lock if a terminal state is reached. If the downstream reports active but omitting `operation_id`, the daemon must not generate a replacement ID and must retain the lock as an indeterminate error. The uCentral client itself must not subscribe to or respond on `status.get`.
*   **TC-UPG-003 (Pending Terminal Delivery Crash Recovery):**
    *   *Requirement Mapping:* `REQ-011`
    *   *Setup:* Simulate a daemon crash after an upgrade is saved with `Active=false` but before the final Cloud delivery or cache population. On restart, populate `OperationStore` with this terminal record.
    *   *Assert:* On boot, `GetPendingTerminalDelivery()` must return the terminal record. The daemon must then successfully enqueue any optional negotiated notifications, store the terminal status internally, and finally delete the record from `OperationStore`.

### PR 3.2: Duplicate Attachment & Cache TTL Tests
*   **TC-RM-004 (Request Lifecycle and Duplicate Rejection):**
    *   *Requirement Mapping:* `REQ-009` (Request Lifecycle and Duplicate Rejection)
    *   *Setup:* (1) Mock a completed, unexpired cached response for `Cloud ID = "tx-1"`. Submit a duplicate request for `tx-1`. (2) Start transaction `Cloud ID = "tx-2"` and hold it in `Created` state. Submit a duplicate request. Advance original to `PreparingDispatch`, then `PendingPublish`, then `InFlight`, submitting a duplicate request at each state. (3) Submit a request for a new `Cloud ID = "tx-3"`.
    *   *Assert:* (1) For `tx-1`, the cached response must be replayed directly to the Cloud without creating a transaction, acquiring the state-changing lock, or writing to NATS. (2) For `tx-2`, in all four active states (`Created`, `PreparingDispatch`, `PendingPublish`, `InFlight`), the duplicate request must fail immediately and return a busy error (`-32603`) without overwriting the map entry, altering timeouts, or triggering a second downstream execution. (3) For `tx-3`, a new transaction must be created and enter `Created`.
*   **TC-RM-005 (Operation-Specific Cache TTLs):**
    *   *Requirement Mapping:* `REQ-010` (Operation-Specific Caching & TTL)
    *   *Setup:* Set environment variables `OLG_CACHE_TTL_CONFIGURE=6m`, `OLG_CACHE_TTL_FACTORY=0s` (invalid), and `OLG_CACHE_TTL_SCRIPT=invalid`. Call `LoadCacheTTLConfigFromEnv()`. Then, write results for `ping` (using `Default` TTL 2 mins), `configure` (using overridden 6 mins), `reboot` (default 10 mins), `factory` (default 30 mins), and `upgrade` (default 60 mins) to `TransactionCache`. Mock clock time to advance 15 minutes.
    *   *Assert:* `LoadCacheTTLConfigFromEnv()` must return an error for the `0s` and `invalid` environment variables (rejecting them). When correctly loaded with valid overrides, `TTLForMethod("configure")` must return 360 seconds. Cache lookups for `ping` and `reboot` must return `false` (expired). Lookups for `factory` and `upgrade` must return `true` (cached).
*   **TC-RM-006 (Transaction Retry Policy & Backoff):**
    *   *Requirement Mapping:* `REQ-025` (Transaction Retry Policy)
    *   *Setup:* Submit a read-only request (`capabilities.get`) and a state-changing request (`configure`). For the read-only request, simulate timeouts for attempt 1 and 2. After attempt 2 times out, simulate a late downstream response for attempt 1 arriving exactly while attempt 3 is active on the wire.
    *   *Assert:* The state-changing request must fail fast on the first error with no retries. The read-only request must retain the exact same `correlation_id` across 3 total attempts (1 initial + 2 retries). Each attempt must use an independent request timeout. The overall transaction must remain active during the exponential backoff periods. When the late response for attempt 1 arrives during attempt 3, the transaction must immediately transition to `Completed`, winning the race, and any subsequent reply from attempt 3 must be gracefully ignored.
*   **TC-RM-007 (JSON-RPC ID Preservation & Boundaries):**
    *   *Requirement Mapping:* `REQ-027` (JSON-RPC ID Preservation & Edge Cases)
    *   *Setup:* Submit: (1) a read-only notification (no `id`), (1b) a state-changing notification (no `id`), (2) a numeric ID `42`, (3) a string ID `"42"`, (4) a reused ID while the original is still active, and (5) a reused ID matching a completed cached transaction.
    *   *Assert:* (1) Read-only notification generates a valid `correlation_id` but sends no Cloud response. (1b) State-changing notification is immediately rejected and dropped without execution. (2/3) Numeric `42` and string `"42"` process in parallel independently without key collision, maintaining raw type in Cloud response. (4) Reused canonical Cloud ID matching an active transaction is rejected, while (5) a completed matching ID replays the cached response. NATS payloads must only contain `correlation_id`.
*   **TC-RM-008 (PendingPublish Dispatch Deadline):**
    *   *Requirement Mapping:* `REQ-007` (Transaction Lifecycle)
    *   *Setup:* Create an action transaction. Move it to `PendingPublish`. Put its payload into the dispatch buffer. Stall the dispatch consumer so no NATS publish occurs. Let the dispatch deadline expire.
    *   *Assert:* State becomes `Failed`. State never becomes `InFlight`. Downstream timeout never starts. Active maps are cleaned. State-changing reservation is released. Cloud receives `local_service_unavailable`. A later delayed buffer item must not be published as an active command.

### PR 3.3: Advanced Concurrency & Race Conditions
*   **TC-RM-009 (Terminal Sequence Mutex Race):**
    *   *Setup:* Run a transaction. Block the terminal method's execution exactly after cache insertion but before active map removal. Have the Cloud submit a duplicate JSON-RPC ID.
    *   *Assert:* The duplicate request must block waiting for the Request Manager mutex. Once the original terminal method releases the mutex, the duplicate request must find the response in the `TransactionCache` and replay it (cache hit), rather than creating a new transaction.
*   **TC-RM-010 (Dispatch Deadline Timer & Stalled Consumer):**
    *   *Setup:* Create a transaction (starts `DispatchTimer`). Move to `PendingPublish`. Stall the NATS dispatch consumer. Let the timer expire.
    *   *Assert:* Timer callback atomically transitions state to `Failed`, caches a failure response, enqueues `-32603` to Cloud, and releases the state lock. The stalled transaction must not remain stuck forever.
*   **TC-RM-011 (MarkInFlight vs Dispatch Timer Race):**
    *   *Setup:* Let `DispatchTimer` expire at the exact same millisecond that `MarkInFlight` is called.
    *   *Assert:* If the timer wins, `MarkInFlight` returns `ErrAlreadyTerminal`. If `MarkInFlight` wins, it successfully stops/invalidates the timer and starts the Response timer. The transaction must never have both timers active simultaneously.
*   **TC-RM-012 (Response Timeout vs Fast Reply Race):**
    *   *Setup:* Transaction is `InFlight`. The downstream reply arrives at the exact millisecond the Response timeout expires.
    *   *Assert:* The first to acquire the Request Manager mutex wins. If the timer wins, it transitions to `TimedOut`. If the reply wins, it transitions to `Completed`. The loser must receive `ErrAlreadyTerminal` and gracefully exit without crashing or panicking.
*   **TC-RM-013 (Fast Reply vs MarkInFlight Race):**
    *   *Setup:* A fast downstream reply arrives before `MarkInFlight` is called. The result handler calls `Complete()`.
    *   *Assert:* `Complete()` MUST NOT transition the state to terminal; instead, it must park the response in the `pendingReplies` buffer and exit gracefully. When `MarkInFlight` is subsequently called, it successfully enters `TxInFlight` and immediately discovers the buffered reply, forwarding it to the terminal sequence.
*   **TC-RM-014 (Priority-0 Delivery Failure):**
    *   *Setup:* Complete a transaction successfully. Fill the Priority-0 websocket queue to capacity so reservation fails.
    *   *Assert:* The transaction state remains `Completed` (the true device outcome). The exact success response is cached. The system triggers path recovery (WebSocket reconnect) rather than rewriting the transaction state to `Failed`.
*   **TC-RM-015 (RequestKey Canonicalization):**
    *   *Setup:* Submit `configure id=42` and complete it. Submit `ping id=42`.
    *   *Assert:* The cache key must structurally combine the method and ID (e.g., `configure:number:42`). `ping id=42` must execute normally as a new transaction and MUST NOT replay the cached `configure` response.

### PR 3.4: Asynchronous Upgrade & Persistence Races
*   **TC-UPG-004 (Upgrade Asynchronous Lock Handoff):**
    *   *Setup:* Submit an `upgrade` command. Wait for it to be accepted downstream.
    *   *Assert:* `RespondAndRetain` caches the "started" response, completes the JSON-RPC transaction, removes it from active maps, cancels the synchronous response timeout, and successfully transfers the state-changing lock ownership to the persistent `OperationID`.
*   **TC-UPG-005 (Persistent Upgrade Timeout Policy):**
    *   *Setup:* Submit an `upgrade` command. The JSON-RPC transaction completes via `RespondAndRetain`. Let 120 seconds pass.
    *   *Assert:* The upgrade must remain actively locked in the background. It MUST NOT be killed by the standard 120-second JSON-RPC timeout, which was canceled during handoff.
*   **TC-UPG-006 (Daemon Crash Before Acceptance):**
    *   *Setup:* Submit an `upgrade`. Crash the daemon after persisting the operation to `OperationStore` but before `RespondAndRetain` enqueues the "started" response to the Cloud.
    *   *Assert:* On reboot, the daemon must recover the active operation from the store and resume tracking.
*   **TC-UPG-007 (Daemon Crash After Acceptance):**
    *   *Setup:* Submit an `upgrade`. Crash the daemon immediately after sending the "started" response but before removing the JSON-RPC transaction from active memory maps.
    *   *Assert:* On reboot, the system relies on `OperationStore` for truth. The in-memory JSON-RPC transaction map is naturally cleared by the crash, preventing any zombie transactions.
*   **TC-UPG-008 (Terminal Upgrade Status vs Recovery Race):**
    *   *Setup:* The daemon crashes during an upgrade. On reboot, it attempts to query `status.get` for recovery. Exactly as it sends the query, the downstream agent unilaterally sends an `upgrade_progress` terminal notification.
    *   *Assert:* The Request Manager must handle the terminal notification, complete the operation, and release the state lock. The subsequent `status.get` response must gracefully drop or ignore the result since the operation is already terminal.

---

## Epic 4: Network & Transport Clients

### PR 4.1: WebSocket Client Tests
*   **TC-NET-001 (Randomized Reconnect Backoff):**
    *   *Requirement Mapping:* `REQ-002` (Reconnection State Machine)
    *   *Setup:* Instantiate reconnection backoff loops. Simulate connection drops.
    *   *Assert:* Reconnect delays must fall within exponential bounds (e.g. attempt 2 delay is between `4.0s` and `4.8s` given base `4s` and `10-20%` randomized additive jitter).

### PR 4.2: NATS Integration Client Tests
*   **TC-NET-003 (JetStream KV Revision Guard & Trigger Contract):**
    *   *Requirement Mapping:* `REQ-006` (JetStream KV Consistency Contract)
    *   *Setup:* Write config payload to JetStream KV. Retrieve the sequence revision and publish the `config.apply` NATS trigger. Intercept the serialized trigger. Then, simulate a downstream agent processing the trigger under two conditions: (A) when the KV store revision exactly matches the trigger `kv_revision`, and (B) when the KV store contains a newer, higher revision payload.
    *   *Assert:* The intercepted trigger must contain `uuid`, `kv_key`, `kv_revision`, `target`, and `correlation_id` while strictly omitting the full configuration `payload`. In condition A (exact match), the simulated agent must successfully download and apply the configuration. In condition B (mismatch), the agent must explicitly abort the apply process, completely fulfilling the consistency contract.
*   **TC-SEC-001 (Target Subject Isolation Constraints):**
    *   *Requirement Mapping:* `REQ-004` (Subject Schema Versioning), `REQ-016` (NATS Security & Target Isolation)
    *   *Setup:* Attempt to publish or subscribe to a subject with a different target serial (e.g. `ucentral.v1.device.different-serial.state`).
    *   *Assert:* Connection/authorization must block or reject the operation, ensuring target-serial isolation.
*   **TC-NET-007 (Device Health Forwarding and No Daemon Status Responder):**
    *   *Requirement Mapping:* `REQ-019`
    *   *Setup:* Publish a valid device health snapshot to `ucentral.v1.device.<own-serial>.health`. Separately, publish/request `ucentral.v1.device.<own-serial>.status.get` with only the uCentral client running and no downstream status responder.
    *   *Assert:* The client must subscribe to `.health`, validate/rate-limit the payload, and enqueue accepted health updates for Cloud forwarding. The client must not respond to `status.get` with daemon liveness/readiness, Cloud connectivity, queue depth, uptime, or metrics.
*   **TC-SEC-002 (TLS v1.3 and CA Verification):**
    *   *Requirement Mapping:* `REQ-023` (TLS v1.3 Security)
    *   *Setup:* Configure the NATS client to connect to a broker without TLS or with an invalid CA cert.
    *   *Assert:* Client must fail to connect and reject the connection attempt. Configure with a valid CA cert and TLS v1.3; the connection must succeed.
*   **TC-NET-013 (Partial Publish Failure Propagation):**
    *   *Requirement Mapping:* `REQ-026` (Desired/Applied Cloud Reconciliation Contract)
    *   *Setup:* Intercept and mock the NATS client to succeed on the JetStream KV write, but intentionally return a network error when publishing the `config.apply` trigger.
    *   *Assert:* The client must not crash. The Request Manager must trap the publish error, leave the KV record intact, and successfully formulate and return a JSON-RPC failure response to the Cloud.


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
*   **TC-NET-010 (Independent Connection-State Transitions):**
    *   *Requirement Mapping:* `REQ-002`
    *   *Setup:* Independently change Cloud, NATS, and protocol verification states.
    *   *Assert:*
        * Cloud Connected + NATS Connected = `Operational`.
        * Cloud Offline/Connecting + NATS Connected = `CloudDegraded`.
        * Cloud Connected + NATS Offline/Connecting = `NATSDegraded`.
        * Neither connection Connected = `Offline`.
        * Cloud Connected + Protocol Verification Failed = `ProtocolFailure` (takes strict precedence regardless of NATS state).
        * Losing Cloud does not change or reconnect the NATS link.
        * Losing NATS does not change or reconnect the Cloud link.
*   **TC-NET-011 (Unix Socket Rate Limiting & Auditing):**
    *   *Requirement Mapping:* `REQ-017` (Local Management Signal Security), `REQ-018` (Audit Logging & Loop Prevention)
    *   *Setup:* Send 10 capability refresh requests to the Unix socket in 1 second. Trigger sensitive actions (`reboot`, `factory`, `upgrade`, `certupdate`, `reenroll`, `script`).
    *   *Assert:* The Unix socket listener must rate-limit and reject excess refresh requests. Sensitive actions must successfully emit high-severity audit logs to the Cloud while guaranteeing the complete redaction of certificate contents, script source, script signatures, and script output.
*   **TC-NET-012 (Syslog-Triggered Capability Refreshes):**
    *   *Requirement Mapping:* `REQ-022` (Capability Caching & Lifecycle)
    *   *Setup:* Input a syslog message indicating a firmware version change, and a NATS message indicating an upgrade reboot log.
    *   *Assert:* Both trigger events must invalidate the capability cache and launch a new downstream NATS capability discovery query.

---

## Epic 5: Main Entry Point & Assembly

### PR 5.1: Main Loop Tests
*   **TC-INT-001 (Graceful Teardown and Priority Deadline):**
    *   *Requirement Mapping:* `REQ-029` (Graceful Teardown)
    *   *Setup:* Boot main client. Populate the outbound scheduler with Priority 0, 1, and 3 messages. Simulate a slow WebSocket connection that cannot flush all messages within 5 seconds. Send `SIGTERM` signal.
    *   *Assert:* The client must preferentially flush Priority 0 messages first. It must enforce a strict 5-second deadline. It must discard lower-priority messages if the deadline expires, force-close WebSocket and NATS connections exactly at or before the 5-second mark, and terminate the process with exit code 0.

### PR 5.2: End-to-End Integration Tests
*   **TC-INT-002 (Config Sync and Rollback Flow):**
    *   *Requirement Mapping:* `REQ-006` (KV Consistency), `REQ-021` (Error Mapping)
    *   *Setup:* Push configuration update from mock WebSocket server. Downstream agent returns rollback result.
    *   *Assert:* Client writes KV config, triggers `config.apply` NATS command, receives `rolled_back` reply, and returns JSON-RPC error `code = -32603` (Internal Error) with `error.data.application_code = 5` (Rollback Completed) containing the active config UUID inside the `error.data` payload.
*   **TC-INT-003 (Concurrent Startup Loops and Independent Connections):**
    *   *Requirement Mapping:* `REQ-001` (Concurrent Startup Loops), `REQ-002` (Reconnection State Machine)
    *   *Setup:* Start the daemon with NATS connection blocked (unreachable broker) but Cloud WebSocket reachable.
    *   *Assert:* Daemon must successfully establish connection to the Cloud WebSocket and report status as `NATSDegraded` (due to NATS being offline) without hanging or blocking on the NATS connection loop.
*   **TC-INT-004 (Priority 0 Overflow Recovery):**
    *   *Requirement Mapping:* `REQ-014`
    *   *Setup:* Simulate a stalled WebSocket writer while the daemon continues generating Priority 0 responses.
    *   *Assert:* The daemon treats the WebSocket writer path as unhealthy, triggers recovery, preserves the terminal transaction state and cached response, and increments the overflow metric instead of allowing unbounded memory growth.
*   **TC-INT-005 (Configuration Validation & Startup Failure):**
    *   *Requirement Mapping:* `REQ-030` (Startup Configuration Validation)
    *   *Setup:* Attempt to boot the daemon with various invalid configurations: missing serial, `http://` cloud URL, insecure `nats://` server, unreadable credentials, zero/negative bounds (`compression_threshold_bytes <= 0`, `cloud.connect_timeout_seconds <= 0`, queue capacities <= 0), and invalid timeout environment variables (malformed strings, `0s`, and `-5s`). Also boot with missing timeout variables to test defaults, and with valid overrides (e.g., `OLG_TIMEOUT_DISPATCH=2s`).
    *   *Assert:* The daemon must strictly validate the configuration before starting any connection loops, log the specific invalid field, and exit immediately with a non-zero status for invalid configurations. Booting with missing timeout variables must succeed and apply the exact defaults (`5s`, `30s`, `60s`, `120s`). Booting with valid overrides must succeed and accurately apply the overridden durations. Booting with valid config defaults must correctly apply `connect_timeout_seconds=10`, `compression_threshold_bytes=2048`, `ws_writer_capacity=500`, `emergency_capacity=100`, `nats_publish_capacity=100`, `command_result_capacity=50`, and `telemetry_capacity=500`.

---

## Epic 6: Requirements Traceability Matrix

| Requirement ID | Requirement Name | Mapping Test Case(s) |
| :--- | :--- | :--- |
| **REQ-001** | Concurrent Startup Loops | `TC-INT-003` |
| **REQ-002** | Reconnection State Machine | `TC-NET-001`, `TC-NET-010`, `TC-INT-003` |
| **REQ-003** | Version Negotiation Fallback | `TC-CON-003` |
| **REQ-004** | Subject Schema Versioning | `TC-SEC-001` |
| **REQ-005** | Permissive Parameter Validation | `TC-VAL-001` |
| **REQ-006** | JetStream KV Consistency Contract | `TC-NET-003`, `TC-INT-002` |
| **REQ-007** | Transaction Lifecycle | `TC-RM-001`, `TC-RM-008` |
| **REQ-008** | Concurrency Serialization | `TC-RM-002`, `TC-RM-003` |
| **REQ-009** | Duplicate Active Request Rejection | `TC-RM-004` |
| **REQ-010** | Operation-Specific Caching & TTL | `TC-RM-005` |
| **REQ-011** | Asynchronous Upgrade Tracking & Crash Recovery | `TC-UPG-001`, `TC-UPG-002`, `TC-UPG-003` |
| **REQ-012** | Command Dispatch Buffer | `TC-BUF-003` |
| **REQ-013** | Command Result Priority Queue | `TC-QUE-001`, `TC-QUE-002`, `TC-QUE-003`, `TC-BUF-006` |
| **REQ-014** | WebSocket Outbound Priority Scheduler | `TC-SCH-001`, `TC-SCH-002`, `TC-SCH-003`, `TC-SCH-004`, `TC-SCH-005`, `TC-SCH-006`, `TC-INT-004` |
| **REQ-015** | State Coalescer & Telemetry Ring Buffer | `TC-BUF-001`, `TC-BUF-002`, `TC-BUF-007` |
| **REQ-016** | NATS Security & Target Isolation | `TC-SEC-001` |
| **REQ-017** | Local Management Signal Security | `TC-NET-005`, `TC-NET-011` |
| **REQ-018** | Audit Logging & Loop Prevention | `TC-NET-006`, `TC-NET-011` |
| **REQ-019** | NATS-Native Health Reporting | `TC-NET-007` |
| **REQ-020** | Sizing Constraints | `TC-CON-004` |
| **REQ-021** | JSON-RPC Error Mapping | `TC-CON-002`, `TC-QUE-002`, `TC-INT-002` |
| **REQ-022** | Capability Caching & Lifecycle | `TC-NET-008`, `TC-NET-012` |
| **REQ-023** | TLS v1.3 Security | `TC-SEC-002` |
| **REQ-024** | Payload Compression | `TC-BUF-004` |
| **REQ-025** | Request Manager Retry Policy | `TC-RM-006` |
| **REQ-026** | Desired/Applied Cloud Reconciliation Contract | `TC-NET-013` |
| **REQ-027** | JSON-RPC ID Preservation | `TC-CON-005`, `TC-RM-007` |
| **REQ-028** | NATS Envelope Serialization Contract | `TC-CON-001` |
| **REQ-029** | Graceful Teardown | `TC-INT-001` |
| **REQ-030** | Startup Configuration Validation | `TC-INT-005` |
| **REQ-031** | OWGW Configure Protocol Compatibility | `TC-CON-006` |
| **REQ-032** | Out of Scope Features | `TC-ACT-013` |
| **REQ-033** | OWGW Reboot Protocol Compatibility | `TC-ACT-001` |
| **REQ-034** | OWGW Factory Protocol Compatibility | `TC-ACT-002` |
| **REQ-035** | OWGW Trace Protocol Compatibility | `TC-ACT-005` |
| **REQ-036** | OWGW Ping Protocol Compatibility | `TC-ACT-006` |
| **REQ-037** | OWGW LEDs Protocol Compatibility | `TC-ACT-007` |
| **REQ-038** | OWGW Telemetry Protocol Compatibility | `TC-ACT-008` |
| **REQ-039** | OWGW RTTY Protocol Compatibility | `TC-ACT-009` |
| **REQ-040** | OWGW Certupdate Protocol Compatibility | `TC-ACT-010` |
| **REQ-041** | OWGW Reenroll Protocol Compatibility | `TC-ACT-011` |
| **REQ-042** | OWGW Script Protocol Compatibility | `TC-ACT-012` |
