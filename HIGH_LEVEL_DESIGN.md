# High-Level Design: uCentral Client with NATS Integration (Language-Agnostic)

This document outlines the High-Level Design (HLD) for the **uCentral Client** (`TIP-olg-ucentral-client`). This client acts as a gateway/bridge between a cloud-based management platform (via the uCentral WebSocket/JSON-RPC protocol) and local device services (such as the VyOS NATS Client) using a local **NATS message bus**.

---

## 1. Architectural Overview

The uCentral client is a daemon that embeds a standard NATS agent library (modeled after the `nats-agent-core` specification). It acts as a **north-south proxy**, bridging the WAN Cloud interface with the LAN NATS bus. 

### Component Architecture

```mermaid
graph TD
    %% Cloud Management
    Cloud[uCentral Cloud Gateway] <-->|WSS JSON-RPC 2.0| WS_Eng[WebSocket Engine]

    subgraph uCentral_Client_Daemon [uCentral Client Daemon]
        WS_Eng <-->|Internal Queue| Req_Mgr[Request Manager]
        Req_Mgr <-->|Internal Queue| Router[Command & Event Router]
        Router <-->|Agent API| NATS_Client[Embedded NATS Integration Client]
    end

    %% Local Bus
    NATS_Client <-->|Pub/Sub & JetStream KV| NATS_Bus[NATS Message Bus]

    %% Other Client Microservices
    NATS_Bus <-->|"cmd.configure.<target>"| VyOS_Client[VyOS NATS Client]
    NATS_Bus <-->|"status.<target>"| Telemetry_Service[State/Telemetry Collectors]
    NATS_Bus <-->|"logs.<target>"| Logging_Service[System Log Forwarder]
```

### 1.1 Ownership Boundaries
To avoid ambiguity between the uCentral client and downstream NATS microservices (such as the VyOS NATS Client), responsibilities are strictly divided as follows:

| Responsibility | Owning Component | Description |
| :--- | :--- | :--- |
| **Transport Validation** | uCentral Client | Checks JSON integrity, validates headers, and ensures the target serial matches the client config. |
| **Schema Parameter Validation** | uCentral Client | Validates incoming configuration payload values (parameter types, bounds, ranges, required fields) in-memory before NATS writes, using a compiled-in validator generated from the `olg-ucentral-schema` repository. **Permissive mode** is enforced: known fields are validated strictly, while unknown future fields are passed through to avoid tight coupling. |
| **Platform-Specific Translation** | Downstream Agent (e.g., VyOS Client) | Evaluates whether the valid schema properties are supported by the local system and translates them into system actions. |
| **Configuration Rollback**| Downstream Agent (e.g., VyOS Client) | Manages the configuration transaction; if an applied configuration fails connectivity or validation checks, the downstream agent performs a transaction rollback to the last known good configuration. |

### 1.2 Dedicated Request Manager
To decouple message routing from request lifecycles, the uCentral Client includes a **Request Manager** module responsible for:
*   **Correlation Tracking:** Maps outgoing NATS commands to incoming Cloud JSON-RPC requests using request IDs.
*   **Deduplication & Cache:** Handles duplicate requests and caches responses.
*   **Request Timeouts:** Enforces timeouts using non-blocking timers and triggers failure responses if local components do not respond in time.
*   **Retry Handling:** Automates transaction retries for transient failures. Only idempotent, background startup discovery queries, such as `capabilities.get` and `status.get`, are retryable for transient failures, using a randomized exponential backoff (e.g., base 2s, max 3 attempts). In contrast, on-demand live telemetry queries (`state.get`) are strictly bounded by a single request attempt (`OLG_TIMEOUT_DISPATCH`, default 5s) with **zero retries** and fail fast on `service_busy` or timeout to respect caller deadlines and prevent overload amplification. State-changing and security-sensitive actions (`configure`, `reboot`, `factory`, `upgrade`, `certupdate`, `reenroll`, `script`) must also fail fast without automatic retries. The `status.get` and `state.get` subjects are owned by the downstream device/local agent; the uCentral client publishes requests to them and must not subscribe to or respond on them.
*   **Metrics Collection:** Tracks latencies, throughputs, and success/error rates per command.

---

## 2. NATS & JetStream Interface

The client leverages standard structures, helper APIs, and connection wrappers defined in the NATS agent library.

### 2.1 Common Message Envelopes
To ensure interoperability, the uCentral client standardizes on standard envelopes:

#### Outgoing Configuration Trigger (ConfigureCommand)
```json
{
  "version": "1.0",
  "rpc_id": "8bc92d11-536c-4860-9d8a-6809694b78ba",
  "target": "00:11:22:33:44:55",
  "uuid": "1698765432",
  "kv_bucket": "cfg_desired",
  "kv_key": "desired.00:11:22:33:44:55",
  "timestamp": "2026-07-20T10:00:00Z"
}
```

#### Outgoing Action Envelope (ActionCommand)
```json
{
  "version": "1.0",
  "rpc_id": "c138d6df-85f0-4322-927b-23fcfdf626c0",
  "target": "00:11:22:33:44:55",
  "command_type": "action",
  "action": "reboot",
  "payload": {
    "when": 0
  },
  "timestamp": "2026-06-25T12:01:00Z"
}
```

```json
{
  "version": "1.0",
  "rpc_id": "d249e7e0-96f1-5433-a38c-34fcfdf737d1",
  "target": "00:11:22:33:44:55",
  "command_type": "action",
  "action": "factory",
  "payload": {
    "keep_redirector": 1,
    "when": 0
  },
  "timestamp": "2026-06-25T12:02:00Z"
}
```

#### Inbound Result Envelope (ResultEnvelope)
*Note:* The `uuid` field refers specifically to the Configuration UUID. It is populated only for `configure` commands and is omitted (empty) for transient action commands (such as `reboot` or `trace`).
```json
{
  "version": "1.0",
  "rpc_id": "8bc92d11-536c-4860-9d8a-6809694b78ba",
  "target": "00:11:22:33:44:55",
  "command_type": "configure",
  "uuid": 1687588800,
  "result": "success",
  "message": "Configuration applied and saved successfully.",
  "timestamp": "2026-06-25T12:00:15Z"
}
```

### 2.2 Correlation ID Propagation
The `rpc_id` field in all envelopes acts as the central correlation ID for NATS transactions.
1.  When a Cloud JSON-RPC request is received, the Request Manager constructs a unique `rpc_id` for NATS. For ordinary requests with an ID, it is deterministically generated by combining the WebSocket Session ID and the Cloud's raw JSON-RPC `id` (e.g., `session-123:42`). For read-only notifications without an ID, it constructs a unique ID (e.g., `session-123:notification:<generated-uuid>`). This automatically solves collisions without an in-memory UUID map.
2.  The Request Manager populates the `rpc_id` field of the NATS payload with this deterministic Canonical Cloud ID.
3.  The downstream agent processes the command and returns a `ResultEnvelope` maintaining the exact same `rpc_id`.
4.  The Request Manager uses this correlation ID to look up the original transaction, recovers the Cloud `id`, and returns the response to the correct WebSocket client.

### 2.3 Flat Subject Schema

To guarantee flawless integration with the existing VyOS NATS client, all NATS subjects use a flat `agentcore` architecture without nested namespaces:

*   **Configure Trigger:** `cmd.configure.<target>` (Pub-Sub / Asynchronous Command Delivery)
*   **Action Command:** `cmd.action.<target>.<command>` (Pub-Sub / Asynchronous Command Delivery)
*   **State Publish:** `status.<target>` (Pub-Sub)
*   **Telemetry Publish:** `telemetry.<target>` (Pub-Sub)
*   **Log Publish:** `logs.<target>` (Pub-Sub)
*   **Health Publish:** `health.<target>` (Pub-Sub)
*   **Command Result:** `result.<target>` (Pub-Sub)
*   **Device Status Query:** `status.get.<target>` (Request-Reply)

The `status.get` subject is owned by the downstream device/local agent. The uCentral client publishes request-reply queries to this subject for current device/platform status and upgrade recovery. The uCentral client must not subscribe to or respond on this subject.

*Security Boundary:* Read-only metadata calls (such as capability and status retrieval) are mapped to their own explicit subject namespaces, keeping them separate from destructive/operational `action.*` subjects. The daemon acts as a 1-to-N gateway routing these actions securely to the appropriate downstream agent.

### 2.4 Capability Discovery & Caching Flow
At startup, the client retrieves the hardware capabilities directly from the provisioned file:

```mermaid
sequenceDiagram
    participant GoClient as uCentral Client
    participant OS as Host Machine File System
    participant Cloud as OpenWiFi Cloud

    GoClient->>OS: os.ReadFile(/etc/ucentral/capabilities.json)
    OS-->>GoClient: JSON capability payload
    GoClient->>GoClient: Parse and Cache Capabilities
    GoClient->>Cloud: WebSocket 'connect' with capabilities
```

*   **Caching Strategy:** The capability cache is populated immediately at startup by reading the securely provisioned hardware file. Once loaded, the capabilities are retained in memory and served directly to the Cloud during the WebSocket handshake. If the physical file is missing or unreadable, the client initialization will fail until the hardware is correctly provisioned by the host machine.

### 2.5 JetStream Consistency Model
*   **Desired Configuration:** Leverages JetStream Key-Value (KV) store using `cfg_desired` bucket and key `desired.<serial>`. The client writes the desired state to KV, then publishes a lightweight trigger to the `config.apply` subject.
*   **UUID Match Contract:** The NATS configure trigger must carry the `uuid`, `kv_bucket`, and `kv_key` to allow the downstream agent to fetch the configuration.
    *   The downstream agent retrieves the configuration using `kv_key` and compares the configuration UUID with its last successfully applied UUID.
    *   If the received UUID is strictly greater than the last applied UUID, it is applied.
    *   If the received UUID is less than or equal to the last applied UUID, the agent **aborts the stale trigger**. This prevents applying configs whose trigger publishes failed mid-sequence.
*   **Failure Handling:**
    *   *KV write succeeds but publish fails:* The client returns an error to the cloud. The updated configuration exists in KV but remains unapplied. The cloud control plane is responsible for recovering from this failure state.
    *   *Publish succeeds but downstream agent cannot read KV:* The agent returns a NATS error payload. The Request Manager maps this to a configuration error response back to the cloud.

---

## 3. Workflows & Lifecycle Management

### 3.1 Request Manager Transaction State Machine
Every incoming Cloud command is modeled as an asynchronous transaction tracked by a state machine inside the Request Manager:

```mermaid
stateDiagram-v2
    [*] --> Created : Request admitted

    Created --> PreparingDispatch : Validation succeeded
    Created --> Failed : Validation / setup failure

    PreparingDispatch --> PendingPublish : Payload prepared
    PreparingDispatch --> Failed : KV / preparation / buffer failure
    PreparingDispatch --> Failed : Preparation deadline exceeded

    PendingPublish --> InFlight : Publish or request succeeded
    PendingPublish --> Failed : Publish failure
    PendingPublish --> Failed : Dispatch deadline exceeded

    InFlight --> Completed : Downstream success
    InFlight --> Failed : Downstream failure
    InFlight --> TimedOut : Downstream response deadline exceeded

    Completed --> [*]
    Failed --> [*]
    TimedOut --> [*]
```

*   **Created:** Transaction metadata is initialized in memory. The downstream response timeout is not started yet.
*   **PreparingDispatch:** The transaction is actively performing local setup work (e.g., writing to JetStream KV for `configure`, or building the `ActionCommand` envelope).
*   **PendingPublish:** The payload is fully assembled and is either waiting in the dispatch buffer or in the process of being sent over the wire.
*   **InFlight:** The message is published to NATS. The downstream response timeout starts when the transaction enters this state. The Request Manager listens for the matching `rpc_id` reply.
*   **Completed / Failed / TimedOut:** Terminal states. Once reached, the client replies to all registered cloud streams, updates metrics, caches the result, and cleans up transaction memory.

### 3.2 Concurrency & Serialization Rules
To protect device integrity, the client divides requests into two execution classes:

1.  **State-Changing Commands (Serialized):** Commands that modify device state or execute sensitive operations (`configure`, `reboot`, `factory`, `upgrade`, `certupdate`, `reenroll`, `script`, `leds`) are **serialized** (one at a time per device).
    *   *Rule:* The Request Manager must atomically reserve the state lock during transaction creation. If a serialized transaction is currently `Created`, `PreparingDispatch`, `PendingPublish`, or `InFlight`, any new serialized request (with a different Cloud `id`) is immediately rejected with a `busy` status (Error Code -32603 / Result: `busy`).
    *   *Benefit:* Eliminates the overhead of complex command queue backlogs, ensures strict sequential application of system changes, and guarantees state mutation exclusivity.
2.  **Read-Only Commands (Parallel):** Metadata, query operations, and diagnostics (`status.get`, `ping`, `trace`, `telemetry`, `remote_access`) are processed in parallel.
    *   *Rule:* Query operations do not acquire the device state lock and are published to NATS immediately, even if a configuration transaction is in progress.
    *   *Ownership:* `status.get` is a downstream device/local-agent query. The uCentral client sends the request and waits for the downstream response; it does not serve responses on this subject.

### 3.3 Duplicate & Overlapping Requests
*   **Overlapping Duplicate Requests:** If the Cloud retries a command sending the exact same Cloud `id` while a transaction is already active (`Created`, `PreparingDispatch`, `PendingPublish`, or `InFlight`), the client rejects the new request immediately with a JSON-RPC busy/internal error (`-32603`). This avoids complex fan-out/listener attachment logic and ensures predictable client behavior.
*   **Duplicate Completed Requests:** If the request matches a cached Cloud `id` that has already completed *within the same Cloud session*, the Request Manager **replays the cached response** directly to the cloud. Cross-session replay by ID is prohibited.
*   **Cache TTL by Operation Type (Environment Configurable):**
    *   `configure`, `leds`: **5 minutes**
    *   `reboot`, `remote_access`: **10 minutes**
    *   `factory`, `certupdate`, `reenroll`, `script`: **30 minutes**
    *   `upgrade` (Firmware): **60 minutes from completion**. Firmware upgrades are processed as **asynchronous actions**—the client returns an immediate "started" status and forwards progress updates to the cloud.
    *   `ping`, `trace`, `telemetry`, `status.get`: **2 minutes** (Diagnostics default)

### 3.4 Behavior Across Reconnects
*   **Cloud Disconnections:** If the WAN connection to the Cloud drops while a transaction is `InFlight`, the client **continues running the operation downstream**. It does not abort the configuration or reboot.
*   **Result Recovery:** If NATS replies while the Cloud is disconnected, the client stores the result in its transaction cache. Once the Cloud reconnects, old-session cached responses are *not* replayed merely by reusing the same JSON-RPC ID. The Cloud must use an explicit status query or `operation_id` mechanism to recover the final outcome, or initiate a new transaction.

### 3.5 Asynchronous Upgrades (Firmware)
Firmware upgrades take minutes to complete and cannot block the Request Manager or hold the WebSocket connection open. The architecture defines the following cross-component contracts for upgrades:

1.  **Operation ID:** A persistent `operation_id` must be generated to identify the long-running upgrade process independently of the original JSON-RPC `id` and the NATS `rpc_id`.
2.  **Initial Response:** The client receives the `upgrade` command, validates it, and immediately returns a JSON-RPC "started" response (closing the JSON-RPC exchange) to the Cloud.
3.  **Background Tracking & Authoritative Status:** The upgrade continues executing asynchronously. The downstream agent must expose structured, authoritative upgrade status. The uCentral client must use this structured status to determine completion; **system logs must not be used as a completion signal**.
4.  **State Lock Lifetime:** The state-changing lock (`activeStateTx`) remains held until a defined terminal upgrade state (e.g., success, failure) is received from the downstream agent.
5.  **Reconnection Resilience:** Upon WebSocket reconnection, the daemon resumes reporting the current upgrade state to the Cloud using the persistent `operation_id`.
6.  **Crash Recovery (Startup Query):** To prevent losing the in-memory state lock if the daemon process crashes, the daemon must explicitly load the active operation from the `OperationStore` on boot to recover the Cloud JSON-RPC `id` and **immediately restore the `activeStateTx` lock**. It must then query the downstream agent via `status.get.<target>` generating a **fresh internal `rpc_id`**, correlate the generic response using the locally persisted `operation_id` (as the NATS agent does not track Cloud identities), and release the lock if a terminal state is observed. The uCentral client is only the requester for this subject; it must not subscribe to or respond on `status.get`.
7.  **Duplicate Rejection:** Any state-changing commands received while the background upgrade is active are rejected immediately as busy.

### 3.6 Timeout Specifications
To prevent hanging operations, the Request Manager enforces strict timeout thresholds configurable via environment variables:

*   **Configuration Apply Timeout (`OLG_TIMEOUT_CONFIGURE`):** **30 seconds** default. Applies only to `configure`.
*   **Extended Action Timeout (`OLG_TIMEOUT_ACTION_EXTENDED`):** **120 seconds** default. Applies to long-running actions: `upgrade`, `certupdate`, `script`, and `trace`.
*   **Action Timeout (`OLG_TIMEOUT_ACTION_DEFAULT`):** **60 seconds** default. Applies to all other standard diagnostic and state-changing actions: `reboot`, `factory`, `ping`, `leds`, `telemetry`, `remote_access`, and `status.get`.
*   **Dispatch / NATS Publish Timeout (`OLG_TIMEOUT_DISPATCH`):** **5 seconds** default. Bounds the local preparation and buffer dispatch phases.


### 3.7 Configuration Validation & Application Flow
The validation pipeline splits structural checks from semantic checks:

```mermaid
sequenceDiagram
    participant Cloud as Cloud Gateway
    participant GoClient as uCentral Client
    participant NATS as NATS Bus
    participant VyOS as VyOS NATS Client

    Cloud->>GoClient: WS JSON-RPC request "configure" (uuid, config or compress_64)
    GoClient->>GoClient: Transport Validation (Syntax, Target & Schedule check)
    
    alt Invalid JSON/Target or Future Schedule
        GoClient-->>Cloud: JSON-RPC Error (Invalid Params)
    end
    
    GoClient->>GoClient: In-Memory Schema Validation (Types, ranges, bounds)
    
    alt Schema Validation Fails
        GoClient-->>Cloud: JSON-RPC Error (Validation Error)
    end
    
    GoClient->>GoClient: Write config to JetStream KV
    GoClient->>NATS: Request 'config.apply' (UUID, KV details)
    NATS->>VyOS: Deliver config trigger
    
    VyOS->>VyOS: Retrieve config from JetStream KV
    VyOS->>VyOS: Perform Local Platform-Specific Validation
    
    alt Local Validation Fails
        VyOS-->>NATS: Reply (Result: fail, error: "Unsupported Config Options")
        NATS-->>GoClient: Deliver Failure Result
        GoClient-->>Cloud: JSON-RPC Response (Error)
    else Local Validation Succeeds
        VyOS->>VyOS: Apply & Commit configuration
        
        alt Apply Fails -> Trigger Rollback
            VyOS->>VyOS: Execute local rollback
            VyOS-->>NATS: Reply (Result: rolled_back, active: <good_uuid>)
        else Apply Succeeds
            VyOS-->>NATS: Reply (Result: success)
        end
        
        NATS-->>GoClient: Deliver Result
        GoClient-->>Cloud: JSON-RPC Response
    end
```

#### 3.7.1 Configuration Rollback Reporting
When the downstream agent fails to apply a configuration and triggers a rollback, the status is propagated to the cloud as follows:
*   The downstream agent returns a result containing `result: rolled_back`, the failure details, and the `active` config UUID (the last known good configuration).
*   The uCentral Client translates this into a JSON-RPC response containing error code `-32603` (Internal Error) with `application_code` equal to `5` (Rollback Completed) in the `error.data` object, a message stating `"Configuration apply failed. Rolled back to active configuration UUID <uuid>"`, and lists any offending rejected configuration keys.

### 3.8 Startup & Reconnect Dependency Handling
The client boots and connects to the Cloud and NATS **concurrently and independently** using separate background execution threads. Cloud connectivity does **not** block on NATS. The `DerivedStatus` is continuously derived from the underlying `CloudState` and `NATSState`.

```mermaid
stateDiagram-v2
    state Cloud_Connection_Loop {
        [*] --> CloudConnecting
        CloudConnecting --> CloudConnected : WSS Open & Connect RPC Complete
        CloudConnected --> CloudConnecting : Connection Lost
    }
    
    state NATS_Connection_Loop {
        [*] --> NATSConnecting
        NATSConnecting --> NATSConnected : Connected
        NATSConnected --> NATSConnecting : Connection Lost
    }
    
    note right of Cloud_Connection_Loop
        DerivedStatus is continuously evaluated:

        else if cloudConnected && natsConnected: Operational
        else if !cloudConnected && natsConnected: CloudDegraded
        else if cloudConnected && !natsConnected: NATSDegraded
        else: Connecting
    end note
```

*   **Two-State Machine Philosophy:** The connection lifecycle is driven by strictly **two actual state machines** (the Cloud loop and the NATS loop). The `DerivedStatus` is not a state machine that drives behavior; it is simply a read-only computed projection used for logs, diagnostics, and high-level request circuit-breaking. 
*   **Cloud Connected Semantics:** `CloudConnected` means that both the WSS transport is open AND the uCentral `connect` JSON-RPC exchange has completed successfully. Until that exchange concludes, the Cloud link remains `Connecting`.
*   **Asynchronous Reconnection:** If the Cloud connection drops, the Cloud link transitions to `Connecting`. The derived status becomes `CloudDegraded` when NATS remains connected, or `Connecting` when NATS is also connecting. The client attempts to reconnect to the Cloud in the background using backoff while NATS continues to function. **No daemon restart is required** for intermittent WAN outages.
*   **Independence:** The client attempts to establish a Cloud connection even if NATS is temporarily unavailable or unreachable after startup validation succeeds. Invalid or missing NATS configuration is fatal and prevents both Cloud and NATS loops from starting. While NATS is `Connecting`, the derived status is `NATSDegraded` if Cloud is connected, or `Connecting` if Cloud is also connecting.
*   **Reconnect Backoff (Cloud):** 
    *   *Initial Delay:* **2 seconds**
    *   *Max Delay:* **300 seconds (5 minutes)**
    *   *Multiplier:* **2.0**
    *   *Jitter:* **Randomized Jitter of 10% to 20%** added to backoff intervals to prevent thundering herd issues on the uCentral gateway.
*   **Reconnect Backoff (NATS):** The client delegates the NATS reconnection loop directly to the **NATS library's automatic reconnect policy** rather than managing a manual dial loop. 
    *   *Initial Delay:* **2 seconds**
    *   *Max Delay:* **60 seconds (1 minute)**
    *   *Multiplier:* **2.0**
    *   *Jitter:* **Randomized Jitter of 10% to 20%**
    *   *State Callbacks:* The daemon registers asynchronous NATS `DisconnectErrHandler`, `ReconnectHandler`, and `ClosedHandler` callbacks to update the internal `natsConnected` boolean and immediately trigger the pure `DeriveConnectionState` recalculation.

---

## 4. Traffic Flow & Queue Model

To protect system resources and prioritize messaging, the client utilizes a priority-based queue model:

```text
Incoming NATS/Cloud Traffic
   |
   +---> [Command Dispatch Buffer] ----> (NATS Handoff)
   +---> [Command Result Priority Queue] ----> [WebSocket Outbound Scheduler] <--- (Priority 0)
   +---> [State Coalescer] ----> (Coalesced Flush) ----> [WebSocket Outbound Scheduler] <--- (Priority 2)
   +---> [Telemetry/Log Ring Buffer] ---> (FIFO Queue) -> [WebSocket Outbound Scheduler] <--- (Priority 3)
```

### 4.1 Queue Specifications
Queue sizes are configurable via daemon configuration file parameters.

| Queue Name | Purpose / Type | Capacity | Overflow / Failure Policy |
| :--- | :--- | :--- | :--- |
| **Command Dispatch Buffer** | Short-lived NATS handoff buffer (Not durable). | Default: 100 messages | If full or NATS is disconnected, fails fast and rejects the Cloud request with `local_service_unavailable` (JSON-RPC code -32603, application_code 3). |
| **Command Result Priority Queue** | Handles NATS replies for config/action processing. | Default: 50 messages | High priority and bounded. Because state-changing commands are serialized per device, this queue is not expected to fill during normal operation. Command results must not be silently dropped. If overflow occurs, the client records the overflow metric and logs the `rpc_id`, command type, and subject. The exact correlated downstream result is passed directly to the Request Manager and finalized according to the true downstream outcome. The exact final Cloud response is stored in the TransactionCache. If the response cannot be delivered through the Priority-0 WebSocket queue, the client treats the WebSocket delivery path as unhealthy and triggers path recovery. After reconnection, the Cloud must use explicit state retrieval because cached responses from the dead session will not be replayed automatically upon a simple JSON-RPC ID retry. |
| **State Coalescer** | Keeps only the latest state report (last-write-wins). | 1 Slot per Serial | Overwrites previous un-flushed stats with newer state reports. Dispatched event-driven on NATS arrival as governed by the device configuration (`metrics.statistics.interval`). |
| **Telemetry/Log Ring Buffer** | Bounded FIFO queue for logs and events. | Default: 500 messages | Bounded FIFO. Drops oldest low-priority events on overflow (FIFO drop). Excludes high-severity audit logs. |

### 4.2 WebSocket Outbound Scheduler
Exposes a priority-aware message dispatch queue writing to the WebSocket connection. The scheduler enforces a capacity limit of `capacity` entries *per priority queue*, ensuring that low-priority traffic cannot exhaust scheduler resources or block high-priority responses. Commands responses are prioritized:
*   **Priority 0:** JSON-RPC responses and errors (always bypasses logs/telemetry backlog).
*   **Priority 1:** Critical audits, system crash logs, and health snapshots.
*   **Priority 2:** Coalesced device state reports.
*   **Priority 3:** Standard telemetry events and syslog logs.

**Circuit Breaker on Priority 0 Overflow:** Priority 0 (JSON-RPC responses) bypasses lower-priority backlog but remains bounded by a fixed emergency queue limit to prevent unbounded memory growth when the WebSocket path is stalled. When this limit is exhausted, the daemon must treat the WebSocket writer path as unhealthy, preserve the terminal transaction state and cached response, trigger recovery if needed, and increment an overflow metric.

**Non-Blocking Safety on Priority 1:** To prevent deadlocking the core NATS handler loop during critical downstream operations, pushes to the Priority 1 queue must be strictly non-blocking. If the queue reaches capacity (e.g., due to a stalled WebSocket), it must return a fast error and record an `audit_delivery_failure` metric instead of blocking the caller.
### 4.3 Rate Limiting & Sizing Constraints
*   State updates (statistics) reporting frequency is governed by the Cloud device configuration (`metrics.statistics.interval`, schema minimum 60 seconds; downstream uses last successfully applied value with local 60s fallback when omitted; values below 60 are schema-invalid and rejected).
*   Telemetry events are rate-limited to **50 events per second**.
*   **Max payload sizes:** Configuration: 10MB; State: 1MB; Telemetry events: 256KB; Log events: 64KB. If no valid JSON-RPC request and ID can be parsed before exceeding limits, the connection must be closed/rejected and a metric recorded. If a valid request and ID are parseable but the payload exceeds the limit, return JSON-RPC error -32602 with application code 4.

---

## 5. Security Architecture

### 5.1 NATS Security Configuration
*   **Authentication:** Authenticates with the NATS bus using standard **.creds files** containing an NKEY user seed, as specified in the daemon configuration file.
*   **TLS Requirements:** TLS 1.2+ is enforced on the NATS connection with CA certificates validation.
*   **Authorization & Access Control (ACLs):** The client acts as a 1-to-N gateway and its NATS credentials must authorize it to route messages to/from specific downstream targets:
    *   *Publish:* `cmd.configure.<target>`, `cmd.action.<target>.*`, `capabilities.get.<target>`, `status.get.<target>`, `state.get.<target>`
    *   *Subscribe:* `status.<target>`, `state.<target>`, `telemetry.<target>`, `logs.<target>`, `health.<target>`, `result.<target>`, `_INBOX.>`
    
    *Security Constraint:* NATS credentials MUST explicitly allow publish/subscribe access to the set of authorized downstream targets rather than relying on unrestricted cross-target wildcard access. The daemon delegates target routing decisions to the internal Request Manager and relies on the NATS broker's credentials to physically enforce access boundaries.

### 5.2 Action Command Authorization & Auditing
*   **NATS ACLs:** Only the uCentral client is authorized to publish to `cmd.action.<target>.*`. Downstream agents are prohibited from publishing to these topics.
*   **Audit Logging:** Every sensitive Action Command (e.g., `reboot`) is logged locally in the system audit stream and sent back to the cloud as a high-severity `log` request containing the Cloud JSON-RPC `id` and the user/system identity that triggered it. *(Note: This is a new security feature introduced in this client and is not present in the legacy OpenWrt client).*
*   **Recursive Loop Prevention:** If the audit log forwarding fails, the client increments the `audit_delivery_failure` metric but **does not generate another log** to prevent recursive log flooding.

---

## 6. Operational Observability & Health Reporting (NATS-Native)

To keep the daemon resource-efficient and secure, it does **not** expose local HTTP ports (such as Prometheus or health endpoints) on the router VM. Monitoring and health checks are integrated directly into NATS.

### 6.1 NATS Health Reporting
The downstream device or local device agent, such as the VyOS NATS agent, periodically publishes device health metrics directly to the NATS bus on:
`health.<target>`

The uCentral daemon subscribes to this subject via the NATS transport client. The overarching daemon managers (not the raw NATS client itself) validate and rate-limit the device health payload, and forward accepted health updates to the Cloud through the internal WebSocket outbound scheduler. The payload is a device-health payload produced by the downstream device/local agent. It is distinct from the uCentral client's own NATS connection health snapshot. The exact schema is owned by the downstream device-health contract.

### 6.2 Device Status Query

The `status.get` request-reply subject is owned by the downstream device/local agent:

`status.get.<target>`

The uCentral client sends requests to this subject when it needs current device/platform status, including upgrade state, apply progress, rollback state, firmware version, and operational readiness. This subject is also used during startup crash recovery to determine whether a firmware upgrade is already active downstream.

The uCentral client must not subscribe to or respond on this subject. Its own daemon liveness/readiness, Cloud connectivity, NATS connectivity, queue depths, uptime, and local metrics are internal daemon state and are reported to the Cloud through the WebSocket control path, not through the device-owned NATS status subject.

### 6.3 Device State Reporting (OpenWiFi State Messages)

The uCentral client bridges downstream router operational state (harvested by the VyOS NATS agent) to the OpenWiFi Cloud Gateway (`ra-wlan-cloud-ucentralgw`) over the mTLS WebSocket connection.

#### 6.3.1 Architectural Flow

```text
+---------------------------------------------------------------------------------------------------+
|                           DOWNSTREAM ROUTER AGENT (VyOS VM / Appliance)                           |
|                                                                                                   |
|  [ VyOS Router Metrics (/proc, sysfs, iproute2) ]                                                 |
|       │                                           ▲                                               |
|       ▼ (Periodic collection)                     │ (On-Demand live query)                        |
|  [ Periodic State Collector ]              [ On-Demand Responder (state.get.<target>) ]           |
|       │                                           ▲                                               |
|       ▼                                           │ (Request)                                     |
|  [ NATS: state.<target> ]                  [ NATS: state.get.<target> ]                           |
|  (Publish StateEnvelope)                   (Request-Reply / Live State)                           |
+───────┼───────────────────────────────────────────▲───────────────────────────────────────────────+
        │                                           │
        ▼ (SubscribeState)                          │ (RequestLiveState)
+───────────────────────────────────────────────────┼───────────────────────────────────────────────+
|                                      uCENTRAL CLIENT DAEMON                                       |
|                                                   │                                               |
|  [ pkg/nats Client ]                              │                                               |
|         │ (Periodic StateEnvelope)                │                                               |
|         ▼                                         │                                               |
|  +-----------------------------------+            │                                               |
|  | StateCoalescer (pkg/queues)       |            │                                               |
|  +-----------------------------------+            │                                               |
|         │                                         │                                               |
|         ▼ (Peek & Commit)                         │                                               |
|  [ Periodic State Frame ]                         │ (Live State + request_uuid)                   |
|         │                                         │                                               |
|         ▼ (Priority 2 / Medium)                   ▼ (Priority 0 / Emergency)                      |
|  +---------------------------------------------------------------------------------------------+  |
|  | PriorityScheduler                                                                           |  |
|  +---------------------------------------------------------------------------------------------+  |
|         │                                                                                         |
|         ▼                                                                                         |
|  [ WebSocket Writer Loop (pkg/websocket) ]                                                        |
+─────────┼─────────────────────────────────────────────────────────────────────────────────────────+
          │
          ▼ (mTLS WebSocket: {"method": "state", "params": {...}})
+───────────────────────────────────────────────────────────────────────────────────────────────────+
|                                     OPENWIFI CLOUD GATEWAY                                        |
|                                                                                                   |
|  [ AP_Process_state.cpp ]                                                                         |
|         ├──► Database: Statistics Table                                                           |
|         ├──► Kafka: state topic                                                                   |
|         └──► Resolve command if request_uuid present                                              |
+---------------------------------------------------------------------------------------------------+
```

##### 6.3.1.1 Execution Environment & Metric Scoping (VyOS VM vs. Ubuntu Host)

In two-tier OLG deployments (Ubuntu host hypervisor + VyOS router VM):
* **Execution Boundary:** `vyos-nats-agent` runs inside the VyOS VM (having direct access to VyOS configuration APIs), while `ucentral-client` and `nats-server` run on the Ubuntu host.
* **Metric Scoping:** All collected telemetry reflects the **VyOS Router Appliance** (guest CPU/RAM, router data-plane interfaces, and routed client neighbors), not the underlying Ubuntu bare-metal hypervisor. Host infrastructure monitoring is managed out-of-band and is decoupled from OpenWiFi router state.

**Subject Routing & Separation Rules:**
* **Exclusive `state.<target>` Routing:** Periodic operational state MUST be routed exclusively over `state.<target>`.
* **On-Demand Live Query (`state.get.<target>`):** On-demand state requests from the Cloud Gateway trigger a live NATS request-reply query to `state.get.<target>` for fresh metrics, bypassing the coalescer. The `state.get` subject is owned by the downstream device/local agent; the uCentral client publishes requests to it and must not subscribe to or serve responses on it.
* **Separation from `status.<target>` and `status.get.<target>`:** The existing `status.<target>` and `status.get.<target>` subjects are strictly reserved for lifecycle events and configure/action execution progress. These message types have distinct schemas and purposes; publishing full device-state payloads on `status.<target>` would cause consumers to interpret them as progress/status messages or reject them as invalid.
* **Agentcore Prerequisite:** Support for dedicated `state.<target>` and `state.get.<target>` subject patterns in `agentcore` (`DefaultStatePattern = "state.%s"`, `DefaultStateGetPattern = "state.get.%s"`, `PublishState`, `RegisterStateHandler`, `RequestState`) must be implemented or verified before state subscription and on-demand queries are activated.

#### 6.3.2 Wire Envelope Formats

##### 6.3.2.1 Downstream NATS Wire Format (`agentcore.StateEnvelope`)

On the local NATS bus, the downstream agent publishes state messages exclusively on `state.<target>` wrapped in a standard `agentcore.StateEnvelope`:

```json
{
  "version": "1.0",
  "target": "vyos",
  "uuid": "1793880893",
  "payload": {
    "version": 1,
    "unit": {
      "load": [ 0.43, 0.34, 0.35 ],
      "cpu_load": [ 10, 11, 13, 10, 7 ],
      "localtime": 1793968533,
      "memory": {
        "total": 255991808,
        "free": 147640320,
        "cached": 26488832,
        "buffered": 0
      },
      "uptime": 119483
    },
    "interfaces": [
      {
        "name": "eth0",
        "location": "/interfaces/0",
        "uptime": 23061,
        "ipv4": {
          "addresses": [ "172.16.1.228/21" ],
          "dhcp_server": "172.16.0.1",
          "leasetime": 43200
        },
        "dns_servers": [ "96.45.45.45", "96.45.46.46" ],
        "counters": {
          "rx_bytes": 1051992836,
          "rx_packets": 5440208,
          "tx_bytes": 4403888,
          "tx_packets": 27885,
          "rx_dropped": 206212,
          "rx_errors": 0,
          "tx_dropped": 0,
          "tx_errors": 0,
          "multicast": 884271,
          "collisions": 0
        },
        "delta_counters": {
          "rx_bytes": 659383,
          "rx_packets": 3852,
          "tx_bytes": 4141,
          "tx_packets": 17
        },
        "clients": [
          {
            "mac": "00:09:0f:09:80:04",
            "ipv4_addresses": [ "172.16.0.1" ],
            "ports": [ "eth0" ],
            "last_seen": 0
          }
        ]
      }
    ],
    "link-state": {
      "upstream": {
        "eth0": { "carrier": true, "speed": 1000, "duplex": "full" }
      },
      "downstream": {
        "eth1": { "carrier": false, "speed": 0 }
      }
    },
    "lldp-peers": {
      "upstream": {
        "eth0": [
          {
            "name": "LabSwitch-1",
            "description": "US-16-150W Switch",
            "mac": "fc:ec:da:7c:d8:8a",
            "capability": [ "Bridge" ],
            "management_ips": [ "172.16.0.2" ],
            "port": "eth1"
          }
        ]
      },
      "downstream": {}
    },
    "radios": []
  },
  "timestamp": "2026-05-12T10:30:00Z"
}
```

##### 6.3.2.2 Upstream Cloud WebSocket Wire Format (JSON-RPC 2.0 Notification)

The uCentral client extracts `payload` as `DeviceState`, correlates the applied UUID, and packages periodic reports into standard JSON-RPC 2.0 notifications sent to the Gateway:

```json
{
  "jsonrpc": "2.0",
  "method": "state",
  "params": {
    "serial": "60cf84f22290",
    "uuid": 1793880893,
    "state": {
      "version": 1,
      "unit": {
        "load": [ 0.43, 0.34, 0.35 ],
        "cpu_load": [ 10, 11, 13, 10, 7 ],
        "localtime": 1793968533,
        "memory": {
          "total": 255991808,
          "free": 147640320,
          "cached": 26488832,
          "buffered": 0
        },
        "uptime": 119483
      },
      "interfaces": [
        {
          "name": "eth0",
          "location": "/interfaces/0",
          "uptime": 23061,
          "ipv4": {
            "addresses": [ "172.16.1.228/21" ],
            "dhcp_server": "172.16.0.1",
            "leasetime": 43200
          },
          "dns_servers": [ "96.45.45.45", "96.45.46.46" ],
          "counters": {
            "rx_bytes": 1051992836,
            "rx_packets": 5440208,
            "tx_bytes": 4403888,
            "tx_packets": 27885,
            "rx_dropped": 206212,
            "rx_errors": 0,
            "tx_dropped": 0,
            "tx_errors": 0,
            "multicast": 884271,
            "collisions": 0
          },
          "delta_counters": {
            "rx_bytes": 659383,
            "rx_packets": 3852,
            "tx_bytes": 4141,
            "tx_packets": 17
          },
          "clients": [
            {
              "mac": "00:09:0f:09:80:04",
              "ipv4_addresses": [ "172.16.0.1" ],
              "ports": [ "eth0" ],
              "last_seen": 0
            }
          ]
        }
      ],
      "link-state": {
        "upstream": {
          "eth0": { "carrier": true, "speed": 1000, "duplex": "full" }
        },
        "downstream": {
          "eth1": { "carrier": false, "speed": 0 }
        }
      },
      "lldp-peers": {
        "upstream": {
          "eth0": [
            {
              "name": "LabSwitch-1",
              "description": "US-16-150W Switch",
              "mac": "fc:ec:da:7c:d8:8a",
              "capability": [ "Bridge" ],
              "management_ips": [ "172.16.0.2" ],
              "port": "eth1"
            }
          ]
        },
        "downstream": {}
      },
      "radios": []
    },
    "request_uuid": "optional-request-uuid-when-answering-cloud-query"
  }
}
```

##### 6.3.2.3 Canonical State Identity Contract & UUID Conversion Rules

To eliminate inner/outer identity ambiguity and prevent default zero/empty values (`uuid: 0`, `serial: ""`):

1. **Outer Identity is Authoritative:**
   * **Upstream WebSocket:** Device identity (`serial`) and active configuration version (`uuid`) belong strictly to the outer `params` object (`params.serial` as `string`, `params.uuid` as `uint64`).
   * **Downstream NATS:** Device target identity and active configuration version belong strictly to the outer `StateEnvelope` (`target: string`, `uuid: string`).
   * **Inner State Payload & Schema Preservation (`DeviceState` / `payload`):** Represents pure telemetry (`version`, `unit`, `interfaces`, `radios`, `link-state`, `lldp-peers`). It MUST NOT contain inner `uuid` or `serial` fields. All schema-supported interface fields—specifically including dual-stack `interfaces[].ipv6_addresses`, `interfaces[].ipv6_leases`, `ntp_server`, and optional wireless structures—must be fully preserved during NATS-to-WebSocket transformation. The transformation handles identity separately: stripping inner `serial` and `uuid` if present from the state payload, while authoritatively populating outer `params.serial` and `params.uuid` in `CloudStateParams`. Unmodeled or extended OpenWiFi schema fields must never be dropped or silently discarded during conversion.

2. **Identity Population & Validation:**
   * **`params.serial`:** Populated authoritatively by the uCentral client from its local configuration (`cfg.Cloud.Serial` or detected hardware MAC).
   * **Target Validation:** When `StateEnvelope` arrives from NATS, the client verifies that `envelope.Target == cfg.NATS.Target`. Messages for unknown targets are discarded with a warning log.

3. **`StateEnvelope.UUID` (string) -> Gateway `params.uuid` (uint64) Conversion Rules:**
   Because `agentcore.StateEnvelope.UUID` is a string (standard across NATS agent envelopes) while OpenWiFi uCentral Gateway requires `params.uuid` as a `uint64`, the client applies the following conversion and fallback rules:

   | Downstream `StateEnvelope.UUID` | Converted Gateway `params.uuid` | Behavior & Fallback |
   | :--- | :--- | :--- |
   | **Valid Numeric String** (e.g. `"1793880893"`) | Parsed `uint64` (`1793880893`) | Parsed via `strconv.ParseUint(envelope.UUID, 10, 64)`. Assigned directly to `params.uuid`. |
   | **Missing, Empty, or `"0"`** (`""` or `"0"`) | `0` | Indicates initial boot, factory default, or unapplied configuration prior to controller provisioning. `0` is the standard OpenWiFi representation for unconfigured devices. |
   | **Invalid / Non-Numeric String** (e.g. `"d820572e..."`) | `0` | If `strconv.ParseUint` fails: client maps to `0` (unverified / unconfigured state). To prevent telemetry from falsely associating with a configuration that the router may have failed to apply or rolled back, the client MUST NOT guess or fall back to locally tracked client memory. A structured warning is logged: `invalid non-numeric downstream state UUID; defaulting params.uuid to 0`. |

4. **Downstream Checkpoint & Concurrency Contract:**
   * **Last-Applied Only:** The downstream agent reports only the last successfully applied configuration UUID (`state.State{AppliedUUID string}`). Failed or rolled-back configuration attempts never overwrite the active UUID checkpoint.
   * **Absent vs. `0`:** If the state checkpoint is missing (initial boot) or corrupt, downstream emits empty `UUID: ""`; if the controller configured an explicit factory/baseline, downstream emits `UUID: "0"`. Both map safely to Gateway `params.uuid = 0`.
   * **Atomic Checkpoint Reads:** Downstream state persistence uses atomic temporary file creation and POSIX rename (`os.Rename`), ensuring telemetry collectors never observe torn or partially written checkpoints during concurrent configuration applications.

#### 6.3.3 Schema Alignment & Router Profile Rules

The state payload adheres strictly to `TIP-olg-ucentral-schema` (`ucentral.state.pretty.json`). For router and gateway appliances, the following profile rules apply:

1. **`radios` Array:** Populated with active Wi-Fi radio metrics (channel, frequency, tx_power, noise) if the gateway appliance contains wireless hardware. For purely wired appliances/VMs with no wireless PHYs, it is populated as an empty array `[]` rather than omitted or null. This ensures that the Gateway's `StateUtils::ComputeAssociations()` check executes safely, registering 0 Wi-Fi associations without warnings.
2. **`unit.uptime`:** Must always be provided as an integer (seconds since boot). This field is actively monitored by the Cloud Gateway for device online status.
3. **`interfaces[].ssids` & `interfaces[].mesh-path`:** Populated dynamically when wireless interfaces/VAPs exist on the appliance; omitted or left empty for standard wired Ethernet interfaces.
4. **`clients` (Observed L3 Neighbors):** Represents observed L3 network neighbors rather than an authoritative connected-client inventory. The collector extracts entries from `ip -j neigh`, discarding `FAILED`, `INCOMPLETE`, and `NOARP` states while retaining `REACHABLE`, `PERMANENT`, `STALE`, `DELAY`, and `PROBE`. Neighbors are deduplicated by MAC address, aggregating multiple IPv4/IPv6 addresses into `ipv4_addresses` and `ipv6_addresses`. For bridge interfaces (`br0`), clients are mapped to physical member ports via `bridge -j fdb show` if resolved, or assigned to the logical interface; VLAN interfaces retain their subinterface scoping. Under the OpenWiFi state schema, `last_seen` represents elapsed seconds since last reliably observed activity at state collection time, never a Unix epoch. `0` is used only if fresh activity is actually known, not merely because an ARP/NDP entry is marked `REACHABLE`. Because `ip -j neigh` state alone is not a last-packet timestamp, if activity age is not measurable, this optional field is omitted (`omitempty`) instead of inventing a timestamp or speculative age (no new MAC tracking subsystem is needed in v1). When an authoritative connected inventory is required, the collector reconciles L2 presence (Bridge FDB / Wi-Fi station dumps), L3 DHCP lease allocations, and L3 neighbor activity.
5. **`interfaces[].delta_counters`:** Conforms to OpenWiFi AP baseline tracking. On cold boot or initial interface discovery, deltas are zero-initialized against an established baseline sample. If an interface resets or restarts (`current < previous`), the baseline resets to `current` and emits `0` to prevent `uint64` underflow. On-demand queries (`state.get`) do not mutate or advance the background periodic baseline.
6. **No `null` Objects or Arrays:** Fields must not serialize as `null`; empty collections must be serialized as `[]` or omitted entirely with `omitempty`.
7. **Applied WAN/LAN Topology & Renderer Port Map Alignment (`link-state` & `lldp-peers`):**
   * Physical port WAN/LAN classification is derived strictly from the **last successfully applied configuration** and the renderer's resolved port map (`WithPortMap`, defaulting to `WAN*` $\rightarrow$ `eth0`, `LAN1` $\rightarrow$ `eth1`, `LAN2` $\rightarrow$ `eth2`), rather than guessing by interface names, kernel routing tables, or LLDP neighbor types. Failed or unapplied configurations are never read as authoritative.
   * `link-state.upstream` and `link-state.downstream` contain physical interfaces mapped to their respective roles with carrier, speed, and duplex. LLDP neighbors are attached to the local receiving physical interface under that same role (`lldp-peers.upstream` or `lldp-peers.downstream`). Logical bridges (`br0`) and VLAN subinterfaces (`eth1.100`) remain strictly in the root `interfaces[]` list.
   * All interface keys must match the OpenWiFi schema pattern `^(eth|lan|wan)[0-9]*$`. Unknown, trunk/shared, or unsupported interface names are safely omitted rather than guessed, with diagnostic logging. Empty LLDP tables (`"upstream": {}`, `"downstream": {}`) are valid. Bounded execution is enforced for `lldpcli` polling.

#### 6.3.4 Coalescing & Priority Scheduling (Priority 2 Rules)

State messages are buffered and scheduled through the priority mechanisms defined in this architecture:

* **Last-Write-Wins Generation Tracking:** The `StateCoalescer` maintains an in-memory copy of the latest state payload and an incrementing `generation` counter. Rapid state bursts from the downstream agent overwrite older uncommitted state without memory growth.
* **Event-Driven Push on NATS Arrival & Dynamic Interval Propagation:** Rather than running an independent, unsynchronized polling timer (which causes clock drift and phase lag against downstream collectors), the uCentral client pushes state event-driven on NATS message arrival. Reporting frequency is governed dynamically by the Cloud configuration:
  * **Schema Validation & Forwarding Boundary:** The uCentral client validates all Cloud configuration payloads against `TIP-olg-ucentral-schema` (`ucentral.schema.pretty.json`). Because the schema strictly enforces an integer `minimum: 60` for `metrics.statistics.interval`, any Cloud configuration containing `interval < 60` (including `interval: 0` or negative numbers) is rejected with a schema validation error and is never forwarded to JetStream KV (`desired.<target>`).
  * **Interval Propagation & Downstream Source of Truth:** When a schema-validated configuration is pushed, `ucentral-client` writes the desired configuration to JetStream KV (`desired.<target>`) and signals `cmd.configure.<target>`. The downstream VyOS agent uses the last successfully applied Cloud interval as its authoritative source of truth and resets its collection ticker (`ticker.Reset(interval)`). If the applied Cloud configuration omits `metrics.statistics.interval`, or during fresh boot before any configuration is applied, the downstream agent falls back to its local 60s configuration default (`agent.state.interval: 60s`).
  * **Disablement Policy (Future Extension):** The OpenWiFi schema enforces `minimum: 60` and does not provide an approved schema-valid mechanism (such as an explicit boolean flag) to disable periodic telemetry. Pushing `interval: 0` is not schema-valid. Cloud-driven disablement of periodic state is therefore reserved as a future extension when a schema-approved mechanism is standardized.
  * **On-Demand Query Independence:** On-demand live state queries (`state.get.<target>`) and their correlated Priority 0 command responses remain completely independent of periodic state reporting and interval settings.
  * **Event-Driven Dispatch:** When a `StateEnvelope` arrives from NATS, `StateCoalescer.Update()` updates the slot and increments the generation, and the client attempts to push the state snapshot directly to `PriorityScheduler` at `PriorityMedium` (Priority 2).
* **Best-Effort Delivery & Disconnection Policy:** Periodic state delivery is strictly best-effort. If the Priority 2 scheduler queue is full (`ErrQueueFull`) or the WebSocket connection is down, periodic state messages are dropped as a natural result of disconnection/backpressure rather than buffered for replay. Stale snapshots are never retained across disconnects or replayed upon reconnection; upon link restoration, the client simply transmits the next fresh periodic state snapshot arriving from the downstream agent.

#### 6.3.5 Handling On-Demand Cloud Requests (`request` Method)

The OpenWiFi Cloud Gateway REST API allows operators to request an immediate, on-demand device state report (e.g., via `POST /api/v1/device/{serial}/request` with `message: "state"`):
```json
{
  "jsonrpc": "2.0",
  "id": 1042,
  "method": "request",
  "params": {
    "serial": "60cf84f22290",
    "when": 0,
    "message": "state",
    "request_uuid": "d820572e-3c74-4b47-bcfc-5bbf2f07d2f9"
  }
}
```

To guarantee that operators receive **100% live, fresh metrics** (rather than a potentially stale snapshot from the previous periodic interval), on-demand requests bypass `StateCoalescer` and query the downstream agent directly:

1. **Detection:** When `cmd/ucentral-client/handler.go` receives `method == "request"` and `params.message == "state"`.
2. **Live Downstream Query (`state.get.<target>`) — Single Bounded Attempt (No Retries):**
   - The uCentral client invokes `agentcore.RequestState(ctx, cfg.NATS.Target)`, issuing a single bounded NATS request-reply to `state.get.<target>` using an ephemeral reply inbox (`_INBOX.>`) and a strict 5-second deadline context (`OLG_TIMEOUT_DISPATCH`).
   - **Zero Retries Policy:** To respect caller HTTP deadlines, prevent exceeding the 5-second budget, and avoid retry storms or overload amplification on an active router, on-demand live state queries execute strictly as a **single attempt with zero retries**. If downstream state collection times out, fails, or returns `service_busy`, the client immediately fails fast without retrying.
   - **Wire Protocol & Error Decoding:** The downstream agent replies on the ephemeral reply inbox using `agentcore.StateGetReply`:
     - **Success:** Returns `StateGetReply` wrapping `state: StateEnvelope` containing fresh `/proc`, `sysfs`, and Netlink metrics.
     - **Saturation:** If all 5 worker slots are occupied, returns `StateGetReply` with `error: {"code": "service_busy", "message": "All state collection workers are occupied"}` without queueing.
     - `RequestState()` unwraps successful state or returns typed `*agentcore.StateGetError` (e.g. `CodeServiceBusy`).
3. **Dual Priority 0 Dispatch & Strict Transmission Ordering:**
   - When downstream state collection succeeds, the client dispatches two sequential frames at Priority 0 (`queues.PriorityHighest`) with strict FIFO ordering:
     1. **Frame 1 — JSON-RPC Acknowledgment (Transmitted First):** Transmitted immediately to unblock `RESTAPI_RPC::WaitForCommand()`, matching the Gateway's contract (`result.status` object with `error: 0`):
        ```json
        {
          "jsonrpc": "2.0",
          "id": 1042,
          "result": {
            "serial": "60cf84f22290",
            "uuid": 1793880893,
            "status": {
              "error": 0,
              "text": "Success"
            }
          }
        }
        ```
     2. **Frame 2 — Asynchronous State Notification (Transmitted Second):** Transmitted immediately following the acknowledgment frame, embedding `request_uuid: params.request_uuid` in `params`:
        ```json
        {
          "jsonrpc": "2.0",
          "method": "state",
          "params": {
            "serial": "60cf84f22290",
            "uuid": 1793880893,
            "request_uuid": "d820572e-3c74-4b47-bcfc-5bbf2f07d2f9",
            "state": { ... }
          }
        }
        ```
   - **Ordering Guarantee:** Both messages are queued into **Priority 0 (Highest / Emergency queue)** in strict order (Frame 1 then Frame 2). Because the WebSocket connection is a single ordered TCP stream, the Gateway's socket reader receives and processes the RPC acknowledgment before the state notification. This prevents race conditions where an exceptionally fast state notification could arrive before the command record is established or acknowledged.
4. **Cloud Gateway Completion Semantics (Acknowledgment vs. Asynchronous Result):**
   - **Synchronous REST Completion (The Acknowledgment):** In OpenWiFi Cloud Gateway, `RESTAPI_RPC::WaitForCommand()` specifically waits for the JSON-RPC acknowledgment matching `id: 1042`. When Frame 1 arrives, `AP_WS_Connection::ProcessWSFrame()` dispatches the frame by `id`. Confirming `result.status.error == 0`, `WaitForCommand()` fulfills its promise, immediately unblocking the synchronous HTTP `POST /api/v1/device/{serial}/request` caller with HTTP 200 and the command metadata.
   - **Asynchronous State-Result Storage (The State Notification):** When Frame 2 arrives, `AP_Process_state.cpp` intercepts the `state` notification and extracts `params.request_uuid`. It invokes `StorageService()->SetCommandResult(request_uuid, StateStr)` to persist the telemetry snapshot into the database record for `request_uuid`. **The state notification does not unblock `WaitForCommand()` directly;** it independently updates command storage for subsequent REST polling (`GET /api/v1/device/{serial}/commands/{request_uuid}`).
5. **Periodic Baseline Isolation:** Servicing an on-demand live query on `state.get.<target>` gathers live interface metrics but MUST NOT mutate or advance the downstream agent's periodic delta baseline (`periodic_counters`). The scheduled periodic reporting cadence and its delta counter calculations remain completely undisturbed by on-demand requests.
6. **Gateway-Side Failure Correlation & Unblocking (Failure Path):**
   - **Gateway Frame Contract Constraint:** In the Cloud Gateway, `AP_WS_Connection::ProcessWSFrame()` only dispatches JSON-RPC responses containing `result` and `id` to pending RPC listeners. Responses formatted as bare JSON-RPC `error` objects are NOT routed to `RESTAPI_RPC::WaitForCommand()`. Furthermore, the Gateway correlates commands **exclusively via the integer RPC `id`** in `ProcessWSFrame()` and `WaitForCommand()`; it does not implement payload inspection of `error.data.request_uuid`.
   - **Compatible Correlated Failure Response:** If downstream state collection times out (5-second deadline) or fails, or if the device is busy, the client transmits **only** the JSON-RPC acknowledgment response for `id: 1042` with non-zero `result.status.error`. **No state notification is emitted on failure.**
     - **Downstream Timeout / Unavailable:**
       ```json
       {
         "jsonrpc": "2.0",
         "id": 1042,
         "result": {
           "serial": "60cf84f22290",
           "uuid": 1793880893,
           "status": {
             "error": 1,
             "text": "downstream state collection timed out or unavailable"
           }
         }
       }
       ```
     - **Service Busy (`service_busy`):**
       ```json
       {
         "jsonrpc": "2.0",
         "id": 1042,
         "result": {
           "serial": "60cf84f22290",
           "uuid": 1793880893,
           "status": {
             "error": 2,
             "text": "service busy: maximum concurrent state requests reached"
           }
         }
       }
       ```
   - **Gateway Failure Handling, Storage Persistence & Unblocking:**
     1. **Dispatch:** `AP_WS_Connection::ProcessWSFrame()` inspects the incoming frame. Matching the integer `id: 1042` in `CommandManager::OutStandingRequests_`, it delivers the payload to the waiting promise.
     2. **Evaluation:** `RESTAPI_RPC::WaitForCommand()` awakes from `rpc_future.wait_for()`, parses `result.status.error` (non-zero) and `result.status.text`, and stores them into `Cmd.ErrorCode` and `Cmd.ErrorText`.
     3. **Terminal Database Persistence (`StorageService()->AddCommand`):** Because `Cmd.ErrorCode > 0`, `WaitForCommand()` branches directly into `SetCommandStatus(Cmd, Request, Response, Handler, Storage::CommandExecutionType::COMMAND_FAILED, Logger)`. In `RESTAPI_RPC::SetCommandStatus()`, the Gateway calls `StorageService()->AddCommand(Cmd.SerialNumber, Cmd, COMMAND_FAILED)`, which executes an SQL `INSERT INTO CommandList` (or `UPDATE`) persisting the terminal `Status = "failed"`, `ErrorCode`, `ErrorText`, `Results`, `Executed`, and `Completed` timestamps for `Cmd.UUID` (`request_uuid`).
     4. **No Pending Command Record:** Crucially, for on-demand `request` commands (`message: "state"`), `RESTAPI_device_commandHandler::MakeRequest()` executes with `RetryLater = false` and does not pre-insert any command row prior to `WaitForCommand()`. Therefore, no record ever enters or remains in `COMMAND_PENDING` state. Since no Frame 2 is emitted on failure, `AP_Process_state.cpp` is never invoked, leaving the database record cleanly in its terminal `failed` state.
     5. **Immediate Synchronous Unblocking:** Within `SetCommandStatus()`, `Handler->ReturnObject(RetObj, HTTP_BAD_REQUEST)` immediately returns HTTP 400 Bad Request with the failed command payload to the synchronous REST caller, terminating without hanging for the 120-second Gateway timeout.

##### 6.3.5.1 Admission Control & Priority 0 Scheduling Invariants

To maintain predictable resource utilization across upstream and downstream boundaries:

1. **Execution Capacity Exclusivity (No Mirroring in uCentral):**
   * The downstream VyOS agent alone owns and enforces the configurable 5-worker state-request capacity limit.
   * The uCentral Client does NOT define or mirror a `max_outstanding_state_requests` configuration setting or separate 5-worker semaphore.
2. **Global Request Admission:**
   * The uCentral Client forwards incoming `state.get` requests under its existing global Request Manager admission limit (default: 100 concurrent transactions).
   * If downstream VyOS capacity is saturated (5 active collections), VyOS immediately returns an explicit, correlated error reply over NATS via `agentcore.StateGetReply` with `error: {"code": "service_busy", "message": "All state collection workers are occupied"}` on the ephemeral reply inbox.
   * The uCentral Client decodes this via `agentcore.RequestState()` into typed `*agentcore.StateGetError` with `Code == agentcore.CodeServiceBusy`, and translates it directly into the Cloud Gateway JSON-RPC failure response (`result.status: {"error": 2, "text": "service busy: maximum concurrent state requests reached"}`).
   * Transport errors, collection errors (`collection_failed`), timeouts (`collection_timeout` / 5s deadline), and `nats.ErrNoResponders` translate to Gateway error code `1` (`result.status: {"error": 1, "text": "downstream state collection timed out or unavailable"}`).
3. **Priority Preservation & Scheduling Invariants:**
   * **Periodic Telemetry:** Delivered exclusively via best-effort Priority 2 (`queues.PriorityMedium`), buffered and deduplicated by `StateCoalescer` with last-write-wins generation tracking.
   * **On-Demand Telemetry:** Correlated command acknowledgments and live state notifications are scheduled in Priority 0 (`queues.PriorityHighest`) to ensure immediate dispatch.
   * **Unchanged Priority 0 Overflow Policy:** The existing bounded Priority 0 queue capacity and overflow policy remain unchanged. No state-specific reservations or new priority classes are introduced. The generic Priority 0 limit remains essential because rapid downstream failures and unrelated command results can still create transient message bursts.

---

## 7. Version Compatibility & Negotiation

*   **Coexistence:** Multiple daemon instances can coexist on the same NATS broker. Different implementations subscribe to their specific serial prefix.
*   **Backward Compatibility:** Within a major version namespace, backward compatibility is required.
*   **Version Verification & Fallback:** During the `connect` handshake, the uCentral client transmits its supported architecture capabilities. Because OWGW does not define a formal negotiation exchange, a successful `connect` response is treated as verification success.


---

## 8. Appendix: Enums & Error Mappings

### 8.1 JSON-RPC Error Codes
The client maps internal failures to standard JSON-RPC 2.0 error codes:

| Error Code | Name | Description |
| :--- | :--- | :--- |
| **-32700** | Parse Error | Invalid JSON received by client. |
| **-32600** | Invalid Request | JSON-RPC request is malformed. |
| **-32601** | Method Not Found | The requested JSON-RPC method does not exist. |
| **-32602** | Invalid Params | Target serial mismatch, invalid UUID, or missing parameters. |
| **-32603** | Internal Error | Internal uCentral client daemon exception or application error wrapper. |
| **-32603 (data.application_code = 1)** | Application Error | Configuration apply failed locally on the agent. |
| **-32603 (data.application_code = 2)** | Timeout | Command execution exceeded time limits (NATS timeout). |
| **-32603 (data.application_code = 3)** | Local Service Unavailable| NATS is disconnected or no downstream agent is listening. |
| **-32603 (data.application_code = 4)** | Validation Failed | Schema check failed at the uCentral client validator. |
| **-32603 (data.application_code = 5)** | Rollback Completed | Configuration failed but rollback to previous UUID succeeded. |
| **-32603 (data.application_code = 6)** | Rollback Failed | Configuration failed and rollback to previous UUID also failed. |


### 8.2 Result Enums
All result structures return one of the following standard values:

*   `success`: Command processed and applied successfully.
*   `rejected`: Command failed validation checks (non-destructive).
*   `failure`: Command failed during execution (destructive).
*   `timeout`: Command timed out on NATS or local apply.
*   `rolled_back`: Config apply failed; device successfully rolled back to last good UUID.
*   `rollback_failed`: Config apply failed and rollback also failed.
*   `stale`: A newer configuration revision has overwritten this trigger.
*   `busy`: A transaction is currently executing on this resource.
*   `unsupported`: The requested operation is not supported by the downstream agent.

**Note on Configure Responses:** For the `configure` command, the Request Manager translates the internal NATS `ResultEnvelope` into the strict JSON-RPC schema expected by the gateway. A successful or partially successful `configure` response MUST include the nested `{"serial": "...", "uuid": 1687588800, "status": {"error": 0, "text": "...", "when": 0, "rejected": [...]}}` structure.

**Configure Response Mapping:** The `configure` command requires translating internal results into the OWGW configure response schema:

| Internal result                       | Cloud response                                 |
| ------------------------------------- | ---------------------------------------------- |
| `success`                             | `result.status.error = 0`                      |
| `failure`                             | agent-provided error code; default to 1 if no valid agent error code is supplied |
| `rejected`, substitutions applied     | `result.status.error = 1`                      |
| `rejected`, configuration not applied | `result.status.error = 2`                      |
| NATS unavailable                      | JSON-RPC error                                 |
| timeout                               | JSON-RPC error                                 |
| malformed request                     | JSON-RPC error                                 |
| `rolled_back`                         | JSON-RPC error (-32603, app_code 5) *Extension*|
| `rollback_failed`                     | JSON-RPC error (-32603, app_code 6) *Extension*|

*Note: The rollback application codes are a deliberate OLG project extension beyond standard OWGW configure responses.*

Example successful configure response:
```json
{
  "jsonrpc": "2.0",
  "result": {
    "serial": "001122334455",
    "uuid": 1687588800,
    "status": {
      "error": 0,
      "text": "Configuration applied successfully",
      "when": 0,
      "rejected": []
    }
  },
  "id": 42
}
```

**Reboot Response Mapping:** The `reboot` command requires translating internal results into the OWGW reboot response schema:

| Internal result                       | OWGW response      |
| ------------------------------------- | ------------------ |
| `success`                             | `status.error = 0` |
| `busy` but accepted for later         | `status.error = 1` |
| `rejected` / `unsupported`            | `status.error = 2` |
| NATS unavailable / timeout            | JSON-RPC error     |

**Factory Response Mapping:** The `factory` command requires translating internal results into the OWGW factory response schema:

| Internal result                       | OWGW response      |
| ------------------------------------- | ------------------ |
| `success`                             | `status.error = 0` |
| `busy` but accepted for later         | `status.error = 1` |
| `rejected` / `unsupported`            | `status.error = 2` |
| NATS unavailable / timeout            | JSON-RPC error     |

**Diagnostic Actions Mapping:** For diagnostic and general commands (`trace`, `ping`, `leds`, `telemetry`, and `remote_access`), the daemon will validate the request, remove the `serial` field, and represent it via the `target` parameter of the `ActionCommand`. The remaining command-specific parameters are strictly preserved in `ActionCommand.payload`.

For `remote_access`, the Cloud-facing JSON-RPC method is `remote_access` and `params.method` must equal `rtty`. Internally it is translated to NATS action `rtty`.

For `ping`, the downstream result is translated to the OWGW response containing `serial`, `uuid`, and `deviceUTCTime`. Internal diagnostic fields such as measured latency must not be added to the Cloud response.

The `ResultEnvelope` responses from NATS for other actions are mapped to their highly specific JSON-RPC counterparts rather than using a generic reboot status.

**System and Security Actions Mapping:** For security-sensitive commands (`certupdate`, `reenroll`, `script`), execution policies are highly constrained. They follow the generic `ActionCommand` envelope pattern but require the Request Manager state lock, strictly prohibit automatic retries, mandate sensitive payload scrubbing/redaction from audit logging, and impose explicit execution/script size limits. For `certupdate`, the client validates base64 syntax and calculates the decoded certificate archive size using a bounded decoder. It rejects decoded payloads exceeding 2 MB but must not unpack, inspect, persist, or log the archive. Valid base64 is forwarded to NATS unchanged. Responses are mapped to precise JSON-RPC layouts (e.g., `result_64` or `result_sz` for `script`, and strict `error`/`txt` for `certupdate` and `reenroll`).
