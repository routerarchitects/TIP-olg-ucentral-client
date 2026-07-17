# Technical Specification: uCentral Client Daemon

This document details the code layout, interface signatures, data structures, and protocol contracts for the Go-based uCentral Client daemon (`TIP-olg-ucentral-client`).

---

## 1. Project Directory Structure

The project follows standard Go layout guidelines:

```text
TIP-olg-ucentral-client/
├── go.mod
├── go.sum
├── HIGH_LEVEL_DESIGN.md
├── REQUIREMENTS.md
├── SPEC.md
├── TDD.md
├── README.md
├── cmd/
│   └── ucentral-client/
│       └── main.go                 # App entrypoint & configuration setup
└── pkg/
    ├── contracts/                  # Shared protocol definitions & structures
    │   ├── rpc.go                  # JSON-RPC 2.0 messages & error codes
    │   ├── envelopes.go            # NATS messages (Configure, Action, Result)
    │   └── enums.go                # Result states and connection enums
    ├── queues/                     # Priority queues, buffers, & scheduler
    │   ├── scheduler.go            # Priority Outbound WebSocket Scheduler
    │   ├── buffer.go               # Bounded Ring Buffer & NATS Dispatch Buffer
    │   ├── coalescer.go            # State message coalescer (last-write-wins)
    │   └── results.go              # High-priority bounded result buffer
    ├── reqmgr/                     # Request Manager & Cache
    │   ├── manager.go              # Request lifecycle coordinator
    │   ├── transaction.go          # Transaction state machine
    │   ├── cache.go                # TTL-based transaction cache
    │   └── store.go                # Durable persistent operation store (e.g. SQLite/JSON)
    ├── websocket/                  # Cloud WebSocket connection loop
    │   ├── client.go               # WebSocket reader & writer
    │   └── handler.go              # JSON-RPC parser & dispatcher
    └── nats/                       # NATS connection & client wrapper
        ├── client.go               # NATS connection management & JetStream KV writes
        └── capabilities.go         # Capability discovery Unix socket & cache
```

---

## 2. Phase-by-Phase Technical Specifications

---

### Epic 1: Scaffold & Base Types

#### PR 1.1: Project Skeleton & Shared Contracts
*   **Target File:** `pkg/contracts/rpc.go`, `pkg/contracts/envelopes.go`, `pkg/contracts/enums.go`
*   **JSON-RPC Structures (`pkg/contracts/rpc.go`):**
    ```go
    package contracts

    import "encoding/json"

    // Standard JSON-RPC 2.0 Error Codes
    const (
    	ErrParse             = -32700
    	ErrInvalidRequest    = -32600
    	ErrMethodNotFound    = -32601
    	ErrInvalidParams     = -32602
    	ErrInternal          = -32603 // Maps to Internal / Busy
    )

    // Application Sub-codes (returned in JSON-RPC error.data.application_code)
    const (
    	ErrAppFailure          = 1
    	ErrTimeout             = 2
    	ErrServiceUnavailable  = 3
    	ErrValidationFailed    = 4
    	ErrRollbackSuccess     = 5
    	ErrRollbackFailed      = 6

    )

    type JSONRPCRequest struct {
    	JSONRPC string          `json:"jsonrpc"`
    	Method  string          `json:"method"`
    	Params  json.RawMessage `json:"params"`
    	ID      json.RawMessage `json:"id"`
    }

    type JSONRPCResponse struct {
    	JSONRPC string          `json:"jsonrpc"`
    	Result  json.RawMessage `json:"result,omitempty"`
    	Error   *JSONRPCError   `json:"error,omitempty"`
    	ID      json.RawMessage `json:"id"`
    }

    type JSONRPCError struct {
    	Code    int             `json:"code"`
    	Message string          `json:"message"`
    	Data    json.RawMessage `json:"data,omitempty"`
    }

    // CloudCompressedConfigureRequest represents the outer wrapper for a compressed config.
    // Compression Contract:
    // * The decoded and zlib-decompressed bytes of Compress64 must be reparsed as a CloudConfigureRequest.
    // * compress_sz must match the exact decompressed byte count.
    // * Decompression must abort if the output exceeds 10MB.
    // * Invalid base64 or zlib data MUST return Invalid Params.
    type CloudCompressedConfigureRequest struct {
    	Compress64 string `json:"compress_64"`
    	CompressSz uint32 `json:"compress_sz"`
    }

    // CloudConfigureRequest represents the incoming configure params.
    // Schedule Contract:
    // * The OLG client currently supports only immediate configuration application.
    // * A missing or zero `when` value is accepted. A non-zero future `when` value MUST
    // * be rejected as unsupported rather than silently ignored. This is a deliberate OLG compatibility limitation.
    type CloudConfigureRequest struct {
    	Serial string          `json:"serial"`
    	UUID   int64           `json:"uuid"`
    	When   int64           `json:"when,omitempty"`
    	Config json.RawMessage `json:"config"`
    }

    type ConfigureRejectedParameter struct {
    	Parameter    json.RawMessage `json:"parameter"`
    	Reason       string          `json:"reason"`
    	Substitution json.RawMessage `json:"substitution,omitempty"`
    }

    type CloudConfigureResultStatus struct {
    	Error    int                          `json:"error"`
    	Text     string                       `json:"text"`
    	When     int64                        `json:"when,omitempty"`
    	Rejected []ConfigureRejectedParameter `json:"rejected,omitempty"`
    }

    // CloudConfigureResponse defines the success/status response expected by the gateway.
    // The Request Manager must translate the internal ResultEnvelope into this structure.
    type CloudConfigureResponse struct {
    	Serial string                     `json:"serial"`
    	UUID   int64                     `json:"uuid"`
    	Status CloudConfigureResultStatus `json:"status"`
    }

    // Reboot Schedule Contract:
    // * Only an absent or zero `when` is accepted. Any non-zero `when` value MUST
    // * be rejected as unsupported rather than silently ignored. This is a deliberate OLG compatibility limitation.
    type CloudRebootRequest struct {
    	Serial string `json:"serial"`
    	When   int64  `json:"when,omitempty"`
    }

    type CloudRebootStatus struct {
    	Error int    `json:"error"`
    	Text  string `json:"text"`
    	When  int64  `json:"when"`
    }

    type CloudRebootResponse struct {
    	Serial string            `json:"serial"`
    	Status CloudRebootStatus `json:"status"`
    }

    // Factory Contract:
    // * Only an absent or zero `when` is accepted. Any non-zero `when` value MUST
    // * be rejected as unsupported rather than silently ignored. This is a deliberate OLG compatibility limitation.
    // * keep_redirector must be provided in the JSON and must be exactly 0 or 1. Missing or invalid values MUST return Invalid Params.
    type CloudFactoryRequest struct {
    	Serial         string `json:"serial"`
    	KeepRedirector *int   `json:"keep_redirector"` // Pointer used to distinguish missing from 0
    	When           int64  `json:"when,omitempty"`
    }

    type CloudFactoryStatus struct {
    	Error int    `json:"error"`
    	Text  string `json:"text"`
    	When  int64  `json:"when"`
    }

    type CloudFactoryResponse struct {
    	Serial string             `json:"serial"`
    	Status CloudFactoryStatus `json:"status"`
    }

    // Diagnostic Command Structures:

    // CloudUpgradeRequest represents the incoming upgrade params.
    type CloudUpgradeRequest struct {
        Serial      string `json:"serial"`
        URI         string `json:"uri"`
        FWsignature string `json:"FWsignature,omitempty"`
        When        int64  `json:"when,omitempty"`
    }

    // CloudUpgradeStatus standard error meanings:
    // 0: Accepted/started
    // 1: Firmware invalid or rejected
    // 2: Required signature absent
    type CloudUpgradeStatus struct {
        Error int    `json:"error"`
        Text  string `json:"text"`
        When  int64  `json:"when"`
    }

    // CloudUpgradeResponse represents the immediate "started" response expected by OWGW.
    type CloudUpgradeResponse struct {
        Serial string             `json:"serial"`
        Status CloudUpgradeStatus `json:"status"`
    }

    // CloudUpgradeProgressNotification represents the full JSON-RPC notification payload (Optional OLG extension).
    // It must only be sent when the connected gateway explicitly advertises support for that extension.
    // The original JSON-RPC ID is included in the params so the Cloud can correlate it.
    type CloudUpgradeProgressNotification struct {
        JSONRPC string                                       `json:"jsonrpc"` // Must be "2.0"
        Method  string                                       `json:"method"`  // E.g. "upgrade_progress"
        Params  CloudUpgradeProgressNotificationParams       `json:"params"`
    }

    type CloudUpgradeProgressNotificationParams struct {
        Serial      string          `json:"serial"`
        ID          json.RawMessage `json:"id"` // Original Cloud JSON-RPC ID
        OperationID string          `json:"operation_id"`
        Stage       string          `json:"stage"`
        Status      string          `json:"status"`
        Message     string          `json:"message"`
    }

    type CloudTraceRequest struct {
    	Serial    string `json:"serial"`
    	When      int64  `json:"when,omitempty"`
    	Duration  *int   `json:"duration,omitempty"`
    	Packets   *int   `json:"packets,omitempty"`
    	Network   string `json:"network,omitempty"`
    	Interface string `json:"interface,omitempty"`
    	URI       string `json:"uri,omitempty"`
    }
    type CloudTraceStatus struct {
    	Error int    `json:"error"`
    	Text  string `json:"text"`
    	When  int64  `json:"when,omitempty"`
    }
    type CloudTraceResponse struct {
    	Serial string           `json:"serial"`
    	Status CloudTraceStatus `json:"status"`
    }

    type CloudPingRequest struct {
    	Serial string `json:"serial"`
    }
    type CloudPingResponse struct {
    	Serial        string `json:"serial"`
    	UUID          int64  `json:"uuid"`
    	DeviceUTCTime int64  `json:"deviceUTCTime"`
    }

    type CloudLedsRequest struct {
    	Serial   string `json:"serial"`
    	When     int64  `json:"when,omitempty"`
    	Duration *int   `json:"duration,omitempty"`
    	Pattern  string `json:"pattern"`
    }
    type CloudLedsStatus struct {
    	Error int    `json:"error"`
    	Text  string `json:"text"`
    }
    type CloudLedsResponse struct {
    	Serial string          `json:"serial"`
    	Status CloudLedsStatus `json:"status"`
    }

    // CloudTelemetryRequest stream configuration.
    // Validation rules:
    // - Interval: integer from 0 through 60 (0 stops the stream).
    // - Types: exactly 1 entry. The only supported type is "dhcp".
    type CloudTelemetryRequest struct {
    	Serial   string   `json:"serial"`
    	Interval *int     `json:"interval,omitempty"`
    	Types    []string `json:"types,omitempty"`
    }
    type CloudTelemetryStatus struct {
    	Error int    `json:"error"`
    	Text  string `json:"text"`
    }
    type CloudTelemetryResponse struct {
    	Serial string               `json:"serial"`
    	Status CloudTelemetryStatus `json:"status"`
    }

    type CloudTelemetryEvent struct {
    	JSONRPC string `json:"jsonrpc"`
    	Method  string `json:"method"`
    	Params  struct {
    		Serial string          `json:"serial"`
    		Data   json.RawMessage `json:"data"`
    	} `json:"params"`
    }

    type CloudRemoteAccessRequest struct {
    	Method  string `json:"method,omitempty"` // Must be validated as "rtty"
    	Serial  string `json:"serial"`
    	Token   string `json:"token"`
    	ID      string `json:"id"`
    	Server  string `json:"server"`
    	Port    int    `json:"port"`
    	User    string `json:"user,omitempty"`
    	Timeout *int   `json:"timeout,omitempty"`
    }
    type CloudRemoteAccessStatus struct {
    	Error int             `json:"error"`
    	Text  string          `json:"text"`
    	Meta  json.RawMessage `json:"meta,omitempty"`
    }
    type CloudRemoteAccessResponse struct {
    	Serial string                  `json:"serial"`
    	Status CloudRemoteAccessStatus `json:"status"`
    }

    type CloudCertupdateRequest struct {
    	Serial       string `json:"serial"`
    	Certificates string `json:"certificates"`
    }
    type CloudCertupdateStatus struct {
    	Error int    `json:"error"`
    	Txt   string `json:"txt"`
    }
    type CloudCertupdateResponse struct {
    	Serial string                `json:"serial"`
    	Status CloudCertupdateStatus `json:"status"`
    }

    type CloudReenrollRequest struct {
    	Serial string `json:"serial"`
    	When   int64  `json:"when,omitempty"`
    }
    type CloudReenrollStatus struct {
    	Error int    `json:"error"`
    	Txt   string `json:"txt"`
    }
    type CloudReenrollResponse struct {
    	Serial string              `json:"serial"`
    	Status CloudReenrollStatus `json:"status"`
    }

    type CloudScriptRequest struct {
    	Serial    string `json:"serial"`
    	Type      string `json:"type"`
    	Script    string `json:"script,omitempty"` // Must be strictly validated as base64
    	Timeout   *int   `json:"timeout,omitempty"`
    	URI       string `json:"uri,omitempty"`
    	Signature string `json:"signature,omitempty"`
    	When      int64  `json:"when,omitempty"`
    }
    type CloudScriptStatus struct {
    	Error    int    `json:"error"`
    	Result64 string `json:"result_64,omitempty"`
    	ResultSz *int   `json:"result_sz,omitempty"`
    	Result   string `json:"result,omitempty"`
    }
    type CloudScriptResponse struct {
    	Serial string            `json:"serial"`
    	Status CloudScriptStatus `json:"status"`
    }
    ```

*   **NATS Envelope Structures (`pkg/contracts/envelopes.go`):**
    ```go
    package contracts

    import "encoding/json"

    type ConfigureCommand struct {
        Version     string          `json:"version"`
        CorrelationID string          `json:"correlation_id"`
        Target      string          `json:"target"`
        UUID        int64          `json:"uuid"`
        KVKey       string          `json:"kv_key"`
        KVRevision  uint64          `json:"kv_revision"`
        Timestamp   string          `json:"timestamp"`
    }

    func (c *ConfigureCommand) Validate() error

    type ActionCommand struct {
    	Version     string          `json:"version"`
    	CorrelationID string          `json:"correlation_id"`
    	OperationID string          `json:"operation_id,omitempty"`
    	Target      string          `json:"target"`
    	CommandType string          `json:"command_type"`
    	Action      string          `json:"action"`
    	Payload     json.RawMessage `json:"payload"`
    	Timestamp   string          `json:"timestamp"`
    }

    // Validate enforces that all required fields are present. If Action == "upgrade", OperationID must be non-empty.
    func (c *ActionCommand) Validate() error


    // DeviceCapabilities represents the parsed result of a capabilities query.
    type DeviceCapabilities struct {
        Capabilities json.RawMessage `json:"capabilities"`
        Firmware     string          `json:"firmware"`
    }

    type DeviceStatus struct {
    	Version     string          `json:"version"`
    	CorrelationID string          `json:"correlation_id,omitempty"`
    	OperationID string          `json:"operation_id,omitempty"` // Identifies the long-running async operation
    	Target      string          `json:"target"`
    	Operation string          `json:"operation,omitempty"`
    	Active    bool            `json:"active,omitempty"`
    	Stage     string          `json:"stage,omitempty"`
    	Status    string          `json:"status"`
    	Message   string          `json:"message,omitempty"`
    	Timestamp   string          `json:"timestamp"`
    }
    // Note: If a response relates to a specific upgrade operation, OperationID must be non-empty, even if it is a terminal state (Active=false). A response with Active=true and an empty OperationID is invalid and must trigger the indeterminate recovery behavior defined by REQ-011.

    type ResultEnvelope struct {
    	Version     string          `json:"version"`
    	CorrelationID string          `json:"correlation_id"`
    	Target      string          `json:"target"`
    	CommandType string          `json:"command_type"`
    	OperationID string          `json:"operation_id,omitempty"` // Mandatory for upgrade results
    	UUID        int64          `json:"uuid,omitempty"` // Omitted for Action
    	Result      ResultType      `json:"result"`
    	Message     string          `json:"message"`
    	Payload     json.RawMessage `json:"payload,omitempty"` // Command-specific data (e.g. latency, result_64)
    	Timestamp   string          `json:"timestamp"`
    }

    func (r *ResultEnvelope) Validate() error

    type CloudCapabilitiesQuery struct {
    	Version       string `json:"version"`
    	CorrelationID string `json:"correlation_id"`
    	Target        string `json:"target"`
    	CommandType   string `json:"command_type"`
    	Action        string `json:"action"`
    	Timestamp     string `json:"timestamp"`
    }

    type CloudDeviceStatusQuery struct {
    	Version       string `json:"version"`
    	CorrelationID string `json:"correlation_id"`
    	OperationID   string `json:"operation_id,omitempty"`
    	Target        string `json:"target"`
    	CommandType   string `json:"command_type"`
    	Action        string `json:"action"`
    	Timestamp     string `json:"timestamp"`
    }
    // Note: For upgrade results, operation_id is mandatory. For non-upgrade commands, operation_id may be omitted.
    ```

*   **Enums (`pkg/contracts/enums.go`):**
    ```go
    package contracts

        type ResultType string

    const (
    	ResultSuccess        ResultType = "success"
    	ResultRejected       ResultType = "rejected"
    	ResultFailed         ResultType = "failed"
    	ResultTimeout        ResultType = "timeout"
    	ResultRolledBack     ResultType = "rolled_back"
    	ResultRollbackFailed ResultType = "rollback_failed"
    	ResultStale          ResultType = "stale"
    	ResultBusy           ResultType = "busy"
    	ResultUnsupported    ResultType = "unsupported"
    )

    type ConnectionState string

    const (
    	StateOffline         ConnectionState = "offline"
    	StateOperational     ConnectionState = "operational"
    	StateCloudDegraded   ConnectionState = "cloud_degraded"
    	StateNATSDegraded    ConnectionState = "nats_degraded"
    	StateProtocolFailure ConnectionState = "protocol_failure"
    )

    type LinkState string

    const (
    	LinkOffline    LinkState = "offline"
    	LinkConnecting LinkState = "connecting"
    	LinkConnected  LinkState = "connected"
    )

    type ConnectionStatus struct {
    	Cloud       LinkState
    	NATS        LinkState
    	Global      ConnectionState
    }
    ```

---

### Epic 2: Traffic Queues & Priority Scheduler

#### PR 2.1: Priority Outbound Scheduler
*   **Target File:** `pkg/queues/scheduler.go`
*   **API & Core Structures:**
    ```go
    package queues

    import (
    	"context"
    	"sync"
    )

    type Priority int

    const (
    	PriorityHighest Priority = 0 // JSON-RPC command responses
    	PriorityHigh    Priority = 1 // Audit logs, crashlogs, health snapshots
    	PriorityMedium  Priority = 2 // Coalesced states
    	PriorityLow     Priority = 3 // Telemetry events, standard logs
    )

    type OutboundMessage struct {
    	Priority Priority
    	Payload  []byte
    }

    // OutboundScheduler defines the priority outbound queue interface.
    // - Pushes append the message to the corresponding priority queue.
    // - ALL PUSHES ARE STRICTLY NON-BLOCKING. If any queue (Priority 0, 1, 2, or 3) reaches its maximum capacity, Push() must immediately return ErrQueueFull. 
    // - Note on Ownership: The scheduler itself does not implement LIFO overwriting or FIFO dropping policies. Those rate-limiting policies are exclusively owned and executed by the upstream StateCoalescer and TelemetryRingBuffer before data is ever pushed to the scheduler.
    // - For Priority 0, if Push() returns ErrQueueFull, the caller must preserve the terminal transaction state and cached response, treat the WebSocket writer path as unhealthy, trigger path recovery, and record an overflow metric.
    // - For Priority 1, if Push() returns ErrQueueFull, the caller must return immediately, increment audit_delivery_failure, and must not generate another audit message.
    // - For Priority 2, if Push() returns ErrQueueFull, the caller must do nothing; the state remains in the upstream StateCoalescer for the next flush.
    // - For Priority 3, if Push() returns ErrQueueFull, the caller must drop the payload and record a dropped_by_reason.scheduler_full metric.
    // - Next() blocks until a message is available or the context is canceled.
    //   Highest priority messages (0) are selected first, but to prevent starvation of lower priorities,
    //   a strict yield mechanism is enforced: after 10 consecutive Priority 0 messages are yielded, 
    //   the scheduler must yield at least one available message from the next highest populated queue (1, 2, or 3).
    // - Context cancellation drives scheduler shutdown and unblocks waiting Next calls.
    type OutboundScheduler interface {
    	Push(msg OutboundMessage) error
    	Next(ctx context.Context) (OutboundMessage, error)
    }

    type PriorityScheduler struct {
    	mu           sync.Mutex
    	cond         *sync.Cond
    	queues       [4][][]byte
    	capacity     int // maximum entries for Priority 1, 2, and 3
    	emergencyCap int // maximum entries for the Priority 0 emergency queue
    }

    func NewPriorityScheduler(capacity int, emergencyCap int) *PriorityScheduler {
    	s := &PriorityScheduler{
    		capacity:     capacity,
    		emergencyCap: emergencyCap,
    	}
    	s.cond = sync.NewCond(&s.mu)
    	return s
    }

    func (s *PriorityScheduler) Push(msg OutboundMessage) error
    func (s *PriorityScheduler) Next(ctx context.Context) (OutboundMessage, error)
    ```

#### PR 2.2: Buffers, Coalescer & Telemetry Ring Buffer
*   **Target File:** `pkg/queues/buffer.go`, `pkg/queues/coalescer.go`, `pkg/queues/results.go`
*   **Core Structures:**
    ```go
    package queues

    import (
    	"context"
    	"sync"
    )

    // TelemetryRingBuffer represents a bounded FIFO queue for low-priority telemetry
    type TelemetryRingBuffer struct {
    	mu       sync.Mutex
    	buffer   [][]byte
    	capacity int
    	head     int
    	tail     int
    	size     int
    }

    func NewTelemetryRingBuffer(capacity int) *TelemetryRingBuffer
    func (b *TelemetryRingBuffer) Push(payload []byte) (dropped bool)
    func (b *TelemetryRingBuffer) Pop() ([]byte, bool)

    // StateCoalescer implements last-write-wins in-memory state storage with generation tracking
    type StateSnapshot struct {
    	Payload    []byte
    	Generation uint64
    }

    type StateCoalescer struct {
    	mu          sync.Mutex
    	latestState []byte
    	generation  uint64
    	hasState    bool
    }

    func NewStateCoalescer() *StateCoalescer
    func (c *StateCoalescer) Update(payload []byte)
    func (c *StateCoalescer) Peek() (StateSnapshot, bool)
    func (c *StateCoalescer) Commit(generation uint64) bool

    // NATSDispatchBuffer buffers commands headed for NATS. Rejects immediately when full.
    // The caller (e.g., Request Manager) MUST explicitly verify that NATS is connected 
    // before calling Push(), returning local_service_unavailable if NATS is offline.
    type NATSDispatchBuffer struct {
    	ch chan []byte
    }

    func NewNATSDispatchBuffer(capacity int) *NATSDispatchBuffer
    func (d *NATSDispatchBuffer) Push(payload []byte) error
    func (d *NATSDispatchBuffer) Pop(ctx context.Context) ([]byte, error)

    // ErrQueueFull is returned when a push fails due to the non-blocking capacity limit.
    var ErrQueueFull = errors.New("queue is at maximum capacity")

    // CommandResultQueue acts as a bounded, high-priority ingress buffer for JSON-RPC 
    // command execution results arriving from the downstream NATS agents.
    type CommandResultQueue struct {
    	mu       sync.Mutex
    	items    [][]byte
    	capacity int
    }

    func NewCommandResultQueue(capacity int) *CommandResultQueue
    func (q *CommandResultQueue) Push(payload []byte) error
    func (q *CommandResultQueue) Pop() ([]byte, bool)
    func (q *CommandResultQueue) Utilization() float64
    ```

**Command Result Queue Lifecycle & Ownership Rules:**
*   **Ownership:** The queue is populated (`Push`) by the asynchronous NATS Subscriber goroutines. It is consumed (`Pop`) exclusively by a dedicated Request Manager processing loop. The Request Manager must correlate the NATS result, transition the transaction state, release any held locks, cache the final response, and then push the finalized JSON-RPC response payload into the Priority 0 Outbound Scheduler.
* **Overflow Policy (Exceptional Local Delivery Failure):** Before attempting to enqueue a command result, the NATS subscriber must decode the existing `ResultEnvelope` sufficiently to obtain its `correlation_id`, command type, and subject context. It then calls `CommandResultQueue.Push(payload)`.

If `Push()` returns `ErrQueueFull`, the subscriber must not silently discard the correlated result or wait for the transaction timeout. Using the already-extracted `correlation_id`, it must:
1. Record the `command_result_overflow` metric.
2. Log the correlation ID, command type, and NATS subject.
3. Pass the exact, original JSON response payload into the normal `RequestManager.Complete()` or `Fail()` methods to finalize the transaction according to the true device outcome.

The Request Manager MUST NOT rewrite the transaction state to Failed and MUST NOT cache a generated `-32603` failure. The exact original downstream response must be preserved in the `TransactionCache`. When `Complete()` or `Fail()` subsequently attempt to enqueue the response to the Priority-0 WebSocket scheduler, and if that queue is also full, the Request Manager must trigger Priority-0 WebSocket path recovery (e.g. WebSocket reconnect). When the Cloud reconnects and retries the command, it will receive the exact original cached response.

If the result payload cannot be decoded or its `correlation_id` does not match an active transaction, it may be discarded only after logging and metric emission.
*   **Telemetry and Log Throttling (Activation & Release):** The Main loop polls `Utilization()` before processing telemetry or standard logs.
    *   **Activation:** If `Utilization() >= 0.90` (90% capacity, e.g., 45/50 items), the daemon engages throttling, pausing all reads of both telemetry and standard logs from the `TelemetryRingBuffer` (which is shared by both streams).
    *   **Release:** Throttling remains engaged until `Utilization() <= 0.50` (queue drops to 50% capacity), creating a hysteresis loop to prevent rapid toggling, at which point telemetry and log forwarding resumes.

---

### Epic 3: Request Manager & Caching

#### PR 3.1: Transaction State Machine & Manager
*   **Target File:** `pkg/reqmgr/transaction.go`, `pkg/reqmgr/manager.go`
*   **Core Structures:**
    ```go
    package reqmgr

    import (
    	"context"
    	"sync"
    	"time"
    )

    // TransactionState represents the lifecycle phase of a request.
    // The Request Manager API must strictly enforce the following valid transitions:
    // 
    // | Current State         | Allowed Next States                |
    // |-----------------------|------------------------------------|
    // | TxCreated             | TxPreparingDispatch, TxFailed      |
    // | TxPreparingDispatch   | TxPendingPublish, TxFailed         |
    // | TxPendingPublish      | TxInFlight, TxFailed               |
    // | TxInFlight            | TxCompleted, TxFailed, TxTimedOut  |
    //
    // Any attempt to transition an unknown/missing transaction, or to perform an
    // illegal transition (e.g., TxCreated directly to TxCompleted, or calling
    // Fail() on a transaction that is already in a terminal state) must be 
    // rejected by the API returning an error, and logged as an internal assertion failure.
    type TransactionState int

    const (
    	TxCreated TransactionState = iota
    	TxPreparingDispatch
    	TxPendingPublish
    	TxInFlight
    	TxCompleted
    	TxFailed
    	TxTimedOut
    )

    type Transaction struct {
    	CorrelationID    string
    	CloudRPCID       json.RawMessage
    	RequestKey       string // method:canonicalID (e.g. "configure:number:42")
    	RespondToCloud   bool
    	Method           string
    	State            TransactionState
    	CreatedAt        time.Time
    	TimeoutDuration  time.Duration
    	DispatchDeadline time.Time
    	DispatchTimer    *time.Timer
    	Cancel           context.CancelFunc
    }

    // DispatchItem represents a payload waiting in the internal dispatch buffer.
    // The consumer MUST verify that time.Now() is before the transaction's DispatchDeadline,
    // the transaction still exists, and the transaction is still in TxPendingPublish state
    // before calling NATS Publish.
    type DispatchItem struct {
    	CorrelationID string
    	Payload       []byte
    }

    type DefaultRequestManager struct {
    	mu                          sync.Mutex
    	dispatchTimeout             time.Duration           // Bounded deadline for TxPreparingDispatch and TxPendingPublish (from OLG_TIMEOUT_DISPATCH)
    	transactionsByCorrelationID map[string]*Transaction // Key: CorrelationID
    	activeCloudRequests         map[string]string       // Key: RequestKey, Value: CorrelationID
    	stateLock                   sync.Mutex              // Enforces serialized state-changing commands
    	activeStateTx               string                  // CorrelationID or OperationID holding the state lock
    	cache                       *TransactionCache
    	scheduler                   *PriorityScheduler
    	store                       OperationStore
    	pendingReplies              map[string][]byte       // Key: CorrelationID
    }

    // CanonicalRequestKey formats the method and raw JSON-RPC ID into a strongly-typed string (e.g., "configure:number:42")
    // to strictly prevent collisions across methods or numeric/string IDs. This key MUST be used by
    // activeCloudRequests, TransactionCache, duplicate-active detection, and completed-response replay.
    func CanonicalRequestKey(method string, id json.RawMessage) (string, error)

    func NewRequestManager(dispatchTimeout time.Duration, cache *TransactionCache, scheduler *PriorityScheduler, store OperationStore) *DefaultRequestManager
    
    // CreateTransaction creates a new transaction.
    // The Request Manager must canonicalize the incoming Cloud JSON-RPC ID and enforce the following order:
    // 1. If a valid, unexpired entry exists in the TransactionCache, replay it and do NOT create a transaction.
    // 2. If the RequestKey matches an active transaction in Created, PreparingDispatch, PendingPublish, or InFlight, reject with JSON-RPC -32603 busy.
    // 3. If isStateChanging is true:
    //    a. If respondToCloud is false (JSON-RPC notification), reject the request immediately. State-changing commands MUST have an ID.
    //    b. Check if the stateLock is available. If it is already held by another active transaction or background operation, reject with JSON-RPC -32603 busy.
    // 4. Otherwise, atomically acquire/reserve the stateLock (if isStateChanging), create the new transaction, and enter Created.
    // The cache lookup, active-map lookup, state-lock reservation, correlation ID generation, and transaction creation
    // must be performed atomically under one Request Manager synchronization boundary. To avoid deadlocks, the lock ordering
    // must be: acquire `DefaultRequestManager.mu` first, then call `TransactionCache.Get` (which acquires the cache RWMutex).
    // If respondToCloud is false (e.g. for notifications), the implementation MUST NOT insert an empty/null Cloud ID into activeCloudRequests or TransactionCache.
    // CreateTransaction records the configured downstream timeout duration, sets DispatchDeadline
    // using the manager's configured dispatchTimeout, and initializes the DispatchTimer. The DispatchTimer callback MUST 
    // atomically verify the transaction identity and transition it to Failed if it expires before MarkInFlight is called.
    func (m *DefaultRequestManager) CreateTransaction(cloudRPCID json.RawMessage, respondToCloud bool, method string, timeout time.Duration, isStateChanging bool) (*Transaction, error)
    // MarkPreparingDispatch transitions the transaction from TxCreated to TxPreparingDispatch.
    func (m *DefaultRequestManager) MarkPreparingDispatch(correlationID string) error
    // MarkPendingPublish transitions the transaction from TxPreparingDispatch to TxPendingPublish.
    func (m *DefaultRequestManager) MarkPendingPublish(correlationID string) error
    // MarkInFlight is the final step of action dispatch. The dispatch sequence MUST be:
    // (1) Create/register transaction, (2) Install/register NATS reply inbox subscription,
    // (3) Prepare NATS command, (4) Publish to NATS successfully, (5) Call MarkInFlight.
    // MarkInFlight atomically transitions from TxPendingPublish to TxInFlight, stops/invalidates the DispatchTimer, 
    // and starts the downstream response timer.
    // Timeout is invalid in TxCreated, TxPreparingDispatch, and TxPendingPublish.
    // If a fast reply was buffered in pendingReplies during TxPendingPublish, MarkInFlight MUST immediately submit it to 
    // the terminal processing sequence after safely entering TxInFlight.
    func (m *DefaultRequestManager) MarkInFlight(correlationID string) error
    // Terminal methods (Complete, Fail, Timeout) perform terminal processing as an atomic logical sequence:
    // (1) Acquire the Request Manager mutex. Evaluate transition legality. If already terminal, return ErrAlreadyTerminal (an expected race).
    // (1b) If the transaction is in TxPendingPublish, store the response in pendingReplies, return nil, and DO NOT proceed.
    // (2) Immediately mark the transaction state as terminal to win the race.
    // (3) Translate the downstream result and build the exact final Cloud response.
    // (4) Store the response in TransactionCache (only if RespondToCloud=true and RequestKey is valid), determining the correct TTL by calling `TTLForMethod(transaction.Method)`.
    // (5) Remove active indexes (activeCloudRequests, transactionsByCorrelationID) and release the activeStateTx lock if held by this correlationID.
    // (6) Release the Request Manager mutex.
    // (7) Reserve/enqueue Priority-0 delivery of the cached response. If reservation fails, DO NOT alter the transaction state. The true device outcome must be preserved. Simply trigger path recovery.
    // These methods are concurrency-safe and may be invoked by the dedicated result-processing loop,
    // by a NATS subscriber, or by the timeout timer.
    func (m *DefaultRequestManager) Complete(correlationID string, response []byte) error
    // RespondAndRetain separates the synchronous JSON-RPC transaction from a persistent background operation (e.g. upgrade).
    // It MUST follow this sequence under the Request Manager mutex: (1) Validate the transaction is valid for retention (e.g. upgrade).
    // (2) Persist the OperationStore record. (3) Transfer state-changing reservation ownership from CorrelationID to OperationID.
    // (4) Cancel the ordinary response timer. (5) Cache the initial "started" response. (6) Remove the JSON-RPC transaction from active maps and mark it completed. (7) Release mutex and enqueue response.
    func (m *DefaultRequestManager) RespondAndRetain(correlationID string, response []byte) error

    func (m *DefaultRequestManager) Fail(correlationID string, errResponse []byte) error
    func (m *DefaultRequestManager) Timeout(correlationID string) error

#### PR 3.2: Duplicate Attachment & Cache TTL
*   **Target File:** `pkg/reqmgr/cache.go`, `pkg/reqmgr/store.go`, `pkg/reqmgr/manager.go` (extensions)
*   **Core Cache Structures:**
    ```go
    package reqmgr

    import (
    	"encoding/json"
    	"sync"
    )

    type CacheEntry struct {
    	Payload   []byte
    	ExpiresAt int64
    }

    type TransactionCache struct {
    	mu    sync.RWMutex
    	items map[string]CacheEntry
    }

    func NewTransactionCache() *TransactionCache
    func (c *TransactionCache) Set(canonicalCloudID string, payload []byte, ttlSeconds int)
    func (c *TransactionCache) Get(canonicalCloudID string) ([]byte, bool)

    type PersistentOperation struct {
    	OperationID string          `json:"operation_id"`
    	CorrelationID string          `json:"correlation_id"`
    	CloudRPCID    json.RawMessage `json:"cloud_rpc_id"`
    	Target      string          `json:"target"`
    	Action      string          `json:"action"`
    	Stage       string          `json:"stage"`
    	Status      string          `json:"status"`
    	Active      bool            `json:"active"`
    	CreatedAt   string          `json:"created_at"`
    	UpdatedAt   string          `json:"updated_at"`
    }

    // OperationStore tracks long-running active operations (like firmware upgrades).
    // Contract: Implementations must preserve active operation records across daemon process termination
    // and host reboot. An in-memory-only implementation does not satisfy this interface contract.
    //
    // Terminal Lifecycle:
    // When a terminal downstream status is received, the daemon first saves the operation
    // with Active=false and its terminal Stage/Status. After the final Cloud response or progress
    // notification has been successfully queued and the TransactionCache entry has been stored,
    // the daemon deletes the OperationStore record. GetActive() must return only records where Active=true. On startup, the daemon must recover both Active=true records and Active=false records (via GetPendingTerminalDelivery) to resume Cloud delivery before deleting them.
    type OperationStore interface {
    	Save(ctx context.Context, operation *PersistentOperation) error
    	Get(ctx context.Context, operationID string) (*PersistentOperation, error)
    	GetActive(ctx context.Context) (*PersistentOperation, error)
    	GetPendingTerminalDelivery(ctx context.Context) ([]*PersistentOperation, error)
    	Delete(ctx context.Context, operationID string) error
    }
    ```

---

### Epic 4: Network & Transport Clients

#### PR 4.1: WebSocket Client & JSON-RPC Handler
*   **Target File:** `pkg/websocket/client.go`, `pkg/websocket/handler.go`
*   **Core WebSocket Signatures:**
    ```go
    package websocket

    import (
    	"context"
    	"github.com/gorilla/websocket"
    	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/queues"
    )

    type WSClient struct {
    	conn      *websocket.Conn
    	scheduler queues.OutboundScheduler
    	url       string
    }

    func NewWSClient(url string, scheduler queues.OutboundScheduler) *WSClient
    func (c *WSClient) StartReaderLoop(ctx context.Context, handler func([]byte))
    func (c *WSClient) StartWriterLoop(ctx context.Context)
    ```

#### PR 4.2: NATS Integration Client
*   **Target File:** `pkg/nats/client.go`
*   **Core NATS Signatures:**
    ```go
    package nats

    import (
    	"context"
    	"github.com/nats-io/nats.go"
    )

    type NATSClient struct {
    	conn *nats.Conn
    	js   nats.JetStreamContext
    	kv   nats.KeyValue
    }

    // NATSConfig defines the mandatory secure connection parameters for the NATS bus.
    type NATSConfig struct {
        Servers         []string // Must strictly use tls:// scheme. nats:// is rejected.
        CredentialsFile string   // Path to NATS credentials (NKEY/JWT).
        CAFile          string   // Mandatory path to the trusted Root CA. Cannot be empty.
    }

    // NewNATSClient initializes a NATS connection.
    // SECURITY CONTRACT: This constructor MUST enforce tls.Config{MinVersion: tls.VersionTLS13}.
    // It must return a fatal error if CAFile is empty, or if any Server URL is insecure.
    func NewNATSClient(cfg NATSConfig) (*NATSClient, error)

    // Asynchronous State-Changing Commands (uses NATS reply-to inbox and CommandResultQueue)
    func (n *NATSClient) PublishConfigTrigger(ctx context.Context, cmd *ConfigureCommand, replyTo string) error
    func (n *NATSClient) ExecuteAction(ctx context.Context, cmd *ActionCommand, replyTo string) error
    func (n *NATSClient) SubscribeCommandReplies(inbox string, handler func(msg *nats.Msg)) (*nats.Subscription, error)

    // Query Envelopes (Defined in pkg/contracts/envelopes.go)

    // Synchronous Read-Only Queries (blocks waiting for ResultEnvelope)
    // QueryCapabilities returns a ResultEnvelope whose Payload must be deserialized into a DeviceCapabilities struct.
    func (n *NATSClient) QueryCapabilities(ctx context.Context, query *contracts.CloudCapabilitiesQuery) (*contracts.ResultEnvelope, error)
    func (n *NATSClient) QueryDeviceStatus(ctx context.Context, query *contracts.CloudDeviceStatusQuery) (*contracts.DeviceStatus, error)

    // Streaming & Data Subscriptions
    func (n *NATSClient) SubscribeTelemetry(serial string, handler func(msg *nats.Msg)) (*nats.Subscription, error)
    func (n *NATSClient) SubscribeLogs(serial string, handler func(msg *nats.Msg)) (*nats.Subscription, error)
    func (n *NATSClient) SubscribeHealth(serial string, handler func(msg *nats.Msg)) (*nats.Subscription, error)
    func (n *NATSClient) SubscribeState(serial string, handler func(msg *nats.Msg)) (*nats.Subscription, error)
    func (n *NATSClient) WriteDesiredConfig(ctx context.Context, serial string, config []byte) (uint64, error)
    func (n *NATSClient) GetDesiredConfigMetadata(ctx context.Context, serial string) (uint64, string, error)
    

    ```

The uCentral client must not register a NATS responder for `ucentral.v1.device.<own-serial>.status.get`. This subject is queried by the uCentral client and served by the downstream device/local agent.

#### PR 4.3: Dynamic Capabilities & Local Signal Sockets
*   **Target File:** `pkg/nats/capabilities.go`
*   **Unix Socket Refresh Handler:**
    ```go
    package nats

    type CapabilityCache struct {
    	capabilities []byte
    	firmware     string
    }

    func StartUnixSignalListener(socketPath string, refreshCallback func()) error
    ```

---

### Epic 5: Main Entry Point & Assembly

#### PR 5.1: Main Loop & Configuration
*   **Target File:** `cmd/ucentral-client/main.go`
*   **Configuration Contract:**
    ```go
    type Config struct {
        Serial                    string      `json:"serial"`
        CompressionThresholdBytes int         `json:"compression_threshold_bytes"`
        Cloud                     CloudConfig `json:"cloud"`
        NATS                      NATSConfig  `json:"nats"`
        Queues                    QueueConfig `json:"queues"`
    }

    type CloudConfig struct {
        URL                   string `json:"url"`
        ConnectTimeoutSeconds int    `json:"connect_timeout_seconds"`
    }

    type NATSConfig struct {
        Servers         []string `json:"servers"`
        CredentialsFile string   `json:"credentials_file"`
        CAFile          string   `json:"ca_file"`
    }

    type QueueConfig struct {
        WSWriterCapacity      int `json:"ws_writer_capacity"`
        EmergencyCapacity     int `json:"emergency_capacity"`
        NATSPublishCapacity   int `json:"nats_publish_capacity"`
        CommandResultCapacity int `json:"command_result_capacity"`
        TelemetryCapacity     int `json:"telemetry_capacity"`
    }

    type CacheTTLConfig struct {
        Configure    int
        LEDs         int
        Reboot       int
        RemoteAccess int
        Factory      int
        CertUpdate   int
        Reenroll     int
        Script       int
        Upgrade      int
        Default      int
    }

    // LoadCacheTTLConfigFromEnv parses the OLG_CACHE_TTL_* environment variables as Go durations,
    // applies the documented defaults if unset, and rejects malformed or negative durations.
    func LoadCacheTTLConfigFromEnv() (CacheTTLConfig, error)
    
    // TTLForMethod returns the configured TTL in seconds for a specific JSON-RPC method.
    func (c CacheTTLConfig) TTLForMethod(method string) int
    ```
    *   **Validation Rules & Defaults:**
        *   `serial`: Required, non-empty
        *   `cloud.url`: Required, valid `wss://` URL
        *   `cloud.connect_timeout_seconds`: Default 10; must be > 0
        *   `nats.servers`: At least one entry; each must use `tls://`
        *   `nats.credentials_file`: Required and readable file path
        *   `nats.ca_file`: Required and readable file path
        *   `compression_threshold_bytes`: Default 2048; must be > 0
        *   `queues.ws_writer_capacity`: Default 500; must be > 0
        *   `queues.emergency_capacity`: Default 100; must be > 0
        *   `queues.nats_publish_capacity`: Default 100; must be > 0
        *   `queues.command_result_capacity`: Default 50; must be > 0
        *   `queues.telemetry_capacity`: Default 500; must be > 0
    *   **Timeout Variables:** The following environment variables must be strictly parsed via `time.ParseDuration`. Missing values safely fall back to defaults, but malformed strings or non-positive durations (e.g. `<= 0`) are fatal validation errors:
        *   `OLG_TIMEOUT_DISPATCH`: Default `5s`
        *   `OLG_TIMEOUT_CONFIGURE`: Default `30s`
        *   `OLG_TIMEOUT_ACTION_DEFAULT`: Default `60s`
        *   `OLG_TIMEOUT_ACTION_EXTENDED`: Default `120s`
    *   **Startup Behavior:** Configuration parsing or validation failure is fatal. The daemon must log the specific invalid field, avoid starting any Cloud or NATS connection loops, and exit immediately with a non-zero status.

*   **Initialization & Signal Handling:**
    *   Loads and strictly validates JSON configuration.
    *   Instantiates Queues, Request Manager, WebSocket client, NATS wrapper.
        * `NewPriorityScheduler(queues.ws_writer_capacity, queues.emergency_capacity)`
        * `NewTelemetryRingBuffer(queues.telemetry_capacity)`
    *   Launches parallel reconnection threads.
    *   Listens for `SIGINT` / `SIGTERM` to perform graceful resource teardowns.

#### PR 5.2: Integration & Simulation Tests
*   **Target File:** `tests/integration_test.go`
*   **NATS Local Broker Setup:**
    *   Verifies end-to-end NATS JetStream KV write, configure triggers, and rollback notifications.
