# Technical Specification: uCentral Client Daemon

This document details the code layout, interface signatures, data structures, and protocol contracts for the Go-based uCentral Client daemon (`olg-ucentral-client`).

---

## 1. Project Directory Structure

The project follows standard Go layout guidelines:

```text
olg-ucentral-client/
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
    │   └── cache.go                # TTL-based transaction cache
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
    	ErrAppFailure        = 1
    	ErrTimeout           = 2
    	ErrServiceUnavailable = 3
    	ErrValidationFailed  = 4
    	ErrRollbackSuccess   = 5
    	ErrRollbackFailed    = 6
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
    ```

*   **NATS Envelope Structures (`pkg/contracts/envelopes.go`):**
    ```go
    package contracts

    import "encoding/json"

    type ConfigureCommand struct {
        Version     string          `json:"version"`
        RPCID       json.RawMessage `json:"rpc_id"`
        Target      string          `json:"target"`
        UUID        string          `json:"uuid"`
        KVKey       string          `json:"kv_key"`
        KVRevision  uint64          `json:"kv_revision"`
        Timestamp   string          `json:"timestamp"`
    }

    type ActionCommand struct {
    	Version     string          `json:"version"`
    	RPCID       json.RawMessage `json:"rpc_id"`
    	Target      string          `json:"target"`
    	CommandType string          `json:"command_type"`
    	Action      string          `json:"action"`
    	Payload     json.RawMessage `json:"payload"`
    	Timestamp   string          `json:"timestamp"`
    }

    type ResultEnvelope struct {
    	Version     string          `json:"version"`
    	RPCID       json.RawMessage `json:"rpc_id"`
    	Target      string          `json:"target"`
    	CommandType string          `json:"command_type"`
    	UUID        string          `json:"uuid,omitempty"` // Omitted for Action
    	Result      ResultType      `json:"result"`
    	Message     string          `json:"message"`
    	Timestamp   string          `json:"timestamp"`
    }
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
    	StateOffline        ConnectionState = "offline"
    	StateConnectingBoth ConnectionState = "connecting_both"
    	StateOperational    ConnectionState = "operational"
    	StateDegraded       ConnectionState = "degraded"
    )
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
    // - If a queue exceeds its capacity, the push blocks.
    //   Exception 1: Priority 0 messages bypass lower-priority backlog, but do not use an unbounded queue. They are placed in a dedicated bounded emergency queue. If that queue reaches its hard limit, Push() returns an overflow error so the caller can treat the WebSocket writer path as unhealthy and trigger recovery if needed, fail affected transactions, and record an overflow metric.
    //   Exception 2: Priority 1 (Audits/Crash logs) must be non-blocking to prevent deadlocking core NATS handlers. If Priority 1 reaches capacity, Push() returns a fast error immediately.
    // - Next() blocks until a message is available or the context is canceled.
    //   Highest priority messages (0) are always selected first.
    // - Context cancellation drives scheduler shutdown and unblocks waiting Push/Next calls.
    type OutboundScheduler interface {
    	Push(ctx context.Context, msg OutboundMessage) error
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

    func (s *PriorityScheduler) Push(ctx context.Context, msg OutboundMessage) error
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

    // StateCoalescer implements last-write-wins in-memory state storage
    type StateCoalescer struct {
    	mu         sync.Mutex
    	latestState []byte
    }

    func NewStateCoalescer() *StateCoalescer
    func (c *StateCoalescer) Update(payload []byte)
    func (c *StateCoalescer) Flush() []byte

    // NATSDispatchBuffer buffers commands headed for NATS. Rejects when full.
    type NATSDispatchBuffer struct {
    	ch chan []byte
    }

    func NewNATSDispatchBuffer(capacity int) *NATSDispatchBuffer
    func (d *NATSDispatchBuffer) Push(ctx context.Context, payload []byte) error
    func (d *NATSDispatchBuffer) Pop(ctx context.Context) ([]byte, error)

    // ErrQueueFull is returned when a push fails due to the non-blocking capacity limit.
    var ErrQueueFull = errors.New("command result queue is at maximum capacity")

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
*   **Ownership:** The queue is populated (`Push`) by the asynchronous NATS Subscriber goroutines. It is consumed (`Pop`) exclusively by the Main WebSocket Writer goroutine (pulling it into the Priority 0 Outbound Scheduler).
*   **Overflow Policy (Reject):** The queue is strictly non-blocking. If a NATS subscriber attempts to push to a full queue, the queue immediately rejects it by returning `ErrQueueFull`. The NATS subscriber must log an overflow error and drop the result to protect the router from backpressure.
*   **Telemetry Throttling (Activation & Release):** The Main loop polls `Utilization()` before processing telemetry.
    *   **Activation:** If `Utilization() >= 0.90` (90% capacity, e.g., 45/50 items), the daemon engages telemetry throttling, pausing all reads from the `TelemetryRingBuffer`.
    *   **Release:** Throttling remains engaged until `Utilization() <= 0.50` (queue drops to 50% capacity), creating a hysteresis loop to prevent rapid toggling, at which point telemetry forwarding resumes.

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

    type TransactionState int

    const (
    	TxCreated TransactionState = iota
    	TxPendingNATS
    	TxInFlight
    	TxCompleted
    	TxFailed
    	TxTimedOut
    )

    type RPCID struct {
    	Raw json.RawMessage // exact ID bytes for response/NATS propagation
    	Key string          // canonical internal map key (e.g. "string:42" or "number:42")
    }

    type Transaction struct {
    	RPCID     RPCID
    	State     TransactionState
    	CreatedAt time.Time
    	ResultCh  chan []byte
    	Cancel    context.CancelFunc
    }

    type DefaultRequestManager struct {
    	mu           sync.Mutex
    	transactions map[string]*Transaction
    	stateLock    sync.Mutex // Enforces serialized state-changing commands
    	activeStateTx string    // Canonical key holding the state lock
    }

    func NewRequestManager() *DefaultRequestManager
    func (m *DefaultRequestManager) CreateTransaction(rpcID RPCID, timeout time.Duration, isStateChanging bool) (*Transaction, error)
    ```

#### PR 3.2: Duplicate Attachment & Cache TTL
*   **Target File:** `pkg/reqmgr/cache.go`, `pkg/reqmgr/manager.go` (extensions)
*   **Core Cache Structures:**
    ```go
    package reqmgr

    import "sync"

    type CacheEntry struct {
    	Payload   []byte
    	ExpiresAt int64
    }

    type TransactionCache struct {
    	mu    sync.RWMutex
    	items map[string]CacheEntry
    }

    func NewTransactionCache() *TransactionCache
    func (c *TransactionCache) Set(rpcID RPCID, payload []byte, ttlSeconds int)
    func (c *TransactionCache) Get(rpcID RPCID) ([]byte, bool)
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
    	"github.com/routerarchitects/olg-ucentral-client/pkg/queues"
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
    func (n *NATSClient) WriteDesiredConfig(ctx context.Context, serial string, config []byte) (uint64, error)
    func (n *NATSClient) PublishConfigTrigger(ctx context.Context, serial string, uuid string, kvKey string, revision uint64) error
    func (n *NATSClient) GetDesiredConfigMetadata(ctx context.Context, serial string) (uint64, string, error) // Returns (revision, uuid, error)
    ```

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
*   **Initialization & Signal Handling:**
    *   Loads JSON configuration.
    *   Instantiates Queues, Request Manager, WebSocket client, NATS wrapper.
    *   Launches parallel reconnection threads.
    *   Listens for `SIGINT` / `SIGTERM` to perform graceful resource teardowns.

#### PR 5.2: Integration & Simulation Tests
*   **Target File:** `tests/integration_test.go`
*   **NATS Local Broker Setup:**
    *   Verifies end-to-end NATS JetStream KV write, configure triggers, and rollback notifications.
