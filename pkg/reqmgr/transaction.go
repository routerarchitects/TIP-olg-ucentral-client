package reqmgr

import (
	"context"
	"encoding/json"
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

func (s TransactionState) String() string {
	switch s {
	case TxCreated:
		return "TxCreated"
	case TxPreparingDispatch:
		return "TxPreparingDispatch"
	case TxPendingPublish:
		return "TxPendingPublish"
	case TxInFlight:
		return "TxInFlight"
	case TxCompleted:
		return "TxCompleted"
	case TxFailed:
		return "TxFailed"
	case TxTimedOut:
		return "TxTimedOut"
	default:
		return "Unknown"
	}
}

type Transaction struct {
	RPCID            string
	CloudSessionID   string
	CloudRPCID       json.RawMessage
	RequestKey       string // sessionID:method:canonicalID (e.g. "session-uuid:configure:number:42")
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
	RPCID   string
	Payload []byte
}
