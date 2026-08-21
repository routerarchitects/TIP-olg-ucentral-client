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
// MarkInFlight twice) must be rejected by the API returning an error.
// (Note: logging of these assertion failures will be added in a future PR).
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

func validateTransition(from, to TransactionState) error {
	switch from {
	case TxCreated:
		if to == TxPreparingDispatch || to == TxFailed {
			return nil
		}
	case TxPreparingDispatch:
		if to == TxPendingPublish || to == TxFailed || to == TxCompleted {
			return nil
		}
	case TxPendingPublish:
		if to == TxInFlight || to == TxFailed || to == TxCompleted {
			return nil
		}
	case TxInFlight:
		if to == TxCompleted || to == TxFailed || to == TxTimedOut {
			return nil
		}
	case TxCompleted, TxFailed, TxTimedOut:
		return ErrAlreadyTerminal
	}
	return ErrInvalidStateTransition
}

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
	RPCID             string
	OperationID       string
	CloudSessionID    string
	CloudRPCID        json.RawMessage
	RequestKey        string // sessionID:typedCanonicalID, e.g. "session-uuid:n:42"
	RespondToCloud    bool
	Method            string
	State             TransactionState
	Payload           []byte
	HandoffInProgress bool
	CreatedAt         time.Time
	TimeoutDuration   time.Duration
	DispatchDeadline  time.Time
	ResponseDeadline  time.Time
	DispatchTimer     *time.Timer
	ResponseTimer     *time.Timer
	Cancel            context.CancelFunc
}

// Clone returns a shallow copy of the transaction with deep copies of slices/raw messages.
func (tx *Transaction) Clone() *Transaction {
	if tx == nil {
		return nil
	}
	cloned := *tx
	if tx.CloudRPCID != nil {
		cloned.CloudRPCID = make(json.RawMessage, len(tx.CloudRPCID))
		copy(cloned.CloudRPCID, tx.CloudRPCID)
	}
	if tx.Payload != nil {
		cloned.Payload = make([]byte, len(tx.Payload))
		copy(cloned.Payload, tx.Payload)
	}
	return &cloned
}

// DispatchItem represents a payload waiting in the internal dispatch buffer.
// The consumer MUST verify that time.Now() is before the transaction's DispatchDeadline,
// the transaction still exists, and the transaction is still in TxPendingPublish state
// before calling NATS Publish.
type DispatchItem struct {
	RPCID   string
	Payload []byte
}
