package reqmgr

import (
	"context"
	"encoding/json"
)

type PersistentOperation struct {
	OperationID string          `json:"operation_id"`
	RPCID       string          `json:"rpc_id"`
	CloudRPCID  json.RawMessage `json:"cloud_rpc_id"`
	Target      string          `json:"target"`
	Action      string          `json:"action"`
	Stage       string          `json:"stage"`
	Status      string          `json:"status"`
	Active      bool            `json:"active"`
	CreatedAt   string          `json:"created_at"`
	UpdatedAt   string          `json:"updated_at"`
}

type OperationStore interface {
	Save(ctx context.Context, operation *PersistentOperation) error
	Get(ctx context.Context, operationID string) (*PersistentOperation, error)
	// GetActive returns up to 'limit' active operations.
	// Contract: Implementations MUST return records sorted by UpdatedAt in descending order (newest first).
	GetActive(ctx context.Context, limit int) ([]*PersistentOperation, error)
	Delete(ctx context.Context, operationID string) error
}
