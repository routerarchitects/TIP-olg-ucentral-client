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
	GetActive(ctx context.Context) (*PersistentOperation, error)
	GetPendingTerminalDelivery(ctx context.Context) ([]*PersistentOperation, error)
	Delete(ctx context.Context, operationID string) error
}
