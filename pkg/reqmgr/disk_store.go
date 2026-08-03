package reqmgr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// DiskOperationStore provides a disk-backed implementation of OperationStore.
// It persists active operations as JSON files in a designated directory.
type DiskOperationStore struct {
	basePath string
}

// NewDiskOperationStore creates a new DiskOperationStore, ensuring the underlying
// directory exists.
func NewDiskOperationStore(basePath string) (*DiskOperationStore, error) {
	if err := os.MkdirAll(basePath, 0750); err != nil {
		return nil, fmt.Errorf("failed to create operation store directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(basePath, "quarantine"), 0750); err != nil {
		return nil, fmt.Errorf("failed to create quarantine directory: %w", err)
	}
	return &DiskOperationStore{basePath: basePath}, nil
}

func (s *DiskOperationStore) getPath(operationID string) (string, error) {
	if _, err := uuid.Parse(operationID); err != nil {
		return "", fmt.Errorf("invalid operation ID: %w", err)
	}
	return filepath.Join(s.basePath, operationID+".json"), nil
}

func (s *DiskOperationStore) Save(ctx context.Context, operation *PersistentOperation) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if operation == nil {
		return errors.New("operation cannot be nil")
	}

	data, err := json.Marshal(operation)
	if err != nil {
		return err
	}

	targetPath, err := s.getPath(operation.OperationID)
	if err != nil {
		return err
	}
	tempPath := fmt.Sprintf("%s.tmp.%s", targetPath, uuid.New().String())

	f, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0640)
	if err != nil {
		return err
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tempPath)
		return err
	}

	// Force hardware durability before renaming
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tempPath)
		return err
	}

	if err := f.Close(); err != nil {
		os.Remove(tempPath)
		return err
	}

	// Atomic rename for concurrent visibility
	if err := os.Rename(tempPath, targetPath); err != nil {
		os.Remove(tempPath)
		return err
	}

	// Sync the parent directory to guarantee the rename survives power loss
	parentDir, err := os.Open(s.basePath)
	if err != nil {
		return fmt.Errorf("failed to open directory for sync: %w", err)
	}
	if err := parentDir.Sync(); err != nil {
		parentDir.Close()
		return fmt.Errorf("failed to sync directory: %w", err)
	}
	parentDir.Close()

	return nil
}

func (s *DiskOperationStore) Get(ctx context.Context, operationID string) (*PersistentOperation, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	targetPath, err := s.getPath(operationID)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var op PersistentOperation
	if err := json.Unmarshal(data, &op); err != nil {
		return nil, err
	}

	return &op, nil
}

func (s *DiskOperationStore) GetActive(ctx context.Context, limit int) ([]*PersistentOperation, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	entries, err := os.ReadDir(s.basePath)
	if err != nil {
		return nil, err
	}

	var allOps []*PersistentOperation

	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		path := filepath.Join(s.basePath, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			log.Printf("reqmgr: failed to read active operation file %s: %v", path, err)
			continue
		}

		var op PersistentOperation
		if err := json.Unmarshal(data, &op); err != nil {
			log.Printf("reqmgr: failed to parse active operation file %s, treating as corrupt and quarantining: %v", path, err)
			if qErr := s.quarantinePathDurably(path); qErr != nil {
				log.Printf("reqmgr: failed to durably quarantine corrupt file %s: %v", path, qErr)
			}
			continue
		}

		if _, err := uuid.Parse(op.OperationID); err != nil {
			log.Printf("reqmgr: invalid operation ID %q in file %s, treating as corrupt and quarantining: %v", op.OperationID, path, err)
			if qErr := s.quarantinePathDurably(path); qErr != nil {
				log.Printf("reqmgr: failed to durably quarantine corrupt file %s: %v", path, qErr)
			}
			continue
		}

		expectedName := op.OperationID + ".json"
		if entry.Name() != expectedName {
			log.Printf("reqmgr: operation ID mismatch (expected %s, got %s), treating as corrupt and quarantining", expectedName, entry.Name())
			if qErr := s.quarantinePathDurably(path); qErr != nil {
				log.Printf("reqmgr: failed to durably quarantine corrupt file %s: %v", path, qErr)
			}
			continue
		}

		if _, err := time.Parse(time.RFC3339, op.UpdatedAt); err != nil {
			log.Printf("reqmgr: invalid UpdatedAt %q in file %s, treating as corrupt and quarantining: %v", op.UpdatedAt, path, err)
			if qErr := s.quarantinePathDurably(path); qErr != nil {
				log.Printf("reqmgr: failed to durably quarantine corrupt file %s: %v", path, qErr)
			}
			continue
		}

		if !op.Active {
			continue
		}

		allOps = append(allOps, &op)
	}

	// Sort descending (newest first) by UpdatedAt
	sort.Slice(allOps, func(i, j int) bool {
		ti, _ := time.Parse(time.RFC3339, allOps[i].UpdatedAt)
		tj, _ := time.Parse(time.RFC3339, allOps[j].UpdatedAt)
		return ti.After(tj)
	})

	if limit > 0 && len(allOps) > limit {
		allOps = allOps[:limit]
	}

	return allOps, nil
}

func (s *DiskOperationStore) Delete(ctx context.Context, operationID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	targetPath, err := s.getPath(operationID)
	if err != nil {
		return err
	}
	return s.deletePathDurably(targetPath)
}

func (s *DiskOperationStore) deletePathDurably(path string) error {
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	parentDir, err := os.Open(s.basePath)
	if err != nil {
		return fmt.Errorf("failed to open directory for sync: %w", err)
	}
	if err := parentDir.Sync(); err != nil {
		parentDir.Close()
		return fmt.Errorf("failed to sync directory: %w", err)
	}
	parentDir.Close()

	return nil
}

func (s *DiskOperationStore) quarantinePathDurably(path string) error {
	filename := filepath.Base(path)
	quarantineDir := filepath.Join(s.basePath, "quarantine")
	target := filepath.Join(
		quarantineDir,
		fmt.Sprintf("%s.%d.corrupt", filename, time.Now().UnixNano()),
	)

	if err := os.Rename(path, target); err != nil {
		return err
	}

	// Sync quarantine dir
	qDir, err := os.Open(quarantineDir)
	if err != nil {
		return fmt.Errorf("failed to open quarantine dir for sync: %w", err)
	}
	if err := qDir.Sync(); err != nil {
		qDir.Close()
		return fmt.Errorf("failed to sync quarantine dir: %w", err)
	}
	qDir.Close()

	// Sync parent dir
	pDir, err := os.Open(s.basePath)
	if err != nil {
		return fmt.Errorf("failed to open operations dir for sync: %w", err)
	}
	if err := pDir.Sync(); err != nil {
		pDir.Close()
		return fmt.Errorf("failed to sync operations dir: %w", err)
	}
	pDir.Close()

	return nil
}
