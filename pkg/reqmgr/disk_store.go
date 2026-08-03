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

	type fileInfo struct {
		path    string
		modTime int64
	}

	var files []fileInfo
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		files = append(files, fileInfo{
			path:    filepath.Join(s.basePath, entry.Name()),
			modTime: info.ModTime().UnixNano(),
		})
	}

	// Contract: Sort descending (newest first)
	sort.Slice(files, func(i, j int) bool {
		return files[i].modTime > files[j].modTime
	})

	var ops []*PersistentOperation
	for _, f := range files {
		// Respect context cancellation to abort loop if taking too long
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if limit > 0 && len(ops) >= limit {
			break
		}

		data, err := os.ReadFile(f.path)
		if err != nil {
			log.Printf("reqmgr: failed to read active operation file %s: %v", f.path, err)
			continue // Skip unreadable files
		}

		var op PersistentOperation
		if err := json.Unmarshal(data, &op); err != nil {
			log.Printf("reqmgr: failed to parse active operation file %s, treating as corrupt and deleting: %v", f.path, err)
			if delErr := s.deletePathDurably(f.path); delErr != nil {
				log.Printf("reqmgr: failed to durably delete corrupt file %s: %v", f.path, delErr)
			}
			continue // Skip corrupted files
		}

		ops = append(ops, &op)
	}

	return ops, nil
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
