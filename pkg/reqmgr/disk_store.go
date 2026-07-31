package reqmgr

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

func (s *DiskOperationStore) getPath(operationID string) string {
	return filepath.Join(s.basePath, operationID+".json")
}

func (s *DiskOperationStore) Save(ctx context.Context, operation *PersistentOperation) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	data, err := json.MarshalIndent(operation, "", "  ")
	if err != nil {
		return err
	}

	targetPath := s.getPath(operation.OperationID)
	tempPath := targetPath + ".tmp"

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
	if parentDir, err := os.Open(s.basePath); err == nil {
		_ = parentDir.Sync()
		_ = parentDir.Close()
	}

	return nil
}

func (s *DiskOperationStore) Get(ctx context.Context, operationID string) (*PersistentOperation, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	data, err := os.ReadFile(s.getPath(operationID))
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
		if limit > 0 && len(ops) >= limit {
			break
		}

		data, err := os.ReadFile(f.path)
		if err != nil {
			continue // Skip unreadable files
		}

		var op PersistentOperation
		if err := json.Unmarshal(data, &op); err != nil {
			continue // Skip corrupted files
		}

		ops = append(ops, &op)
	}

	return ops, nil
}

func (s *DiskOperationStore) Delete(ctx context.Context, operationID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	err := os.Remove(s.getPath(operationID))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
