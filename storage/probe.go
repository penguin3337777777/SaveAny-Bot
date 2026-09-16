package storage

import (
	"context"
	"fmt"
)

// StorageExistenceChecker distinguishes a missing object from a failed check.
// Collection must never fall back to the lossy Exists bool interface.
type StorageExistenceChecker interface {
	CheckExists(context.Context, string) (bool, error)
}

func CheckExists(ctx context.Context, stor Storage, path string) (bool, error) {
	checker, ok := stor.(StorageExistenceChecker)
	if !ok {
		return false, fmt.Errorf("storage %s does not support reliable existence checks", stor.Name())
	}
	return checker.CheckExists(ctx, path)
}
