package local

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func (l *Local) CheckExists(ctx context.Context, p string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	p = filepath.Clean(p)
	if filepath.IsAbs(p) || p == ".." || strings.HasPrefix(p, ".."+string(filepath.Separator)) {
		return false, fmt.Errorf("invalid storage path")
	}
	_, err := os.Stat(l.JoinStoragePath(p))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("stat destination: %w", err)
	}
	return true, nil
}
