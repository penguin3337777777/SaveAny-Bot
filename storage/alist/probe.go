package alist

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// CheckExists fails closed on transport, permission and backend errors.
func (a *Alist) CheckExists(ctx context.Context, p string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	body, err := json.Marshal(map[string]any{"path": a.JoinStoragePath(p), "password": ""})
	if err != nil {
		return false, fmt.Errorf("encode existence request: %w", err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+"/api/fs/get", bytes.NewReader(body))
		if err != nil {
			return false, fmt.Errorf("create existence request: %w", err)
		}
		req.Header.Set("Authorization", a.authHeader())
		req.Header.Set("Content-Type", "application/json")
		resp, err := a.client.Do(req)
		if err != nil {
			return false, fmt.Errorf("check remote file: %w", err)
		}
		var result fsGetResponse
		decodeErr := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result)
		resp.Body.Close()
		if attempt == 0 && a.loginInfo != nil && (resp.StatusCode == 401 || resp.StatusCode == 403 || result.Code == 401 || result.Code == 403) {
			if err := a.getToken(ctx); err != nil {
				return false, fmt.Errorf("refresh storage login: %w", err)
			}
			continue
		}
		if resp.StatusCode != http.StatusOK {
			return false, fmt.Errorf("existence HTTP status %d", resp.StatusCode)
		}
		if decodeErr != nil {
			return false, fmt.Errorf("decode existence response: %w", decodeErr)
		}
		if result.Code == 200 {
			return true, nil
		}
		// Only the explicit OpenList object-not-found result is absence. In
		// particular an HTTP 404 may be a wrong API URL, not a missing file.
		msg := strings.ToLower(strings.TrimSpace(result.Message))
		for {
			previous := msg
			msg = strings.TrimPrefix(msg, "failed to get parent folder: ")
			msg = strings.TrimPrefix(msg, "failed to get obj: ")
			if msg == previous {
				break
			}
		}
		if result.Code == 500 && msg == "object not found" {
			return false, nil
		}
		return false, fmt.Errorf("existence API status %d: %s", result.Code, result.Message)
	}
	return false, fmt.Errorf("existence check exhausted")
}
