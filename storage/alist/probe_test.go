package alist

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestExistenceDoesNotTurnErrorsIntoMissingFiles(t *testing.T) {
	for _, tc := range []struct {
		name           string
		http, code     int
		message        string
		found, wantErr bool
	}{
		{"exists", 200, 200, "ok", true, false},
		{"missing", 200, 500, "failed to get obj: object not found", false, false},
		{"new directory", 200, 500, "failed to get parent folder: failed to get obj: object not found", false, false},
		{"permission", 200, 403, "forbidden", false, true},
		{"backend", 200, 500, "storage unavailable", false, true},
		{"missing API", 404, 404, "not found", false, true},
		{"upstream timeout", 502, 502, "bad gateway", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.http)
				json.NewEncoder(w).Encode(map[string]any{"code": tc.code, "message": tc.message})
			}))
			defer srv.Close()
			a := Alist{client: srv.Client(), baseURL: srv.URL}
			found, err := a.CheckExists(t.Context(), "file.mp4")
			if found != tc.found || (err != nil) != tc.wantErr {
				t.Fatalf("got %v %v", found, err)
			}
		})
	}
}

func TestExistenceHonorsCancellation(t *testing.T) {
	a := Alist{client: http.DefaultClient, baseURL: "http://127.0.0.1:1"}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := a.CheckExists(ctx, "video.mp4"); err == nil {
		t.Fatal("expected cancellation")
	}
}
