package handlers

import (
	"context"
	"errors"
	"github.com/celestix/gotgproto/ext"
	"github.com/gotd/td/tg"
	"github.com/krau/SaveAny-Bot/database"
	"io"
	"strings"
	"testing"

	"github.com/krau/SaveAny-Bot/storage"
)

type collectProbeStorage struct {
	storage.Storage
	exists bool
	err    error
	saves  int
}

func (s *collectProbeStorage) Name() string { return "probe" }

func (s *collectProbeStorage) CheckExists(context.Context, string) (bool, error) {
	return s.exists, s.err
}
func (s *collectProbeStorage) Save(context.Context, io.Reader, string) error { s.saves++; return nil }

func TestCollectUploadRechecksDestination(t *testing.T) {
	for _, exists := range []bool{false, true} {
		base := &collectProbeStorage{exists: exists}
		guard := collectStorage{Storage: base}
		if err := guard.Save(t.Context(), strings.NewReader("data"), "video.mp4"); err != nil {
			t.Fatal(err)
		}
		if guard.skipped != exists || base.saves != map[bool]int{true: 0, false: 1}[exists] {
			t.Fatal(guard.skipped, base.saves)
		}
	}
}

func TestCollectCheckFailureDoesNotUpload(t *testing.T) {
	base := &collectProbeStorage{err: errors.New("unauthorized")}
	guard := collectStorage{Storage: base}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := guard.Save(ctx, strings.NewReader("data"), "video.mp4"); err == nil {
		t.Fatal("expected error")
	}
	if base.saves != 0 {
		t.Fatal("uploaded despite failed check")
	}
}

func TestCollectExistingFileSkipsBeforeTelegramDownload(t *testing.T) {
	base := &collectProbeStorage{exists: true}
	msg := &tg.Message{ID: 42, Media: &tg.MessageMediaDocument{Document: &tg.Document{ID: 42, MimeType: "video/mp4", Attributes: []tg.DocumentAttributeClass{&tg.DocumentAttributeFilename{FileName: "existing.mp4"}}}}}
	// A nil Telegram API would panic if any refresh or download were attempted.
	skipped, err := collectFile(t.Context(), "test", &database.User{}, &ext.Context{}, base, "videos", msg, []*tg.Message{msg})
	if err != nil || !skipped || base.saves != 0 {
		t.Fatalf("skipped=%v saves=%d err=%v", skipped, base.saves, err)
	}
}
