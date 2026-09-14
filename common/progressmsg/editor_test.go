package progressmsg

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/tg"
)

func request(text string) *tg.MessagesEditMessageRequest {
	return &tg.MessagesEditMessageRequest{ID: 1, Message: text}
}
func receive(t *testing.T, edits <-chan string, want string) {
	t.Helper()
	select {
	case got := <-edits:
		if got != want {
			t.Fatalf("edit=%q want=%q", got, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("missing edit %q", want)
	}
}
func quiet(t *testing.T, edits <-chan string) {
	t.Helper()
	select {
	case got := <-edits:
		t.Fatalf("unexpected edit %q", got)
	case <-time.After(15 * time.Millisecond):
	}
}

func TestIntervalCoalescesAndFinalBypasses(t *testing.T) {
	edits := make(chan string, 100)
	var clock atomic.Int64
	clock.Store(time.Now().UnixNano())
	editor := NewWithSender(t.Context(), 15*time.Second, func(_ context.Context, r *tg.MessagesEditMessageRequest) error { edits <- r.Message; return nil })
	editor.now = func() time.Time { return time.Unix(0, clock.Load()) }
	editor.Submit(request("start"), true, false)
	receive(t, edits, "start")
	// Wait until the send has been committed before advancing the fake clock.
	editor.mu.Lock()
	editor.mu.Unlock()
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Go(func() { editor.Submit(request(fmt.Sprint(i)), false, false) })
	}
	wg.Wait()
	editor.Submit(request("latest"), false, false)
	quiet(t, edits)
	clock.Add(int64(15 * time.Second))
	editor.Submit(request("latest"), false, false)
	receive(t, edits, "latest")
	editor.Submit(request("pending"), false, false)
	quiet(t, edits)
	editor.Submit(request("final"), true, true)
	receive(t, edits, "final")
	editor.Submit(request("late callback"), true, false)
	quiet(t, edits)
}

func TestSlowFailedEditDoesNotBlockCallbacksOrReplayFrames(t *testing.T) {
	edits := make(chan string, 100)
	entered := make(chan struct{})
	release := make(chan struct{})
	editor := NewWithSender(t.Context(), 15*time.Second, func(ctx context.Context, r *tg.MessagesEditMessageRequest) error {
		if r.Message == "start" {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
			}
			return errors.New("simulated FLOOD_WAIT")
		}
		edits <- r.Message
		return nil
	})
	editor.Submit(request("start"), true, false)
	<-entered
	finished := make(chan struct{})
	go func() {
		for i := range 1000 {
			editor.Submit(request(fmt.Sprint(i)), false, false)
		}
		editor.Submit(request("cancelled"), true, true)
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("progress callback blocked behind edit")
	}
	close(release)
	receive(t, edits, "cancelled")
	quiet(t, edits)
}

func TestIdenticalContentNotEditedAgain(t *testing.T) {
	edits := make(chan string, 10)
	editor := NewWithSender(t.Context(), 0, func(_ context.Context, r *tg.MessagesEditMessageRequest) error { edits <- r.Message; return nil })
	editor.Submit(request("same"), true, false)
	receive(t, edits, "same")
	editor.Submit(request("same"), false, false)
	quiet(t, edits)
	editor.Submit(request("done"), true, true)
	receive(t, edits, "done")
}
