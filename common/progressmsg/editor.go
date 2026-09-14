// Package progressmsg sends best-effort Telegram progress without blocking
// transfer callbacks. Each editor owns one message and one replaceable snapshot.
package progressmsg

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/charmbracelet/log"
	"github.com/gotd/td/tg"

	"github.com/krau/SaveAny-Bot/common/utils/tgutil"
)

type update struct {
	request *tg.MessagesEditMessageRequest
	force   bool
}

type Editor struct {
	now         func() time.Time
	mu          sync.Mutex
	interval    time.Duration
	send        func(context.Context, *tg.MessagesEditMessageRequest) error
	ctx         context.Context
	pending     *update
	running     bool
	final       bool
	lastAt      time.Time
	lastRequest *tg.MessagesEditMessageRequest
	wake        chan struct{}
}

// New uses the Telegram client context so task cancellation can still report a
// final Cancelled state. Each RPC also has a bounded deadline.
func New(ctx context.Context, chatID int64, interval time.Duration) *Editor {
	ext := tgutil.ExtFromContext(ctx)
	if ext == nil {
		return nil
	}
	return NewWithSender(ext.Context, interval, func(sendCtx context.Context, req *tg.MessagesEditMessageRequest) error {
		copy := *ext
		copy.Context = sendCtx
		_, err := copy.EditMessage(chatID, req)
		if err != nil {
			log.FromContext(ctx).Warn("Failed to edit Telegram progress message", "error", err)
		}
		return err
	})
}

// NewWithSender permits deterministic, network-free testing of message edits.
func NewWithSender(ctx context.Context, interval time.Duration, send func(context.Context, *tg.MessagesEditMessageRequest) error) *Editor {
	return &Editor{now: time.Now, ctx: ctx, interval: interval, send: send, wake: make(chan struct{}, 1)}
}

// Submit never waits for a network call. A final snapshot replaces any pending
// progress and permanently rejects later callbacks. Callers transfer ownership
// of the request; the worker copies it before Telegram fills in its peer.
func (e *Editor) Submit(req *tg.MessagesEditMessageRequest, force, final bool) {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.final || e.ctx.Err() != nil {
		return
	}
	if e.pending != nil {
		force = force || e.pending.force
	}
	e.pending = &update{request: req, force: force || final}
	e.final = final
	select {
	case e.wake <- struct{}{}:
	default:
	}
	if !e.running {
		e.running = true
		go e.run()
	}
}

func (e *Editor) run() {
	for {
		e.mu.Lock()
		next := e.pending
		if next == nil || e.ctx.Err() != nil {
			e.pending = nil
			e.running = false
			e.mu.Unlock()
			return
		}
		delay := e.lastAt.Add(e.interval).Sub(e.now())
		if !next.force && !e.lastAt.IsZero() && delay > 0 {
			e.mu.Unlock()
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-e.wake:
			case <-e.ctx.Done():
			}
			timer.Stop()
			continue
		}
		e.pending = nil
		duplicate := reflect.DeepEqual(next.request, e.lastRequest)
		e.mu.Unlock()
		if !duplicate {
			ctx, cancel := context.WithTimeout(e.ctx, 5*time.Minute)
			request := *next.request
			err := e.send(ctx, &request)
			cancel()
			e.mu.Lock()
			// Measure from completion, including middleware FLOOD_WAIT retries. This
			// avoids a catch-up burst when an edit took longer than the interval.
			e.lastAt = e.now()
			if err == nil {
				e.lastRequest = next.request
			}
			e.mu.Unlock()
		}
	}
}
