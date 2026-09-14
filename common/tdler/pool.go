package tdler

// DC selection follows iyear/tdl core/dcpool (AGPL-3.0), revision
// 70c561c0a6d44d8d7fffdea14988fc4104bc56d2. This implementation owns its
// registry and lifecycle; it does not use tdl's user-only takeout API.
import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/charmbracelet/log"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/tg"

	"github.com/krau/SaveAny-Bot/config"
	"github.com/krau/SaveAny-Bot/pkg/tfile"
)

var poolRegistry sync.Map // *tg.Client -> *poolManager; pointer identity isolates sessions.

type poolEntry struct {
	ready   chan struct{}
	invoker telegram.CloseInvoker
	client  downloader.Client
	err     error
}

type poolManager struct {
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	closed  bool
	entries map[int]*poolEntry
	create  func(context.Context, int) (telegram.CloseInvoker, error)
}

func newPoolManager(ctx context.Context, create func(context.Context, int) (telegram.CloseInvoker, error)) *poolManager {
	ctx, cancel := context.WithCancel(ctx)
	return &poolManager{ctx: ctx, cancel: cancel, entries: make(map[int]*poolEntry), create: create}
}

// RegisterClient registers the exact raw client used by TGFile constructors.
// The owner context must be the Telegram client lifetime, not a task context.
func RegisterClient(ctx context.Context, raw *tg.Client, base *telegram.Client, middlewares ...telegram.Middleware) {
	size := max(1, config.C().Telegram.DownloadPoolSize)
	if raw == nil || base == nil || size == 1 {
		return
	}
	manager := newPoolManager(ctx, func(ctx context.Context, dc int) (telegram.CloseInvoker, error) {
		var invoker telegram.CloseInvoker
		var err error
		if dc == base.Config().ThisDC {
			invoker, err = base.Pool(int64(size))
		} else {
			invoker, err = base.DC(ctx, dc, int64(size))
		}
		if err != nil {
			return nil, err
		}
		var wrapped tg.Invoker = invoker
		for i := len(middlewares) - 1; i >= 0; i-- {
			wrapped = middlewares[i].Handle(wrapped)
		}
		return &pooledInvoker{Invoker: wrapped, close: invoker.Close}, nil
	})
	if _, loaded := poolRegistry.LoadOrStore(raw, manager); loaded {
		manager.cancel()
		return
	}
	context.AfterFunc(ctx, func() {
		poolRegistry.CompareAndDelete(raw, manager)
		if err := manager.Close(); err != nil {
			log.FromContext(ctx).Warn("Close Telegram download pools", "error", err)
		}
	})
}

func (p *poolManager) Client(ctx context.Context, dc int) (downloader.Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	if p.closed || p.ctx.Err() != nil {
		p.mu.Unlock()
		return nil, errors.New("download pool closed")
	}
	if entry, ok := p.entries[dc]; ok {
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.ctx.Done():
			return nil, p.ctx.Err()
		case <-entry.ready:
			return entry.client, entry.err
		}
	}
	entry := &poolEntry{ready: make(chan struct{})}
	p.entries[dc] = entry
	p.mu.Unlock()

	// gotd pools inherit the base client's lifetime. Only initialization is
	// bounded by this task and deadline; cancelling it cannot close a reused pool.
	initCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	stop := context.AfterFunc(p.ctx, cancel)
	invoker, err := p.create(initCtx, dc)
	stop()
	cancel()
	p.mu.Lock()
	if err == nil && (p.closed || p.ctx.Err() != nil) {
		err = errors.New("download pool closed during initialization")
	}
	if err != nil {
		if invoker != nil {
			if closeErr := invoker.Close(); closeErr != nil {
				err = errors.Join(err, fmt.Errorf("close failed pool: %w", closeErr))
			}
		}
		entry.err = fmt.Errorf("create DC %d download pool: %w", dc, err)
		delete(p.entries, dc) // A later task can retry a transient initialization failure.
	} else {
		entry.invoker = invoker
		entry.client = tg.NewClient(invoker)
	}
	close(entry.ready)
	p.mu.Unlock()
	return entry.client, entry.err
}

func (p *poolManager) Close() error {
	p.cancel()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	var invokers []telegram.CloseInvoker
	for _, entry := range p.entries {
		if entry.invoker != nil {
			invokers = append(invokers, entry.invoker)
		}
	}
	p.mu.Unlock()
	var result error
	for _, invoker := range invokers {
		if err := invoker.Close(); err != nil {
			result = errors.Join(result, fmt.Errorf("close download pool: %w", err))
		}
	}
	return result
}

func downloadClient(ctx context.Context, file tfile.TGFile) (downloader.Client, string) {
	original := file.Dler()
	if config.C().Telegram.DownloadPoolSize <= 1 {
		return original, "pool_disabled"
	}
	if file.DC() <= 0 {
		return original, "no_dc"
	}
	raw, ok := original.(*tg.Client)
	if !ok {
		return original, "session_not_registered"
	}
	value, ok := poolRegistry.Load(raw)
	if !ok {
		return original, "session_not_registered"
	}
	client, err := value.(*poolManager).Client(ctx, file.DC())
	if err != nil {
		log.FromContext(ctx).Warn("Telegram download pool unavailable; using original client", "dc", file.DC(), "error", err)
		return original, "pool_create_failed"
	}
	return client, ""
}

type pooledInvoker struct {
	tg.Invoker
	close func() error
}

func (p *pooledInvoker) Close() error { return p.close() }
