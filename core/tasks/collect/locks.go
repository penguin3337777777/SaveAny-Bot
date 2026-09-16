package collect

import (
	"context"
	"sync"
)

type entry struct {
	busy chan struct{}
	refs int
}
type PathLocks struct {
	mu      sync.Mutex
	entries map[string]*entry
}

// Acquire only retains active/waiting paths, never a completed-file history.
func (p *PathLocks) Acquire(ctx context.Context, key string) (func(), error) {
	p.mu.Lock()
	if p.entries == nil {
		p.entries = make(map[string]*entry)
	}
	e := p.entries[key]
	if e == nil {
		e = &entry{busy: make(chan struct{}, 1)}
		p.entries[key] = e
	}
	e.refs++
	p.mu.Unlock()
	drop := func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		e.refs--
		if e.refs == 0 {
			delete(p.entries, key)
		}
	}
	select {
	case e.busy <- struct{}{}:
		var once sync.Once
		return func() { once.Do(func() { <-e.busy; drop() }) }, nil
	case <-ctx.Done():
		drop()
		return nil, ctx.Err()
	}
}
