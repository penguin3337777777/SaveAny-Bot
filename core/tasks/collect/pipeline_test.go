package collect

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPipelineOverlapsStagesWithoutAccumulatingFiles(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	p := NewPipeline()
	a, err := p.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Release()
	gotB := make(chan *Lease, 1)
	go func() { b, _ := p.Acquire(ctx); gotB <- b }()
	select {
	case <-gotB:
		t.Fatal("two downloads admitted")
	case <-time.After(20 * time.Millisecond):
	}
	if err := a.BeginUpload(ctx); err != nil {
		t.Fatal(err)
	}
	var b *Lease
	select {
	case b = <-gotB:
	case <-time.After(time.Second):
		t.Fatal("B cannot download during A upload")
	}
	defer b.Release()
	bUpload := make(chan error, 1)
	go func() { bUpload <- b.BeginUpload(ctx) }()
	gotC := make(chan *Lease, 1)
	go func() { c, _ := p.Acquire(ctx); gotC <- c }()
	select {
	case <-bUpload:
		t.Fatal("two uploads admitted")
	case <-time.After(20 * time.Millisecond):
	}
	select {
	case <-gotC:
		t.Fatal("third resident file admitted")
	case <-time.After(20 * time.Millisecond):
	}
	a.Release()
	select {
	case err := <-bUpload:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("B upload blocked")
	}
	select {
	case c := <-gotC:
		if c == nil {
			t.Fatal("C absent")
		}
		c.Release()
	case <-time.After(time.Second):
		t.Fatal("C download blocked")
	}
	b.Release()
	if len(p.files) != 0 || len(p.upload) != 0 || len(p.download) != 0 {
		t.Fatal("slots leaked")
	}
}
func TestPipelineCancellationReleasesWaitingFile(t *testing.T) {
	p := NewPipeline()
	ctx, cancel := context.WithCancel(t.Context())
	a, _ := p.Acquire(ctx)
	defer a.Release()
	if err := a.BeginUpload(ctx); err != nil {
		t.Fatal(err)
	}
	b, _ := p.Acquire(ctx)
	done := make(chan error, 1)
	go func() { err := b.BeginUpload(ctx); b.Release(); done <- err }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel blocked")
	}
	a.Release()
	if len(p.files) != 0 || len(p.upload) != 0 || len(p.download) != 0 {
		t.Fatal("slots leaked on cancellation")
	}
}
