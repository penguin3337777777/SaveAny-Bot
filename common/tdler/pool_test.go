package tdler

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/spf13/viper"

	"github.com/krau/SaveAny-Bot/config"
	"github.com/krau/SaveAny-Bot/pkg/tfile"
)

type testPoolInvoker struct {
	closed atomic.Int32
	data   []byte
}

func (p *testPoolInvoker) Close() error { p.closed.Add(1); return nil }
func (p *testPoolInvoker) Invoke(_ context.Context, input bin.Encoder, output bin.Decoder) error {
	switch req := input.(type) {
	case *tg.UploadGetFileRequest:
		result := &tg.UploadFile{Type: &tg.StorageFileUnknown{}, Bytes: p.data[min(int(req.Offset), len(p.data)):min(int(req.Offset)+req.Limit, len(p.data))]}
		var b bin.Buffer
		if err := result.Encode(&b); err != nil {
			return err
		}
		return output.Decode(&b)
	default:
		return errors.New("unexpected RPC")
	}
}

func configurePool(t *testing.T, size int) {
	t.Helper()
	viper.Reset()
	text := "[telegram]\ndownload_pool_size=8\n"
	if size == 1 {
		text = "[telegram]\ndownload_pool_size=1\n"
	}
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	if err := config.Init(t.Context(), path); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(viper.Reset)
}

func TestPoolReuseSessionIsolationAndClose(t *testing.T) {
	configurePool(t, 8)
	var calls atomic.Int32
	invokers := make(chan *testPoolInvoker, 10)
	makeManager := func() *poolManager {
		manager := newPoolManager(t.Context(), func(context.Context, int) (telegram.CloseInvoker, error) {
			calls.Add(1)
			p := new(testPoolInvoker)
			invokers <- p
			return p, nil
		})
		t.Cleanup(func() {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
		})
		return manager
	}
	first, second := makeManager(), makeManager()
	raw1, raw2 := tg.NewClient(new(testPoolInvoker)), tg.NewClient(new(testPoolInvoker))
	poolRegistry.Store(raw1, first)
	poolRegistry.Store(raw2, second)
	t.Cleanup(func() { poolRegistry.Delete(raw1); poolRegistry.Delete(raw2) })
	var wg sync.WaitGroup
	for range 30 {
		wg.Go(func() {
			if _, err := first.Client(t.Context(), 5); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("created %d pools for same DC", calls.Load())
	}
	a, _ := downloadClient(t.Context(), tfile.NewTGFile(nil, raw1, 1, "a", tfile.WithDC(5)))
	b, _ := downloadClient(t.Context(), tfile.NewTGFile(nil, raw2, 1, "b", tfile.WithDC(5)))
	if a == b || calls.Load() != 2 {
		t.Fatal("sessions shared a pool")
	}
	if _, err := first.Client(t.Context(), 2); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Fatal("different DC was not isolated")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Client(t.Context(), 5); err == nil {
		t.Fatal("closed pool reused")
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	close(invokers)
	for p := range invokers {
		if p.closed.Load() != 1 {
			t.Fatalf("closed %d times", p.closed.Load())
		}
	}
}

func TestPoolFallbackDownloadsOriginalBytes(t *testing.T) {
	for _, tt := range []struct {
		name       string
		size, dc   int
		registered bool
		reason     string
	}{
		{"disabled", 1, 5, true, "pool_disabled"},
		{"unknown DC", 8, 0, true, "no_dc"},
		{"unregistered", 8, 5, false, "session_not_registered"},
		{"creation failed", 8, 5, true, "pool_create_failed"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			configurePool(t, tt.size)
			data := []byte("original client still works")
			raw := tg.NewClient(&testPoolInvoker{data: data})
			if tt.registered {
				manager := newPoolManager(t.Context(), func(context.Context, int) (telegram.CloseInvoker, error) { return nil, errors.New("creation failed") })
				poolRegistry.Store(raw, manager)
				t.Cleanup(func() {
					poolRegistry.Delete(raw)
					if err := manager.Close(); err != nil {
						t.Error(err)
					}
				})
			}
			f := tfile.NewTGFile(&tg.InputDocumentFileLocation{ID: 1}, raw, int64(len(data)), "a", tfile.WithDC(tt.dc))
			got, reason := downloadClient(t.Context(), f)
			if got != raw || reason != tt.reason {
				t.Fatalf("fallback got=%T reason=%s", got, reason)
			}
			var output bytes.Buffer
			if _, err := NewDownloader(t.Context(), f).Stream(t.Context(), &output); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(output.Bytes(), data) {
				t.Fatal("fallback corrupted bytes")
			}
		})
	}
}

func TestPoolCancelledInitializationCanRetry(t *testing.T) {
	entered := make(chan struct{})
	var attempts atomic.Int32
	invoker := new(testPoolInvoker)
	manager := newPoolManager(t.Context(), func(ctx context.Context, _ int) (telegram.CloseInvoker, error) {
		if attempts.Add(1) == 1 {
			close(entered)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return invoker, nil
	})
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := manager.Client(ctx, 5); done <- err }()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error=%v", err)
	}
	if _, err := manager.Client(t.Context(), 5); err != nil {
		t.Fatal(err)
	}
	if invoker.closed.Load() != 0 {
		t.Fatal("task cancellation closed shared pool")
	}
}

func TestPoolShutdownDuringCreationClosesNewInvoker(t *testing.T) {
	entered := make(chan struct{})
	invoker := new(testPoolInvoker)
	manager := newPoolManager(t.Context(), func(ctx context.Context, _ int) (telegram.CloseInvoker, error) {
		close(entered)
		<-ctx.Done()
		return invoker, nil
	})
	done := make(chan error, 1)
	go func() { _, err := manager.Client(t.Context(), 5); done <- err }()
	<-entered
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil {
		t.Fatal("shutdown initialization succeeded")
	}
	if invoker.closed.Load() != 1 {
		t.Fatal("initialized pool leaked on shutdown")
	}
}

func TestPooledDownloadsUseSelectedDCAndKeepEOFWorkaround(t *testing.T) {
	configurePool(t, 8)
	data := bytes.Repeat([]byte{42}, 1024*1024)
	raw := tg.NewClient(&testPoolInvoker{data: []byte("wrong client")})
	var calls atomic.Int32
	manager := newPoolManager(t.Context(), func(_ context.Context, dc int) (telegram.CloseInvoker, error) {
		if dc != 5 {
			t.Errorf("DC=%d want 5", dc)
		}
		calls.Add(1)
		return &testPoolInvoker{data: data}, nil
	})
	poolRegistry.Store(raw, manager)
	t.Cleanup(func() {
		poolRegistry.Delete(raw)
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
	})
	file := tfile.NewTGFile(&tg.InputDocumentFileLocation{ID: 1}, raw, int64(len(data)), "pooled", tfile.WithDC(5))
	for _, parallel := range []bool{false, true} {
		var got []byte
		if parallel {
			got = make([]byte, len(data))
			if _, err := NewDownloader(t.Context(), file).WithThreads(4).Parallel(t.Context(), &memWriterAt{b: got}); err != nil {
				t.Fatal(err)
			}
		} else {
			var output bytes.Buffer
			if _, err := NewDownloader(t.Context(), file).Stream(t.Context(), &output); err != nil {
				t.Fatal(err)
			}
			got = output.Bytes()
		}
		if !bytes.Equal(got, data) {
			t.Fatal("did not download pooled data")
		}
	}
	if calls.Load() != 1 {
		t.Fatal("files did not reuse pool")
	}
}
