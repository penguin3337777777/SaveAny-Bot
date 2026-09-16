package collect

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/tg"
)

func video(id int, group int64, text string) *tg.Message {
	return &tg.Message{ID: id, GroupedID: group, Message: text, Media: &tg.MessageMediaDocument{Document: &tg.Document{ID: int64(id), MimeType: "video/mp4"}}}
}

func TestOptionsAndExactTags(t *testing.T) {
	for _, tc := range []struct {
		cmd   string
		valid bool
	}{
		{`/collect @group --tag "#教程"`, true},
		{`/collect @group --tag 教程 --storage 115 --type video`, true},
		{`/collect @group --storage 115`, false},
		{`/collect @group --tag "#two tags"`, false},
		{`/collect @group --tag x --type audio`, false},
		{`/collect @group --tag x --tag y`, false},
		{`/collect @group --tag x --unknown y`, false},
	} {
		t.Run(tc.cmd, func(t *testing.T) {
			o, e := Parse(tc.cmd)
			if (e == nil) != tc.valid {
				t.Fatalf("got %v", e)
			}
			if e == nil && o.Kind != "video" {
				t.Fatal(o)
			}
		})
	}
	o, err := Parse(`/collect @group --tag x`)
	if err != nil || o.Storage != "" {
		t.Fatal(o, err)
	}
	for _, tc := range []struct {
		text, tag string
		want      bool
	}{
		{"#教程推荐", "#教程", false}, {"#教程 #其他", "#教程", true}, {"#TAG", "#tag", true}, {"#tag_2", "#tag", false}, {"标签文字", "#标签", false},
	} {
		if got := Matches(tc.text, tc.tag); got != tc.want {
			t.Fatalf("%s got %v", tc.text, got)
		}
	}
}

func TestAlbumCaptionAcrossPagesAndDuplicatePageItems(t *testing.T) {
	caption := &tg.Message{ID: 8, GroupedID: 1, Message: "#tag"} // caption on non-video album member
	pages := []Page{
		{Messages: []*tg.Message{video(10, 1, ""), video(9, 1, "")}, Next: 9},
		{Messages: []*tg.Message{video(9, 1, ""), caption, video(7, 0, "#tagExtra"), video(6, 0, "#tag")}, Next: 6},
		{Done: true},
	}
	var processed []int
	var final Stats
	task := Task{Tag: "#tag", Fetch: func(ctx context.Context, o int) (Page, error) { p := pages[0]; pages = pages[1:]; return p, nil }, Process: func(ctx context.Context, m *tg.Message, g []*tg.Message) (bool, error) {
		processed = append(processed, m.ID)
		return m.ID == 9, nil
	}, Report: func(s Stats, f bool, e error) {
		if f {
			final = s
		}
	}}
	if err := task.Execute(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(processed, []int{9, 10, 6}) {
		t.Fatal(processed)
	}
	if final.Matched != 3 || final.Saved != 2 || final.Skipped != 1 || final.Scanned != 5 {
		t.Fatal(final)
	}
}

func TestFailedItemDoesNotStopFollowingItems(t *testing.T) {
	var final Stats
	task := Task{Tag: "#tag", Fetch: func(context.Context, int) (Page, error) {
		return Page{Messages: []*tg.Message{video(2, 0, "#tag"), video(1, 0, "#tag")}, Done: true}, nil
	}, Process: func(_ context.Context, m *tg.Message, _ []*tg.Message) (bool, error) {
		if m.ID == 2 {
			return false, fmt.Errorf("probe unavailable")
		}
		return false, nil
	}, Report: func(s Stats, f bool, e error) {
		if f {
			final = s
		}
	}}
	if err := task.Execute(t.Context()); err == nil {
		t.Fatal("failure lost")
	}
	if final.Failed != 1 || final.Saved != 1 || final.LastError != "probe unavailable" {
		t.Fatal(final)
	}
}

func TestCancelStopsPaginationAndProcessing(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	calls := 0
	task := Task{Tag: "#tag", Fetch: func(context.Context, int) (Page, error) {
		return Page{Messages: []*tg.Message{video(2, 0, "#tag"), video(1, 0, "#tag")}, Next: 1}, nil
	}, Process: func(context.Context, *tg.Message, []*tg.Message) (bool, error) {
		calls++
		cancel()
		return false, context.Canceled
	}}
	if err := task.Execute(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal(calls)
	}
}

func TestCursorCannotLoop(t *testing.T) {
	task := Task{Tag: "#tag", Fetch: func(context.Context, int) (Page, error) { return Page{Next: 5}, nil }}
	if err := task.Execute(t.Context()); err == nil {
		t.Fatal("expected stuck cursor error")
	}
}

func TestPathLocksCancellationAndRelease(t *testing.T) {
	var locks PathLocks
	unlock, err := locks.Acquire(t.Context(), "same")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err = locks.Acquire(ctx, "same"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	unlock()
	unlock()
	if len(locks.entries) != 0 {
		t.Fatal("retained completed paths")
	}
	unlock, err = locks.Acquire(t.Context(), "same")
	if err != nil {
		t.Fatal(err)
	}
	unlock()
}

func TestLargeScanRetainsOnlyOnePage(t *testing.T) {
	const count = 10000
	n := count
	processed := 0
	task := Task{Tag: "#tag", Fetch: func(context.Context, int) (Page, error) {
		p := Page{}
		for i := 0; i < PageSize && n > 0; i++ {
			p.Messages = append(p.Messages, video(n, 0, "#tag"))
			n--
		}
		p.Next = n + 1
		p.Done = n == 0
		return p, nil
	}, Process: func(context.Context, *tg.Message, []*tg.Message) (bool, error) { processed++; return true, nil }}
	if err := task.Execute(t.Context()); err != nil {
		t.Fatal(err)
	}
	if processed != count {
		t.Fatal(processed)
	}
}

func TestCollectConcurrencyBoundAndCancelWaitsForCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started := make(chan struct{}, 10)
	release := make(chan struct{})
	finished := make(chan error, 1)
	var active, peak, cleaned atomic.Int32
	task := &Task{Tag: "#x", Concurrency: 2}
	task.Fetch = func(context.Context, int) (Page, error) {
		return Page{Messages: []*tg.Message{video(4, 0, "#x"), video(3, 0, "#x"), video(2, 0, "#x"), video(1, 0, "#x")}, Done: true}, nil
	}
	task.Process = func(ctx context.Context, _ *tg.Message, _ []*tg.Message) (bool, error) {
		n := active.Add(1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		defer active.Add(-1)
		started <- struct{}{}
		<-ctx.Done()
		<-release
		cleaned.Add(1)
		return false, ctx.Err()
	}
	go func() { finished <- task.Execute(ctx) }()
	for range 2 {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatal("two files did not start concurrently")
		}
	}
	cancel()
	select {
	case <-finished:
		t.Fatal("returned before file cleanup")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancel deadlock")
	}
	if peak.Load() != 2 || active.Load() != 0 || cleaned.Load() < 2 {
		t.Fatalf("peak=%d active=%d cleaned=%d", peak.Load(), active.Load(), cleaned.Load())
	}
}

func TestCollectConcurrentCompletionCountsEveryOutcome(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var final Stats
	task := &Task{Tag: "#x", Concurrency: 2}
	task.Fetch = func(context.Context, int) (Page, error) {
		return Page{Messages: []*tg.Message{video(3, 0, "#x"), video(2, 0, "#x"), video(1, 0, "#x")}, Done: true}, nil
	}
	task.Process = func(ctx context.Context, m *tg.Message, _ []*tg.Message) (bool, error) {
		if m.ID > 1 {
			started <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return false, ctx.Err()
			}
		}
		if m.ID == 1 {
			return false, errors.New("sample failure")
		}
		return m.ID == 2, nil
	}
	task.Report = func(s Stats, last bool, _ error) {
		if last {
			final = s
		}
	}
	done := make(chan error, 1)
	go func() { done <- task.Execute(t.Context()) }()
	for range 2 {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatal("not concurrent")
		}
	}
	close(release)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("failure missing")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("not completed")
	}
	if final.Scanned != 3 || final.Matched != 3 || final.Saved != 1 || final.Skipped != 1 || final.Failed != 1 {
		t.Fatal(final)
	}
}
