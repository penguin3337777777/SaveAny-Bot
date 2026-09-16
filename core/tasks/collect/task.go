// Package collect runs a bounded, in-memory scan. It does not persist jobs,
// cursors, manifests, deduplication history or per-message failure records.
package collect

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/gotd/td/tg"
	"github.com/krau/SaveAny-Bot/pkg/enums/tasktype"
)

const PageSize = 100
const MaxAlbum = 10

type Page struct {
	Messages []*tg.Message
	Next     int
	Done     bool
}
type Stats struct {
	Scanned, Matched, Saved, Skipped, Failed int
	LastError                                string
}
type Task struct {
	ID, Tag     string
	Concurrency int
	Fetch       func(context.Context, int) (Page, error)
	Process     func(context.Context, *tg.Message, []*tg.Message) (bool, error)
	Report      func(Stats, bool, error)
}

func (t *Task) TaskID() string          { return t.ID }
func (t *Task) Title() string           { return "collect " + t.Tag }
func (t *Task) Type() tasktype.TaskType { return tasktype.TaskTypeCollect }

func IsVideo(m *tg.Message) bool {
	media, ok := m.Media.(*tg.MessageMediaDocument)
	if !ok || media.Document == nil {
		return false
	}
	doc, ok := media.Document.AsNotEmpty()
	if !ok {
		return false
	}
	if strings.HasPrefix(doc.MimeType, "video/") {
		return true
	}
	for _, a := range doc.Attributes {
		if _, ok := a.(*tg.DocumentAttributeVideo); ok {
			return true
		}
		if f, ok := a.(*tg.DocumentAttributeFilename); ok {
			name := strings.ToLower(f.FileName)
			for _, ext := range []string{".mp4", ".mkv", ".avi", ".mov", ".webm", ".ts", ".m2ts", ".flv", ".wmv", ".rmvb"} {
				if strings.HasSuffix(name, ext) {
					return true
				}
			}
		}
	}
	return false
}

func (t *Task) Execute(ctx context.Context) (result error) {
	ctx, cancel := context.WithCancel(ctx)
	var pending sync.WaitGroup
	var mu sync.Mutex
	limit := make(chan struct{}, max(1, t.Concurrency))
	var stats Stats
	report := func(final bool, err error) {
		if t.Report != nil {
			t.Report(stats, final, err)
		}
	}
	defer func() {
		cancel()
		pending.Wait()
		mu.Lock()
		defer mu.Unlock()
		report(true, result)
	}()
	report(false, nil)
	// Keep at most one consecutive Telegram album across page boundaries.
	var album []*tg.Message
	flush := func() error {
		group := album
		album = nil
		matched := false
		for _, m := range group {
			matched = matched || Matches(m.Message, t.Tag)
		}
		if !matched {
			return nil
		}
		// Submit oldest-first inside a media group; concurrent completion may differ.
		for i := len(group) - 1; i >= 0; i-- {
			m := group[i]
			if !IsVideo(m) {
				continue
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			select {
			case limit <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}
			process := func() {
				defer func() { <-limit }()
				mu.Lock()
				stats.Matched++
				report(false, nil)
				mu.Unlock()
				skipped, err := t.Process(ctx, m, group)
				mu.Lock()
				defer mu.Unlock()
				if ctx.Err() != nil {
					return
				}
				if err != nil {
					stats.Failed++
					detail := []rune(err.Error())
					if len(detail) > 200 {
						detail = detail[:200]
					}
					stats.LastError = string(detail)
				} else if skipped {
					stats.Skipped++
				} else {
					stats.Saved++
				}
				report(false, nil)
			}
			if t.Concurrency <= 1 {
				process()
			} else {
				pending.Add(1)
				go func() { defer pending.Done(); process() }()
			}

		}
		return nil
	}
	offset, lastID := 0, 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := t.Fetch(ctx, offset)
		if err != nil {
			return fmt.Errorf("fetch history: %w", err)
		}
		if len(page.Messages) > PageSize {
			return errors.New("oversized history page")
		}
		sort.Slice(page.Messages, func(i, j int) bool { return page.Messages[i].ID > page.Messages[j].ID })
		for _, m := range page.Messages {
			if m.ID <= 0 || (lastID > 0 && m.ID >= lastID) {
				continue
			}
			lastID = m.ID
			mu.Lock()
			stats.Scanned++
			mu.Unlock()
			if len(album) > 0 && (m.GroupedID == 0 || m.GroupedID != album[0].GroupedID) {
				if err := flush(); err != nil {
					return err
				}
			}
			if len(album) == MaxAlbum {
				return errors.New("unexpected oversized Telegram album")
			}
			album = append(album, m)
			if m.GroupedID == 0 {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		mu.Lock()
		report(false, nil)
		mu.Unlock()
		if page.Done {
			break
		}
		if page.Next <= 0 || (offset > 0 && page.Next >= offset) {
			return errors.New("history cursor did not advance")
		}
		offset = page.Next
	}
	if err := flush(); err != nil {
		return err
	}
	pending.Wait()
	if err := ctx.Err(); err != nil {
		return err
	}
	if stats.Failed > 0 {
		return fmt.Errorf("%d collection items failed", stats.Failed)
	}
	return nil
}
