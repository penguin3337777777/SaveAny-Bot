// Package collect runs a bounded, in-memory scan. It does not persist jobs,
// cursors, manifests, deduplication history or per-message failure records.
package collect

import (
	"context"
	"errors"
	"fmt"
	"math"
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
	ScanComplete                             bool
}
type Task struct {
	ID, Tag     string
	Concurrency int
	Fetch       func(context.Context, int) (Page, error)
	Load        func(context.Context, []int) ([]*tg.Message, error)
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

// The plan contains identifiers only, never file data, captions or documents.
// This fixed limit keeps the in-memory plan small on a 1 GiB server.
const MaxPlannedMessages = 100000

var ErrPlanTooLarge = errors.New("collection plan exceeds memory limit")

type plannedGroup struct {
	IDs     [MaxAlbum]int32
	GroupID int64
	Count   uint8
	Videos  uint16
}

func (t *Task) Execute(ctx context.Context) (result error) {
	ctx, cancel := context.WithCancel(ctx)
	var pending sync.WaitGroup
	var mu sync.Mutex
	var stats Stats
	report := func(final bool, err error) {
		if t.Report != nil {
			t.Report(stats, final, err)
		}
	}
	defer func() { cancel(); pending.Wait(); mu.Lock(); defer mu.Unlock(); report(true, result) }()
	report(false, nil)
	if t.Load == nil {
		return errors.New("collection message loader unavailable")
	}
	var plan []plannedGroup
	plannedMessages := 0
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
		var entry plannedGroup
		for i, m := range group {
			if m.ID <= 0 || int64(m.ID) > math.MaxInt32 {
				return errors.New("invalid source message ID")
			}
			entry.IDs[i] = int32(m.ID)
			if IsVideo(m) {
				entry.Videos |= 1 << i
			}
		}
		if entry.Videos == 0 {
			return nil
		}
		if plannedMessages+len(group) > MaxPlannedMessages {
			return fmt.Errorf("%w: %d source messages", ErrPlanTooLarge, MaxPlannedMessages)
		}
		entry.Count = uint8(len(group))
		entry.GroupID = group[0].GroupedID
		plan = append(plan, entry)
		plannedMessages += len(group)
		for i := range group {
			if entry.Videos&(1<<i) != 0 {
				stats.Matched++
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
			stats.Scanned++
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
		report(false, nil)
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
	if err := ctx.Err(); err != nil {
		return err
	}
	stats.ScanComplete = true
	report(false, nil)

	// Only after the scan succeeds do we reload each planned group and save it.
	limit := make(chan struct{}, max(1, t.Concurrency))
	fail := func(err error) {
		stats.Failed++
		detail := []rune(err.Error())
		if len(detail) > 200 {
			detail = detail[:200]
		}
		stats.LastError = string(detail)
	}
	for _, entry := range plan {
		if err := ctx.Err(); err != nil {
			return err
		}
		ids := make([]int, entry.Count)
		for i := range ids {
			ids[i] = int(entry.IDs[i])
		}
		loaded, loadErr := t.Load(ctx, ids)
		if err := ctx.Err(); err != nil {
			return err
		}
		group := make([]*tg.Message, entry.Count)
		byID := make(map[int]*tg.Message, len(loaded))
		for _, m := range loaded {
			if m != nil {
				byID[m.ID] = m
			}
		}
		for i, id := range ids {
			group[i] = byID[id]
			if loadErr == nil && (group[i] == nil || group[i].GroupedID != entry.GroupID) {
				loadErr = fmt.Errorf("planned source message %d unavailable or album changed", id)
			}
		}
		if loadErr != nil {
			mu.Lock()
			for i := range ids {
				if entry.Videos&(1<<i) != 0 {
					fail(fmt.Errorf("reload source: %w", loadErr))
				}
			}
			report(false, nil)
			mu.Unlock()
			continue
		}
		for i := len(group) - 1; i >= 0; i-- {
			if entry.Videos&(1<<i) == 0 {
				continue
			}
			m := group[i]
			select {
			case limit <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}
			process := func() {
				defer func() { <-limit }()
				var skipped bool
				var err error
				if !IsVideo(m) {
					err = fmt.Errorf("planned video %d is no longer available", m.ID)
				} else {
					skipped, err = t.Process(ctx, m, group)
				}
				mu.Lock()
				defer mu.Unlock()
				if ctx.Err() != nil {
					return
				}
				if err != nil {
					fail(err)
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
