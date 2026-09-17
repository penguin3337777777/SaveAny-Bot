package handlers

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gotd/td/tg"
	"github.com/krau/SaveAny-Bot/common/i18n"
	"github.com/krau/SaveAny-Bot/common/i18n/i18nk"
	"github.com/krau/SaveAny-Bot/common/progressmsg"
	"github.com/krau/SaveAny-Bot/common/utils/tgutil"
	"github.com/krau/SaveAny-Bot/core/tasks/collect"
	tftask "github.com/krau/SaveAny-Bot/core/tasks/tfile"
)

// All collection commands share one download lane and one upload lane.
var collectPipeline = collect.NewPipeline()

type collectItemState struct {
	name         string
	phase        i18nk.Key
	bytes, total int64
	started      time.Time
	attempt      int
}
type collectProgress struct {
	once       sync.Once
	stop       chan struct{}
	mu         sync.Mutex
	editor     *progressmsg.Editor
	id         string
	messageID  int
	stats      collect.Stats
	items      map[string]*collectItemState
	done       bool
	lastRender time.Time
}

func (p *collectProgress) report(s collect.Stats, final bool, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return
	}
	p.once.Do(func() {
		p.stop = make(chan struct{})
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-p.stop:
					return
				case <-ticker.C:
					p.mu.Lock()
					if !p.done {
						p.emit(false, nil)
					}
					p.mu.Unlock()
				}
			}
		}()
	})
	p.stats = s
	p.done = final
	if final {
		close(p.stop)
	}
	p.emit(final, err)
}
func (p *collectProgress) emit(final bool, err error) {
	if !final && time.Since(p.lastRender) < time.Second {
		return
	}
	p.lastRender = time.Now()
	text, entities, e := p.render(final, err)
	if e != nil {
		return
	}
	req := &tg.MessagesEditMessageRequest{ID: p.messageID}
	req.SetMessage(text)
	req.SetEntities(entities)
	if final {
		req.SetReplyMarkup(&tg.ReplyInlineMarkup{})
	} else {
		req.SetReplyMarkup(collectCancelMarkup(p.id))
	}
	p.editor.Submit(req, false, final)
}
func (p *collectProgress) render(final bool, err error) (string, []tg.MessageEntityClass, error) {
	state := i18nk.CollectRunning
	if final {
		state = i18nk.CollectCompleted
	}
	if err != nil {
		state = i18nk.CollectFailed
	}
	if errors.Is(err, context.Canceled) {
		state = i18nk.CollectCancelled
	}
	detail := p.stats.LastError
	if err != nil && detail == "" {
		detail = err.Error()
	}
	html := i18n.T(i18nk.CollectProgress, tgutil.EscapeHTMLTemplateData(map[string]any{
		"ID": p.id, "State": i18n.T(state), "Scanned": p.stats.Scanned, "Matched": p.stats.Matched, "Saved": p.stats.Saved, "Skipped": p.stats.Skipped, "Failed": p.stats.Failed, "Error": detail,
	}))
	if !final {
		keys := make([]string, 0, len(p.items))
		for k := range p.items {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		// Keep Telegram's message limit even with unusually large worker settings.
		for index, k := range keys {
			if index == 4 {
				html += "\n" + i18n.T(i18nk.CollectMoreActive, map[string]any{"Count": len(keys) - index})
				break
			}
			item := p.items[k]
			name := []rune(item.name)
			if len(name) > 100 {
				name = append(name[:99], '…')
			}
			percent := int64(0)
			if item.total > 0 {
				percent = min(100, item.bytes*100/item.total)
			}
			bar := strings.Repeat("█", int(percent/10)) + strings.Repeat("░", 10-int(percent/10))
			rate := float64(0)
			if !item.started.IsZero() && item.phase != i18nk.CollectConfirming {
				rate = float64(item.bytes) / max(time.Since(item.started).Seconds(), 0.001)
			}
			html += "\n" + i18n.T(i18nk.CollectItem, tgutil.EscapeHTMLTemplateData(map[string]any{
				"Name": string(name), "Phase": i18n.T(item.phase), "Bar": bar, "Percent": percent, "Done": collectSize(float64(item.bytes)), "Total": collectSize(float64(item.total)), "Rate": collectSize(rate), "Attempt": item.attempt,
			}))
		}
	}
	return tgutil.RenderHTML(html)
}
func collectSize(n float64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	i := 0
	for n >= 1024 && i < len(units)-1 {
		n /= 1024
		i++
	}
	return fmt.Sprintf("%.1f %s", n, units[i])
}

type collectItemProgress struct {
	lease  *collect.Lease
	parent *collectProgress
	id     string
}

func (p *collectProgress) add(id string) *collectItemProgress {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.items == nil {
		p.items = make(map[string]*collectItemState)
	}
	p.items[id] = &collectItemState{name: id, phase: i18nk.CollectChecking}
	p.emit(false, nil)
	return &collectItemProgress{parent: p, id: id}
}
func (i *collectItemProgress) update(fn func(*collectItemState)) {
	p := i.parent
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return
	}
	s := p.items[i.id]
	if s == nil {
		return
	}
	fn(s)
	p.emit(false, nil)
}
func (i *collectItemProgress) close() {
	p := i.parent
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.items, i.id)
	if !p.done {
		p.emit(false, nil)
	}
}
func (i *collectItemProgress) OnStart(_ context.Context, info tftask.TaskInfo) {
	i.update(func(s *collectItemState) {
		s.name = info.FileName()
		s.total = info.FileSize()
		s.bytes = 0
		s.started = time.Now()
		s.phase = i18nk.CollectDownloading
	})
}
func (i *collectItemProgress) OnProgress(_ context.Context, _ tftask.TaskInfo, n, total int64) {
	i.update(func(s *collectItemState) { s.bytes = max(s.bytes, n); s.total = total })
}

// Keep the row until Process returns, so task counters and cleanup precede removal.
func (i *collectItemProgress) OnDone(context.Context, tftask.TaskInfo, error) {}
func (i *collectItemProgress) OnUploadStart(_ context.Context, _ tftask.TaskInfo, total int64) {
	i.update(func(s *collectItemState) {
		s.bytes = 0
		s.total = total
		s.started = time.Now()
		s.attempt++
		s.phase = i18nk.CollectUploading
	})
}
func (i *collectItemProgress) OnUploadProgress(_ context.Context, _ tftask.TaskInfo, n, total int64) {
	i.update(func(s *collectItemState) {
		if n < s.bytes {
			s.started = time.Now()
			s.attempt++
			s.phase = i18nk.CollectUploading
		}
		s.bytes = n
		s.total = total
		if total > 0 && n >= total {
			s.phase = i18nk.CollectConfirming
		}
	})
}

func (i *collectItemProgress) awaitUpload(ctx context.Context) error {
	i.update(func(s *collectItemState) {
		s.phase = i18nk.CollectWaitingUpload
		s.bytes = s.total
		s.started = time.Time{}
	})
	return i.lease.BeginUpload(ctx)
}
