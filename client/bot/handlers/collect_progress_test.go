package handlers

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/krau/SaveAny-Bot/common/i18n"
	"github.com/krau/SaveAny-Bot/common/i18n/i18nk"
	"github.com/krau/SaveAny-Bot/core/tasks/collect"
)

func TestCollectProgressMultipleFilesEscapingAndConfirmation(t *testing.T) {
	i18n.Init("zh-Hans")
	p := &collectProgress{id: "job", items: map[string]*collectItemState{
		"1": {name: "<b>A&B</b>.mp4", phase: i18nk.CollectDownloading, bytes: 50, total: 100, started: time.Now()},
		"2": {name: "other.mp4", phase: i18nk.CollectConfirming, bytes: 100, total: 100, started: time.Now()},
	}}
	text, entities, err := p.render(false, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"<b>A&B</b>.mp4", "下载中", "等待存储完成", "50%", "100%"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %s in %s", want, text)
		}
	}
	blocks, codes, bolds := 0, 0, 0
	for _, e := range entities {
		switch e.(type) {
		case *tg.MessageEntityBlockquote:
			blocks++
		case *tg.MessageEntityCode:
			codes++
		case *tg.MessageEntityBold:
			bolds++
		}
	}
	if blocks != 2 || codes != 3 || bolds != 3 {
		t.Fatalf("blocks=%d codes=%d bolds=%d", blocks, codes, bolds)
	}
	row := &collectItemProgress{parent: p, id: "2"}
	row.OnUploadStart(t.Context(), nil, 100)
	if p.items["2"].bytes != 0 || p.items["2"].phase != i18nk.CollectUploading {
		t.Fatal("retry did not reset phase")
	}
	row.OnUploadProgress(t.Context(), nil, 100, 100)
	if p.items["2"].phase != i18nk.CollectConfirming {
		t.Fatal("confirmation not shown")
	}
	row.close()
	if len(p.items) != 1 {
		t.Fatal("completed row retained")
	}
}
func TestCollectProgressConcurrentCallbacksAndFinal(t *testing.T) {
	i18n.Init("zh-Hans")
	p := &collectProgress{id: "job"}
	p.report(collect.Stats{}, false, nil)
	rows := []*collectItemProgress{p.add("1"), p.add("2")}
	var wg sync.WaitGroup
	for _, r := range rows {
		wg.Go(func() {
			for n := range 100 {
				r.OnProgress(t.Context(), nil, int64(n), 100)
			}
		})
	}
	wg.Wait()
	for _, r := range rows {
		r.close()
	}
	p.report(collect.Stats{Saved: 2}, true, nil)
	rows[0].OnProgress(context.Background(), nil, 100, 100)
	if len(p.items) != 0 || !p.done {
		t.Fatal("final state not terminal")
	}
}
