package handlers

import (
	"context"
	"errors"
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

func TestCollectScanAndCompletionStatesKeepExistingLayout(t *testing.T) {
	i18n.Init("zh-Hans")
	cases := []struct {
		name  string
		stats collect.Stats
		final bool
		err   error
		want  string
	}{
		{name: "scan", want: "正在扫描"},
		{name: "save", stats: collect.Stats{Matched: 3, ScanComplete: true}, want: "扫描完成，正在保存"},
		{name: "success", stats: collect.Stats{Matched: 3, Saved: 3, ScanComplete: true}, final: true, want: "保存完成，全部成功"},
		{name: "skips", stats: collect.Stats{Matched: 3, Saved: 2, Skipped: 1, ScanComplete: true}, final: true, want: "保存完成（同名文件已跳过）"},
		{name: "empty", stats: collect.Stats{ScanComplete: true}, final: true, want: "没有匹配的视频"},
		{name: "failure", stats: collect.Stats{Matched: 1, Failed: 1, ScanComplete: true}, final: true, err: errors.New("failed"), want: "结束（存在失败）"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := &collectProgress{id: "job", stats: tc.stats}
			text, _, err := p.render(tc.final, tc.err)
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{tc.want, "已扫描消息：", "匹配视频：", "保存成功：", "同名跳过：", "失败："} {
				if !strings.Contains(text, want) {
					t.Fatalf("missing %q in %q", want, text)
				}
			}
		})
	}
}
