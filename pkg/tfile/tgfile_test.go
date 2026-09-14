package tfile

import (
	"testing"

	"github.com/gotd/td/tg"
)

func TestMediaDCPreserved(t *testing.T) {
	for _, tt := range []struct {
		name  string
		media tg.MessageMediaClass
		dc    int
	}{
		{"document", &tg.MessageMediaDocument{Document: &tg.Document{ID: 1, DCID: 5, Size: 100}}, 5},
		{"photo", &tg.MessageMediaPhoto{Photo: &tg.Photo{ID: 2, DCID: 2, Sizes: []tg.PhotoSizeClass{&tg.PhotoSize{Type: "x", W: 10, H: 10, Size: 100}}}}, 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f, err := FromMedia(tt.media, nil)
			if err != nil {
				t.Fatal(err)
			}
			if f.DC() != tt.dc {
				t.Fatalf("DC=%d want %d", f.DC(), tt.dc)
			}
			m, err := FromMediaMessage(tt.media, nil, &tg.Message{ID: 1})
			if err != nil {
				t.Fatal(err)
			}
			if m.DC() != tt.dc {
				t.Fatalf("message DC=%d want %d", m.DC(), tt.dc)
			}
		})
	}
	if f := NewTGFile(nil, nil, 0, "unknown"); f.DC() != 0 {
		t.Fatalf("unknown DC=%d", f.DC())
	}
}
