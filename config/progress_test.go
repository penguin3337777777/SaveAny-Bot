package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/viper"
)

func TestDownloadAndProgressConfig(t *testing.T) {
	for _, tt := range []struct {
		name, text    string
		pool, seconds int
	}{
		{"old config", "", 8, 15},
		{"explicit", "[telegram]\ndownload_pool_size=1\n[progress]\nupdate_interval_seconds=30\n", 1, 30},
		{"clamp", "[telegram]\ndownload_pool_size=0\n[progress]\nupdate_interval_seconds=-5\n", 1, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			viper.Reset()
			old := *cfg
			t.Cleanup(func() { *cfg = old; viper.Reset() })
			path := filepath.Join(t.TempDir(), "config.toml")
			if err := os.WriteFile(path, []byte(tt.text), 0600); err != nil {
				t.Fatal(err)
			}
			if err := Init(t.Context(), path); err != nil {
				t.Fatal(err)
			}
			if C().Telegram.DownloadPoolSize != tt.pool || ProgressInterval() != time.Duration(tt.seconds)*time.Second {
				t.Fatalf("got pool=%d interval=%v", C().Telegram.DownloadPoolSize, ProgressInterval())
			}
		})
	}
	t.Run("environment", func(t *testing.T) {
		viper.Reset()
		old := *cfg
		t.Cleanup(func() { *cfg = old; viper.Reset() })
		t.Setenv("SAVEANY_TELEGRAM_DOWNLOAD_POOL_SIZE", "3")
		t.Setenv("SAVEANY_PROGRESS_UPDATE_INTERVAL_SECONDS", "20")
		path := filepath.Join(t.TempDir(), "config.toml")
		if err := os.WriteFile(path, nil, 0600); err != nil {
			t.Fatal(err)
		}
		if err := Init(t.Context(), path); err != nil {
			t.Fatal(err)
		}
		if C().Telegram.DownloadPoolSize != 3 || ProgressInterval() != 20*time.Second {
			t.Fatal("environment overrides were not loaded")
		}
	})
}
