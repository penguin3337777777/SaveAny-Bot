package tfile

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	filepkg "github.com/krau/SaveAny-Bot/pkg/tfile"
)

type finalRecorder struct {
	starts, finishes int
	err              error
}

func (p *finalRecorder) OnStart(context.Context, TaskInfo)                  { p.starts++ }
func (p *finalRecorder) OnProgress(context.Context, TaskInfo, int64, int64) {}
func (p *finalRecorder) OnDone(_ context.Context, _ TaskInfo, err error)    { p.finishes++; p.err = err }

func TestCacheCreationFailureReportsFinal(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocker, []byte("block directory creation"), 0600); err != nil {
		t.Fatal(err)
	}
	recorder := new(finalRecorder)
	task := &Task{File: filepkg.NewTGFile(nil, nil, 1, "a"), localPath: filepath.Join(blocker, "download"), Progress: recorder}
	err := task.Execute(t.Context())
	if err == nil || recorder.err == nil || recorder.starts != 1 || recorder.finishes != 1 {
		t.Fatalf("err=%v starts=%d finishes=%d final=%v", err, recorder.starts, recorder.finishes, recorder.err)
	}
}
