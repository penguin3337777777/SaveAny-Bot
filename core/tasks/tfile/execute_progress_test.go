package tfile

import (
	"context"
	"errors"
	"fmt"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
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

type pipelineDownloadInvoker struct{}

func (pipelineDownloadInvoker) Invoke(_ context.Context, input bin.Encoder, output bin.Decoder) error {
	req, ok := input.(*tg.UploadGetFileRequest)
	if !ok {
		return fmt.Errorf("unexpected request %T", input)
	}
	data := []byte("video-data")
	result := &tg.UploadFile{Type: &tg.StorageFileUnknown{}, Bytes: data[min(int(req.Offset), len(data)):min(int(req.Offset)+req.Limit, len(data))]}
	var b bin.Buffer
	if err := result.Encode(&b); err != nil {
		return err
	}
	return output.Decode(&b)
}
func TestCancelledUploadWaitDeletesDownloadedFile(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "video.mp4")
	raw := tg.NewClient(pipelineDownloadInvoker{})
	task := &Task{File: filepkg.NewTGFile(&tg.InputDocumentFileLocation{}, raw, 10, "video.mp4"), localPath: filename}
	called := false
	task.AwaitUpload = func(ctx context.Context) error {
		called = true
		data, err := os.ReadFile(filename)
		if err != nil || string(data) != "video-data" {
			t.Fatalf("download before wait: %q %v", data, err)
		}
		return context.Canceled
	}
	// Storage is nil: reaching upload after a canceled wait would panic.
	err := task.Execute(t.Context())
	if !called || !errors.Is(err, context.Canceled) {
		t.Fatalf("called=%v err=%v", called, err)
	}
	if _, err := os.Stat(filename); !os.IsNotExist(err) {
		t.Fatalf("temporary file retained: %v", err)
	}
}
