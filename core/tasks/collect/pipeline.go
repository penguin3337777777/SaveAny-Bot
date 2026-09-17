package collect

import (
	"context"
	"sync"
)

// Pipeline retains at most two files: one uploading, and one downloading or
// waiting for upload. A lease lives until the file's temporary data is removed.
type Pipeline struct {
	files, download, upload chan struct{}
}

func NewPipeline() *Pipeline {
	return &Pipeline{make(chan struct{}, 2), make(chan struct{}, 1), make(chan struct{}, 1)}
}

type Lease struct {
	pipeline               *Pipeline
	downloading, uploading bool
	once                   sync.Once
}

func take(ctx context.Context, slot chan struct{}) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case slot <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (p *Pipeline) Acquire(ctx context.Context) (*Lease, error) {
	if err := take(ctx, p.files); err != nil {
		return nil, err
	}
	if err := take(ctx, p.download); err != nil {
		<-p.files
		return nil, err
	}
	return &Lease{pipeline: p, downloading: true}, nil
}

// BeginUpload releases the download stage before waiting for the upload stage.
// The resident-file slot remains occupied, preventing a disk-backed backlog.
// A lease is owned by one file goroutine; Release follows BeginUpload's return.
func (l *Lease) BeginUpload(ctx context.Context) error {
	if l.downloading {
		<-l.pipeline.download
		l.downloading = false
	}
	if err := take(ctx, l.pipeline.upload); err != nil {
		return err
	}
	l.uploading = true
	return nil
}
func (l *Lease) Release() {
	l.once.Do(func() {
		if l.downloading {
			<-l.pipeline.download
		}
		if l.uploading {
			<-l.pipeline.upload
		}
		<-l.pipeline.files
	})
}
