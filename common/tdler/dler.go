package tdler

import (
	"context"

	"github.com/charmbracelet/log"
	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/tg"

	"github.com/krau/SaveAny-Bot/common/utils/dlutil"
	"github.com/krau/SaveAny-Bot/config"
	"github.com/krau/SaveAny-Bot/pkg/consts/tglimit"
	"github.com/krau/SaveAny-Bot/pkg/tfile"
)

func NewDownloader(ctx context.Context, file tfile.TGFile) *downloader.Builder {
	client, reason := downloadClient(ctx, file)
	threads := dlutil.BestThreads(file.Size(), config.C().Threads)
	log.FromContext(ctx).Debug("Telegram download", "file", file.Name(), "dc", file.DC(), "threads", threads, "pool_size", max(1, config.C().Telegram.DownloadPoolSize), "pooled", reason == "", "reason", reason)
	return downloader.NewDownloader().WithPartSize(tglimit.MaxPartSize).
		Download(eofAwareClient{Client: client, size: file.Size()}, file.Location()).
		WithThreads(threads)
}

// eofAwareClient answers upload.getFile requests at or past the end of the
// file with an empty chunk. gotd's downloader is size-unaware: for files
// whose size is an exact multiple of the part size it issues one final
// request at offset == size and expects an empty chunk, but Telegram rejects
// it with 400 OFFSET_INVALID and the whole download fails.
type eofAwareClient struct {
	downloader.Client
	size int64
}

func (c eofAwareClient) UploadGetFile(ctx context.Context, req *tg.UploadGetFileRequest) (tg.UploadFileClass, error) {
	if c.size > 0 && req.Offset >= c.size {
		return &tg.UploadFile{}, nil
	}
	return c.Client.UploadGetFile(ctx, req)
}
