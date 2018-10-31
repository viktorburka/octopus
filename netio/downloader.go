package netio

import (
    "context"
    "fmt"
	"io"
)

type dlData struct {
    data  []byte // data
    done  bool   // download complete
    br    int64  // bytes read
	total int64  // total bytes
}

type dlMessage struct {
    sender string
    err error
}

type receiver interface {
	GetFileInfo(ctx context.Context, uri string, options map[string]string) (info FileInfo, err error)
	OpenWithContext(ctx context.Context, uri string, opt map[string]string) error
	IsOpen() bool
	ReadPartWithContext(ctx context.Context, output io.WriteSeeker, opt map[string]string) (string, error)
	CancelWithContext(ctx context.Context) error
	CloseWithContext(ctx context.Context) error
}

type FileInfo struct {
	Size int64
}

type Downloader interface {
    Download(ctx context.Context, uri string, options map[string]string, data chan dlData, msg chan dlMessage, rc receiver)
}

func getProbeForScheme(scheme string) (r receiver, err error) {
    return getReceiver(scheme, 1)
}

func getDownloader(scheme string) (dl Downloader, err error) {
	switch scheme {
	case "http":
		fallthrough
	case "https":
		return DownloaderSimple{}, nil
	case "s3":
		return DownloaderConcurrent{}, nil
	default:
		return nil, fmt.Errorf("download scheme %v is not supported", scheme)
	}
}

func getReceiver(scheme string, size int64) (receiver, error) {
	switch scheme {
	case "http":
		fallthrough
	case "https":
		return &HttpReceiver{}, nil
	case "s3":
		return &S3ReceiverRanged{}, nil
	default:
		return nil, fmt.Errorf("receiver scheme %v is not supported", scheme)
	}
}
