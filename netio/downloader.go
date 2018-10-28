package netio

import (
    "context"
    "fmt"
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

type Downloader interface {
    Download(ctx context.Context, uri string, options map[string]string, data chan dlData, msg chan dlMessage)
}

func getDownloaderForScheme(scheme string) (dl Downloader, err error) {
    switch scheme {
    case "http":
       fallthrough
    case "https":
        return &HttpDownloader{}, nil
    case "s3":
        return &S3Downloader{}, nil
    default:
        return nil, fmt.Errorf("download scheme %v is not supported", scheme)
    }
}
