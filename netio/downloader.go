package netio

import (
    "context"
    "fmt"
)

type dlData struct {
    data []byte
    size int
    done bool
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
        return &S3DownloaderAws{}, nil
    default:
        return nil, fmt.Errorf("download scheme %v is not supported", scheme)
    }
}
