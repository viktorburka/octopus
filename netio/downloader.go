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

type FileInfo struct {
	Size int64
}

type Downloader interface {
	GetFileInfo(ctx context.Context, uri string, options map[string]string) (info FileInfo, err error)
    Download(ctx context.Context, uri string, options map[string]string, data chan dlData, msg chan dlMessage)
}

func getProbeForScheme(scheme string) (dl Downloader, err error) {
    switch scheme {
    case "http":
       fallthrough
    case "https":
        return DownloaderHttp{}, nil
    case "s3":
        return DownloaderS3{}, nil
    default:
        return nil, fmt.Errorf("download scheme %v is not supported", scheme)
    }
}

func getDownloader(scheme string, size int64) (dl Downloader, err error) {
	return getProbeForScheme(scheme)
}
