package main

import (
	"fmt"
	"log"
)

type downloader interface {
	download() error
}

type httpDownloader struct {
}

func getDownloaderForScheme(scheme string) (dl downloader, err error) {
	switch scheme {
	case "http":
		return &httpDownloader{}, nil
	default:
		return nil, fmt.Errorf("download scheme %v is not supported", scheme)
	}
}

func (h *httpDownloader) download() error {
	log.Println("http download done")
	return nil
}
