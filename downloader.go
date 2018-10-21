package main

import (
	"fmt"
	"log"
)

type Downloader interface {
	Download() error
}

type HttpDownloader struct {
}

func getDownloaderForScheme(scheme string) (dl Downloader, err error) {
	switch scheme {
	case "http":
		return &HttpDownloader{}, nil
	default:
		return nil, fmt.Errorf("download scheme %v is not supported", scheme)
	}
}

func (h *HttpDownloader) Download() error {
	log.Println("http download done")
	return nil
}
