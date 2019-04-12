package main

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"testing"
	"time"
)

func TestDownload(t *testing.T) {
	dataChan := make(chan transData)
	ctx, _ := context.WithCancel(context.Background())
	err := initiateDownload(ctx,"mem://test/movie", getTestDownloader, dataChan)
	if err != nil {
		t.Fatal(err)
	}
}

func getTestDownloader(scheme string) (downloader, error) {
	switch scheme {
	case "mem":
		return NewMemDownloader(), nil
	default:
		return nil, fmt.Errorf("file transfer scheme %v is not supported", scheme)
	}
}

func generateFile(size int) memFile {
	rand.Seed(time.Now().UnixNano())
	file := memFile{
		data: make([]byte, size),
	}
	for i:=0; i<size; i++ {
		file.data[i] = byte(rand.Intn(256))
	}
	return file
}

type memFile struct {
	data []byte
}

type memDownloader struct {
	files map[string]memFile
}

func NewMemDownloader() *memDownloader {
	dl := memDownloader{
		files: make(map[string]memFile),
	}
	dl.files["/test/movie"] = generateFile(1 * Megabyte)
	return &dl
}

func (md *memDownloader) GetFileInfo(url string) (fileInfo, error) {
	info := fileInfo{
		Size: uint64(len(md.files[url].data)),
	}
	return info, nil
}

func (md *memDownloader) DownloadChunk(ctx context.Context, srcUrl string, start uint64, size uint64) ([]byte, error) {
	u, _ := url.Parse(srcUrl)
	end  := start + size - 1
	return md.files[u.Path].data[start:end], nil
}
