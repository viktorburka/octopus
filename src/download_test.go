package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestDownload(t *testing.T) {

	const fileSize = 1 * Megabyte

	file := generateFile(fileSize)

	memDl := NewMemDownloader()
	memDl.files["mem://test/movie"] = file

	factory := testDlCreator{memDl}
	config  := dlConfig{16 * Kilobyte, 4}

	dataChan := make(chan transData)
	ctx, _ := context.WithCancel(context.Background())
	go func() {
		err := initiateDownload(ctx,"mem://test/movie", &factory, config, dataChan)
		if err != nil {
			t.Fatal(err)
		}
		close(dataChan)
	}()
	data, err := mergeDataChunks(dataChan, fileSize)
	if err != nil {
		t.Fatal(err)
	}

	h := md5.New()
	if bytes.Compare(h.Sum(file.data), h.Sum(data)) != 0 {
		t.Fatal("original and received data md5 sums don't match")
	}
}

func mergeDataChunks(dataChan chan transData, size uint64) ([]byte, error) {
	data := make([]byte, size)
	for chunk := range dataChan {
		copy(data[chunk.Start:], chunk.Data)
	}
	return data, nil
}

func generateFile(size uint64) memFile {
	rand.Seed(time.Now().UnixNano())
	file := memFile{
		data: make([]byte, size),
	}
	var i uint64
	for i=0; i<size; i++ {
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
	return &dl
}

func (md *memDownloader) GetFileInfo(url string) (fileInfo, error) {
	info := fileInfo{
		Url: url,
		Size: uint64(len(md.files[url].data)),
	}
	return info, nil
}

func (md *memDownloader) DownloadChunk(ctx context.Context, srcUrl string, start uint64, size uint64) ([]byte, error) {
	end := start + size
	return md.files[srcUrl].data[start:end], nil
}

type testDlCreator struct {
	dl downloader
}

func (c *testDlCreator) CreateDownloader(scheme string) (downloader, error) {
	switch scheme {
	case "mem":
		return c.dl, nil
	default:
		return nil, fmt.Errorf("file transfer scheme %v is not supported", scheme)
	}
}
