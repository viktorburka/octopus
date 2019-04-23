package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)


func TestDownloadVaryConfig(t *testing.T) {
	const fileSize = 1 * Megabyte
	file := generateFile(fileSize)

	memDl := NewMemDownloader()
	memDl.Files["mem://test/movie"] = file

	factory := testDlCreator{memDl}

	ctx, _ := context.WithCancel(context.Background())

	var minDlChunkSize, maxDlChunkSize uint64

	setToDefault := false
	minMax := strings.Split(os.Getenv("MIN_MAX_DL_CHUNK_SIZE"), ",")
	if setToDefault = len(minMax) != 2; !setToDefault {
		var err1, err2 error
		minDlChunkSize, err1 = strconv.ParseUint(minMax[0], 10, 64)
		maxDlChunkSize, err2 = strconv.ParseUint(minMax[1], 10, 64)
		setToDefault = err1 != nil || err2 != nil
	}
	if setToDefault {
		// no external config has been passed - set all to default
		minDlChunkSize = 16 * Kilobyte
		maxDlChunkSize = 16 * Kilobyte
	}

	maxDlGortnCount, err := strconv.Atoi(os.Getenv("MAX_DL_GORTN_COUNT"))
	if err != nil {
		maxDlGortnCount = 4
	}

	var kb uint64
	for kb = minDlChunkSize; kb <= maxDlChunkSize; kb *= 2 {
		for goroutines := 1; goroutines <= maxDlGortnCount; goroutines++ {
			dataChan := make(chan transData)
			go func() {
				config := dlConfig{kb, goroutines}
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
	}
}

func TestDownloadCtxCancellation(t *testing.T) {
	const fileSize = 64 * Kilobyte
	file := generateFile(fileSize)

	memDl := NewMemDownloader()
	memDl.Files["mem://test/movie"] = file
	memDl.Pause = 200 * time.Millisecond

	factory := testDlCreator{memDl}

	ctx, cancel := context.WithCancel(context.Background())

	dataChan := make(chan transData)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		config := dlConfig{16 * Kilobyte, 4}
		err := initiateDownload(ctx,"mem://test/movie", &factory, config, dataChan)
		if err != nil && err.Error() != ctx.Err().Error() { // only context cancellation error is acceptable
			t.Fatal(err)
		}
		close(dataChan)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := mergeDataChunks(dataChan, fileSize)
		if err != nil {
			t.Fatal(err)
		}
	}()

	// pause to let the goroutines start working so
	// the can be interrupted in the middle
	time.Sleep(memDl.Pause / 5)

	start := time.Now()

	cancel()
	wg.Wait()

	if time.Since(start) > memDl.Pause / 5 {
		t.Fatal("context cancellation took too long")
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
	Pause time.Duration
	Files map[string]memFile
}

func NewMemDownloader() *memDownloader {
	dl := memDownloader{
		Files: make(map[string]memFile),
	}
	return &dl
}

func (md *memDownloader) GetFileInfo(url string) (fileInfo, error) {
	info := fileInfo{
		Url: url,
		Size: uint64(len(md.Files[url].data)),
	}
	return info, nil
}

func (md *memDownloader) DownloadChunk(ctx context.Context, srcUrl string,
	start uint64, size uint64) ([]byte, error) {
	chunk := make([]byte, size)
	end := start + size
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(md.Pause):
		copy(chunk, md.Files[srcUrl].data[start:end])
	}
	return chunk, nil
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
