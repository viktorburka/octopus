package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)


func TestChunkCountCalculation(t *testing.T) {

	type chunkTest struct {
		FileSize   uint64
		ChunkSize  uint64
		ChunkCount uint64
	}
	table := []chunkTest {
		{ 100,  25,  4},
		{  32,  15,  3},
		{   0, 100,  0},
		{1111, 123, 10},
	}

	for _, testCase := range table {
		count := getChunkCount(testCase.FileSize, testCase.ChunkSize)
		if count != testCase.ChunkCount {
			t.Fatal("expected", testCase.ChunkCount, "chunk count but got", count)
		}
	}
}

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
			// errChan to prevent goroutine deadlock since
			// t.Fatal() won't fail the test as it supposed to
			errChan  := make(chan error)
			dataChan := make(chan transData)
			// test
			go func() {
				defer close(dataChan)
				config := dlConfig{kb, goroutines}
				err := initiateDownload(ctx,"mem://test/movie", &factory, config, dataChan)
				if err != nil {
					errChan<- err
				}
			}()
			// verify
			receivedFile := make([]byte, fileSize)
			loop:
			for {
				select {
				case chunk, ok := <-dataChan:
					copy(receivedFile[chunk.Start:], chunk.Data)
					if !ok { // channel closed
						break loop
					}
				case err := <-errChan:
					t.Fatal(err)
				}
			}
			h := md5.New()
			if bytes.Compare(h.Sum(file.data), h.Sum(receivedFile)) != 0 {
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
	// errChan to prevent goroutine deadlock since
	// t.Fatal() won't fail the test as it supposed to
	errChan  := make(chan error)

	//var wg sync.WaitGroup
	//wg.Add(1)
	go func() {
		//defer wg.Done()
		defer close(dataChan)
		config := dlConfig{16 * Kilobyte, 4}
		err := initiateDownload(ctx,"mem://test/movie", &factory, config, dataChan)
		if err != nil && err.Error() != ctx.Err().Error() { // only context cancellation error is acceptable
			errChan<- err
		}
	}()

	time.Sleep(memDl.Pause / 5)

	start := time.Now()
	cancel()

	// verify
	loop:
	for {
		select {
		case _, ok := <-dataChan:
			if !ok { // channel closed
				break loop
			}
		case err := <-errChan:
			t.Fatal(err)
		}
	}

	if time.Since(start) > memDl.Pause/ 5 {
		t.Fatal("context cancellation took too long")
	}
}

func TestDownloadChunkSizes(t *testing.T) {
	//const fileSize = 1 * Gigabyte + 200 * Megabyte + 31 * Kilobyte + 71
	const chunkSize = 12345
	const fileSize  = 1 * Megabyte + 123 * Kilobyte + 21
	const filePath  = "nop://test/movie"

	nopDl := NewNoOpDownloader()
	nopDl.Files[filePath] = nopFile{fileSize}
	nopDl.PauseMs = 50
	nopDl.RandomPause = true // randomize goroutines to break the order of incoming chunks

	factory := testDlCreator{nopDl}

	ctx, _ := context.WithCancel(context.Background())

	dataChan := make(chan transData)
	// errChan to prevent goroutine deadlock since
	// t.Fatal() won't fail the test as it supposed to
	errChan  := make(chan error)
	go func() {
		defer close(dataChan)
		config := dlConfig{chunkSize, 4}
		err := initiateDownload(ctx, filePath, &factory, config, dataChan)
		if err != nil {
			errChan<- err
		}
	}()
	chunks := make([]transData, 0, fileSize / chunkSize)
	loop:
	for {
		select {
		case chunk, ok := <-dataChan:
			if !ok { // channel closed
				break loop
			}
			chunks = append(chunks, chunk)
		case err := <-errChan:
			t.Fatal(err)
		}
	}
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].Start < chunks[j].Start
	})
	for i := 0; i < len(chunks)-1; i++ {
		cur  := chunks[i]
		next := chunks[i+1]
		if cur.Start + cur.Size != next.Start {
			t.Fatal("received chunks are not contiguous")
		}
	}
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

type nopFile struct {
	Size uint64
}

type nopDownloader struct {
	Files       map[string]nopFile
	PauseMs     time.Duration // Pause in milliseconds
	RandomPause bool
}

func NewNoOpDownloader() *nopDownloader {
	nop := nopDownloader{
		Files: make(map[string]nopFile),
	}
	return &nop
}

func (nop *nopDownloader) GetFileInfo(url string) (fileInfo, error) {
	file, ok := nop.Files[url]
	if !ok {
		return fileInfo{}, fmt.Errorf("file not found: %v", url)
	}
	info := fileInfo{
		Url: url,
		Size: file.Size,
	}
	return info, nil
}

func (nop *nopDownloader) DownloadChunk(ctx context.Context, srcUrl string,
	start uint64, size uint64) ([]byte, error) {
	if nop.RandomPause {
		src := rand.NewSource(time.Now().UnixNano())
		r := rand.New(src)
		n := r.Intn(int(nop.PauseMs))
		select {
		case <-ctx.Done():
		case <-time.After(time.Duration(n)*time.Millisecond):
		}
	}
	return []byte(""), nil
}

type testDlCreator struct {
	dl downloader
}

func (c *testDlCreator) CreateDownloader(scheme string) (downloader, error) {
	switch scheme {
	case "mem":
		return c.dl, nil
	case "nop":
		return c.dl, nil
	default:
		return nil, fmt.Errorf("file transfer scheme %v is not supported", scheme)
	}
}
