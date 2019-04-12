package main

import (
	"context"
	"fmt"
	"net/url"
	"sync"
)

const Megabyte  = 1024 * 1024

type fileInfo struct {
	Size uint64
}

type protectedError struct {
	err error
	m sync.Mutex
}

func (p *protectedError) setError(err error) {
	p.m.Lock()
	defer p.m.Unlock()
	if p.err == nil {
		p.err = err
	}
}

func (p *protectedError) getError() error {
	p.m.Lock()
	defer p.m.Unlock()
	return p.err
}

type downloader interface {
	GetFileInfo(url string) (fileInfo, error)
	DownloadChunk(ctx context.Context, url string, start uint64, size uint64) ([]byte, error)
}

// getDlFunc functional type is interface for create downloader function
type getDlFunc func (string) (downloader, error)

// getDownloader returns a downloader based on scheme
var getDownloader getDlFunc = func(scheme string) (downloader, error) {
	// The reason the function is declared through a var assignment
	// is to comply to getDlFunc type should it sometime change
	switch scheme {
	default:
		return nil, fmt.Errorf("file transfer scheme %v is not supported", scheme)
	}
}

func streamingDownload(ctx context.Context, srcUrl string, dataChan chan transData) error {
	return initiateDownload(ctx, srcUrl, getDownloader, dataChan)
}

func initiateDownload(ctx context.Context, srcUrl string, createDownloader getDlFunc, dataChan chan transData) error {
	u, err := url.Parse(srcUrl)
	if err != nil {
		return err
	}
	dl, err := createDownloader(u.Scheme)
	if err != nil {
		return err
	}
	info, err := dl.GetFileInfo(u.Path)
	if err != nil {
		return err
	}
	return startDownload(ctx, dl, srcUrl, info.Size, dataChan)
}

func startDownload(ctx context.Context, dl downloader, srcUrl string, size uint64, dataChan chan transData) error {
	const ChunkSize   = 1 * Megabyte
	const Concurrency = 3 // can't be 0

	// determine the number of chunks we can split the download to
	totalChunksCount := getChunkCount(size, ChunkSize)

	ctlChan := make(chan struct{}, Concurrency) // control channel controls the number of goroutines

	var opErr protectedError // operation error to be returned from the function
	var wg sync.WaitGroup

	var i uint64
	loop:
	for i = 0; i < totalChunksCount; i++ {
		select {
			case ctlChan<- struct{}{}:
				wg.Add(1)
				go func(chunkNumber uint64) {
					defer wg.Done()
					defer func() {<-ctlChan}() // unlock semaphore
					startByte := uint64(chunkNumber*ChunkSize)
					chunkSize := min(ChunkSize, size - startByte)
					data, err := dl.DownloadChunk(ctx, srcUrl, startByte, chunkSize)
					if err != nil {
						opErr.setError(err)
						return
					}
					chunk := transData{data, startByte, chunkSize}
					if err := writeChunk(ctx, dataChan, chunk); err != nil {
						opErr.setError(err)
						return
					}
				}(i)
			case <-ctx.Done():
				// break the loop and wait for active goroutines to finish
				break loop
		}
	}

	wg.Wait()

	return opErr.getError()
}

func writeChunk(ctx context.Context, dataChan chan transData, chunk transData) error {
	select {
	case dataChan<- chunk:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func getChunkCount(totalSize uint64, chunkSize uint64) uint64 {
	chunkCount := totalSize / chunkSize
	if totalSize % chunkSize > 0 {
		chunkCount += 1
	}
	return chunkCount
}

func min(v1 uint64, v2 uint64) uint64 {
	if v1 < v2 {
		return v1
	}
	return v2
}
