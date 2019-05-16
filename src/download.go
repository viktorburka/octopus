package main

import (
	"context"
	"fmt"
	"net/url"
	"sync"
)

const Kilobyte  = 1024
const Megabyte  = 1024 * Kilobyte
const Gigabyte  = 1024 * Megabyte

type fileInfo struct {
	Url string
	Size uint64
}

type protectedError struct {
	err error
	m sync.Mutex
}

func (p *protectedError) SetError(err error) {
	p.m.Lock()
	defer p.m.Unlock()
	if p.err == nil {
		p.err = err
	}
}

func (p *protectedError) GetError() error {
	p.m.Lock()
	defer p.m.Unlock()
	return p.err
}

type dlConfig struct {
	ChunkSize uint64
	Concurrency int
}

type downloader interface {
	GetFileInfo(url string) (fileInfo, error)
	DownloadChunk(ctx context.Context, url string, start uint64, size uint64) ([]byte, error)
}

type dlFactory interface {
	CreateDownloader(scheme string) (downloader, error)
}

type dlCreator struct {
}

//type opErr struct {
//	Err error
//}
//
//func (e *opErr) SetErr(err error) {
//	if e.Err == nil {
//		e.Err = err
//	}
//}

func (c *dlCreator) CreateDownloader(scheme string) (downloader, error) {
	switch scheme {
	default:
		return nil, fmt.Errorf("downloader file transfer scheme %v is not supported", scheme)
	}
}

func streamingDownload(ctx context.Context, srcUrl string, cfg dlConfig, dataChan chan transData) error {
	return initiateDownload(ctx, srcUrl, &dlCreator{}, cfg, dataChan)
}

func initiateDownload(ctx context.Context, srcUrl string, factory dlFactory, cfg dlConfig,
	dataChan chan transData) error {
	u, err := url.Parse(srcUrl)
	if err != nil {
		return err
	}
	dl, err := factory.CreateDownloader(u.Scheme)
	if err != nil {
		return err
	}
	info, err := dl.GetFileInfo(srcUrl)
	if err != nil {
		return err
	}
	return startDownload(ctx, info, dl, cfg, dataChan)
}

func startDownload(ctx context.Context, info fileInfo, dl downloader, cfg dlConfig, dataChan chan transData) error {
	// operation error to be returned from the function
	var opErr error
	//var result opErr

	// determine the number of chunks we can split the download to
	totalChunksCount := getChunkCount(info.Size, cfg.ChunkSize)

	semaphore := make(chan struct{}, cfg.Concurrency) // control channel controls the number of goroutines
	errorChan := make(chan error)

	dlCtx, cancel := context.WithCancel(ctx)

	var wg sync.WaitGroup

	var i uint64
	loop:
	for {
		select {
			case semaphore <- struct{}{}: // this controls number of goroutines
				if i == totalChunksCount {
					<-semaphore // release semaphore before exit
					break loop
				}
				wg.Add(1)
				go func(chunkNumber uint64) {
					defer wg.Done()
					defer func() { <-semaphore }() // unlock semaphore
					startByte := uint64(chunkNumber*cfg.ChunkSize)
					chunkSize := min(cfg.ChunkSize, info.Size - startByte)
					data, err := dl.DownloadChunk(dlCtx, info.Url, startByte, chunkSize)
					if err != nil {
						setError(dlCtx, errorChan, err)
						return
					}
					chunk := transData{data, startByte, chunkSize}
					if err := writeChunk(dlCtx, dataChan, chunk); err != nil {
						setError(dlCtx, errorChan, err)
						return
					}
				}(i)
				i++
			case err := <-errorChan:
				opErr = err
				cancel() // cancel all other goroutines
				break loop // break also makes sure we don't overwrite opErr with ctx.Err()
			case <-dlCtx.Done():
				opErr = dlCtx.Err()
				break loop
		}
	}

	wg.Wait()

	return opErr
}

func writeChunk(ctx context.Context, dataChan chan transData, chunk transData) error {
	select {
	case dataChan<- chunk:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func setError(ctx context.Context, errorChan chan error, err error) {
	select {
	case errorChan<- err:
	case <-ctx.Done():
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
