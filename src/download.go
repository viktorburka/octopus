package main

import (
	"context"
	"fmt"
	"net/url"
)

const Megabyte  = 1024 * 1024

type fileInfo struct {
	Size uint64
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
	const Concurrency = 3

	// determine the number of chunks we can split the download to
	totalChunksCount := getChunkCount(size, ChunkSize)

	chunkChan := make(chan struct{})
	errChan   := make(chan error)
	semaphore := make(chan struct{}, Concurrency)

	var i uint64
	for i = 0; i < totalChunksCount; i++ {
		semaphore <- struct{}{} // stop if we exceed the given level of concurrent chunk downloads
		go func(chunkNumber uint64) {
			startByte := uint64(chunkNumber*ChunkSize)
			data, err := dl.DownloadChunk(ctx, srcUrl, startByte, ChunkSize)
			if err != nil {
				errChan<- err
				return
			}
			// stream chunk for upload
			dataChan  <- transData{data, startByte, ChunkSize}
			// increment chunks received count
			chunkChan <- struct{}{}
			// unlock the semaphore
			<-semaphore
		}(i)
	}

	var chunksReceived uint64
	for ; chunksReceived != totalChunksCount ; {
		select {
		case err := <-errChan:
			return err
		case <-dataChan:
			chunksReceived++
		}
	}

	return nil
}

func getChunkCount(totalSize uint64, chunkSize uint64) uint64 {
	chunkCount := totalSize / chunkSize
	if totalSize % chunkSize > 0 {
		chunkCount += 1
	}
	return chunkCount
}
