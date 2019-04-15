package main

import (
	"context"
	"sync"
)

type transData struct {
	Data []byte
	Start uint64
	Size uint64
}

func transfer(src, dst string, cfg dlConfig) error {
	// operation error to be returned from the function
	var opErr protectedError
	// streamingChan will conduct streaming download chunks
	streamingChan := make(chan transData)

	// this is needed to cancel another operation when of them fails
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	// start concurrent streaming download
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(streamingChan)
		err := streamingDownload(ctx, src, cfg, streamingChan)
		if err != nil {
			opErr.SetError(err)
			cancel()
			return
		}
	}()
	// start concurrent streaming upload
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := streamingUpload(ctx, dst, streamingChan)
		if err != nil {
			opErr.SetError(err)
			cancel()
			return
		}
	}()

	wg.Wait()

	return opErr.GetError()
}
