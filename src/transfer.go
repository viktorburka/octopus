package main

import "context"

type transData struct {
	data []byte
	start uint64
	size uint64
}

func transfer(src, dst string) error {
	// streamingChan will conduct streaming download chunks
	streamingChan := make(chan transData)
	// completeChan indicates when one chunk download is complete
	completeChan  := make(chan struct{})
	// errorChan for errors on either of operations
	errorChan     := make(chan error)

	// this is needed to cancel another operation when of them fails
	ctx, cancel := context.WithCancel(context.Background())

	// start concurrent streaming download
	go func() {
		err := streamingDownload(ctx, src, streamingChan)
		if err != nil {
			errorChan<- err
			return
		}
		completeChan<- struct{}{}
	}()
	// start concurrent streaming upload
	go func() {
		err := streamingUpload(ctx, dst, streamingChan)
		if err != nil {
			errorChan<- err
			return
		}
		completeChan<- struct{}{}
	}()
	// sync - wait for both of streaming operations to complete
	var opCount int
	for ; opCount != 2 ; {
		select {
		case err := <-errorChan:
			cancel()
			return err
		case <-completeChan:
			opCount++
		}
	}
	return nil
}
