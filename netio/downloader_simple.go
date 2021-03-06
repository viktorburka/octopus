package netio

import (
	"context"
	"fmt"
	"strconv"
)

type DownloaderSimple struct {
}

func (h DownloaderSimple) Download(ctx context.Context, uri string, options map[string]string,
	data chan dlData, rc receiver) error {

	if err := rc.OpenWithContext(ctx, uri, options); err != nil {
		return err
	}

	contentLength, err := strconv.ParseInt(options["contentLength"],10,64)
	if err != nil {
		return fmt.Errorf("error reading 'contentLength' value: %v", err)
	}

	writer := newChanWriter(contentLength, data)

	_, err = rc.ReadPartWithContext(ctx, writer, options)
	if err != nil {
		return err
	}

	close(data)

	return nil
}


type chanWriter struct {
	data chan dlData
	total int64
	totalBytesRead int64
}

func newChanWriter(contentLength int64, data chan dlData) *chanWriter {
	return &chanWriter{data: data, total:contentLength}
}

func (w *chanWriter) Write(p []byte) (n int, err error) {
	br := len(p)
	w.totalBytesRead += int64(br)
	w.data <- dlData{data:p, br:w.totalBytesRead, total:w.total}
	return br,nil
}

func (w *chanWriter) Seek(offset int64, whence int) (int64, error) {
	return -1, fmt.Errorf("not implemented")
}
