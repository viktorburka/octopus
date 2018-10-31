package netio

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"sync"
)

type DownloaderHttp struct {
}

type HttpReceiverSimple struct {
	m sync.Mutex
	uri string
	client *http.Client
}

type chanWriter struct {
	data chan dlData
	total int64
	totalBytesRead int64
}

func (h DownloaderHttp) Download(ctx context.Context, uri string, options map[string]string,
	data chan dlData, msg chan dlMessage, rc receiver) {

	if err := rc.OpenWithContext(ctx, uri, options); err != nil {
		msg<-dlMessage{sender:"downloader", err: err}
		return
	}

	contentLength, err := strconv.ParseInt(options["contentLength"],10,64)
	if err != nil {
		msg<-dlMessage{sender:"downloader", err: err}
		return
	}

	writer := newChanWriter(contentLength, data)

	_, err = rc.ReadPartWithContext(ctx, writer, options)
	if err != nil {
		msg<-dlMessage{sender:"downloader", err: err}
		return
	}

	close(data)

	msg<-dlMessage{sender:"downloader", err: nil}
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
