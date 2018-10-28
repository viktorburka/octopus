package netio

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
)

type HttpDownloader struct {
}

func (h *HttpDownloader) Download(ctx context.Context, uri string, options map[string]string, data chan dlData, msg chan dlMessage) {
	// start download
	client := &http.Client{} //TODO: might also instantiate it once
	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		msg<-dlMessage{sender:"downloader", err: err}
		return
	}
	resp, err := client.Do(req.WithContext(ctx))
	if err != nil {
		msg<-dlMessage{sender:"downloader", err: err}
		return
	}
	defer resp.Body.Close()

	if resp.ContentLength == -1 { // ContentLength unknown
		err := fmt.Errorf("can't start download: ContentLength unknown")
		msg<-dlMessage{sender:"downloader", err: err}
		return
	}

	var totalBytesRead int64

	// read data
	reader := bufio.NewReader(resp.Body)
	buffer := make([]byte, 3*1024*1024) // 3MB
	for {
		log.Println("downloader: reading data...")
		br, err := reader.Read(buffer)
		if err != nil && err != io.EOF { // its an error (io.EOF is fine)
			msg<-dlMessage{sender:"downloader", err: err}
			return
		}
		totalBytesRead += int64(br)
		log.Printf("downloader: received %v bytes\n", totalBytesRead)
		data<-dlData{data:buffer[:br], br:totalBytesRead, total:resp.ContentLength}
		if err == io.EOF { // done reading
			close(data)
			break
		}
	}

	log.Println("download finished. total size:", totalBytesRead)
	msg<-dlMessage{sender:"downloader", err: nil}
}
