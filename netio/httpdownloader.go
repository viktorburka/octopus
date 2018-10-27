package netio

import (
	"bufio"
	"context"
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

	var totalBytes uint64

	// read data
	reader := bufio.NewReader(resp.Body)
	for {
		//TODO: frequent allocations - improve to reuse the buffer
		log.Println("downloader: reading data...")
		buf := make([]byte, 3*1024*1024) // 3MB
		br, err := reader.Read(buf)
		if err != nil && err != io.EOF { // its an error (io.EOF is fine)
			msg<-dlMessage{sender:"downloader", err: err}
			return
		}
		totalBytes += uint64(br)
		log.Printf("downloader: received %v bytes\n", totalBytes)
		data<-dlData{data:buf[:br], size:br}
		if err == io.EOF { // done reading
			close(data)
			break
		}
	}

	log.Println("download finished. total size:", totalBytes)
	msg<-dlMessage{sender:"downloader", err: nil}
}

