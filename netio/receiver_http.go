package netio

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
)

type HttpReceiver struct {
	m sync.Mutex
	uri string
	client *http.Client
}

func (r *HttpReceiver) OpenWithContext(ctx context.Context, uri string, opt map[string]string) error {
	r.m.Lock()
	defer r.m.Unlock()
	r.uri    = uri
	r.client = &http.Client{}
	return nil
}

func (r *HttpReceiver) IsOpen() bool {
	r.m.Lock()
	defer r.m.Unlock()
	return r.client != nil
}

func (r *HttpReceiver) ReadPartWithContext(ctx context.Context, output io.WriteSeeker,
	opt map[string]string) (string, error) {

	r.m.Lock()
	uri := r.uri
	r.m.Unlock()

	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return "", err
	}
	resp, err := r.client.Do(req.WithContext(ctx))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.ContentLength == -1 { // ContentLength unknown
		err := fmt.Errorf("can't start download: ContentLength unknown")
		return "", err
	}

	var totalBytesRead int64

	// read data
	reader := bufio.NewReader(resp.Body)
	buffer := make([]byte, 3*1024*1024) // 3MB
	for {
		log.Println("downloader: reading data...")
		br, err := reader.Read(buffer)
		if err != nil && err != io.EOF { // its an error (io.EOF is fine)
			return "", err
		}
		totalBytesRead += int64(br)
		log.Printf("downloader: received %v bytes\n", totalBytesRead)
		output.Write(buffer[:br])
		if err == io.EOF { // done reading
			//close(data)
			break
		}
	}

	return "", nil
}

func (r *HttpReceiver) CancelWithContext(ctx context.Context) error {
	return fmt.Errorf("not implemented")
}

func (r *HttpReceiver) CloseWithContext(ctx context.Context) error {
	return fmt.Errorf("not implemented")
}

func (r *HttpReceiver) GetFileInfo(ctx context.Context, uri string, options map[string]string) (FileInfo, error) {
	var info FileInfo
	client := &http.Client{} //TODO: might also instantiate it once
	req, err := http.NewRequest("HEAD", uri, nil)
	if err != nil {
		return info, err
	}
	resp, err := client.Do(req.WithContext(ctx))
	if err != nil {
		return info, err
	}
	info.Size = resp.ContentLength
	return info, nil
}
