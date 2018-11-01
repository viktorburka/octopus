package netio

import (
	"context"
	"io"
	"strconv"
	"testing"
)

const DownloadSize = 1000

func TestSimpleDownloaderHappyPath(t *testing.T)  {

	downloader, err := getDownloader("http")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	opt := map[string]string{"contentLength": strconv.FormatInt(DownloadSize, 10)}
	uri := "s3://amazon.aws.com/bucket/key.mp4"
	dtx := make(chan dlData)
	rcv := &mockReceiverSimple{}

	// data provider goroutine
	var bytesReceived int64
	var downloadBytes = make([]byte, DownloadSize)
	go func() {
		for data := range dtx {
			br := copy(downloadBytes[bytesReceived:], data.data)
			bytesReceived += int64(br)
			if data.done {
				break
			}
		}
	}()

	downloadError := downloader.Download(ctx, uri, opt, dtx, rcv)

	if downloadError != nil {
		t.Fatalf("expected Download() to return '%v' error but got '%v'\n", nil, downloadError)
	}

	if bytesReceived != DownloadSize {
		t.Fatalf("expected Download() to read '%v' bytes but got '%v'\n", DownloadSize, bytesReceived)
	}

	// check if content given matched content sent
	match := true
	for _, val := range downloadBytes {
		if val != 0xEF {
			match = false
			break
		}
	}
	if !match {
		t.Fatalf("invalid content")
	}
}

type mockReceiverSimple struct {
	openError error
	isOpen bool
}

func (r *mockReceiverSimple) GetFileInfo(ctx context.Context, uri string,
	options map[string]string) (info FileInfo, err error) {

	return FileInfo{}, nil
}

func (r *mockReceiverSimple) OpenWithContext(ctx context.Context, uri string, opt map[string]string) error {
	return r.openError
}

func (r *mockReceiverSimple) IsOpen() bool {
	return r.isOpen
}

func (r *mockReceiverSimple) ReadPartWithContext(ctx context.Context,
	output io.WriteSeeker, opt map[string]string) (string, error) {

	buf := make([]byte, DownloadSize)
	for i:=0; i<len(buf); i++ {
		buf[i] = 0xEF
	}

	_, err := output.Write(buf)
	if err != nil {
		return "", err
	}

	return "", nil
}

func (r *mockReceiverSimple) CancelWithContext(ctx context.Context) error {
	return nil
}

func (r *mockReceiverSimple) CloseWithContext(ctx context.Context) error {
	return nil
}
