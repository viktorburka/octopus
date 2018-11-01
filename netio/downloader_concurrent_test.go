package netio

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"testing"
)

func TestConcurrentDownloadConnectionInitError(t *testing.T) {

	downloader, err := getDownloader("s3")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	opt := map[string]string{"contentLength": "1"}
	uri := "s3://amazon.aws.com/bucket/key.mp4"
	dtx := make(chan dlData)
	sdr := &mockReceiverRanged{}

	// set to not being able to start sending
	sdr.openError = fmt.Errorf("open error")

	downloadError := downloader.Download(ctx, uri, opt, dtx, sdr)

	if downloadError != sdr.openError {
		t.Fatalf("expected Download() to return '%v' error but got '%v'\n",
			sdr.openError, downloadError)
	}
}

func TestConcurrentDownloaderHappyPath(t *testing.T)  {

	const DownloadSize = 1000

	downloader, err := getDownloader("s3")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	opt := map[string]string{"contentLength": strconv.FormatInt(DownloadSize, 10)}
	uri := "s3://amazon.aws.com/bucket/key.mp4"
	dtx := make(chan dlData)
	rcv := &mockReceiverRanged{}

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

type mockReceiverRanged struct {
	openError error
	isOpen bool
}

func (r *mockReceiverRanged) GetFileInfo(ctx context.Context, uri string,
	options map[string]string) (info FileInfo, err error) {

	return FileInfo{}, nil
}

func (r *mockReceiverRanged) OpenWithContext(ctx context.Context, uri string, opt map[string]string) error {
	return r.openError
}

func (r *mockReceiverRanged) IsOpen() bool {
	return r.isOpen
}

func (r *mockReceiverRanged) ReadPartWithContext(ctx context.Context,
	output io.WriteSeeker, opt map[string]string) (string, error) {

	partSize, err := strconv.ParseInt(opt["partSize"], 10, 64)
	if err != nil {
		return "", fmt.Errorf("can't parse 'partSize': %v", err)
	}

	buf := make([]byte, partSize)
	for i:=0; i<len(buf); i++ {
		buf[i] = 0xEF
	}

	_, err = output.Write(buf)
	if err != nil {
		return "", err
	}

	return "", nil
}

func (r *mockReceiverRanged) CancelWithContext(ctx context.Context) error {
	return nil
}

func (r *mockReceiverRanged) CloseWithContext(ctx context.Context) error {
	return nil
}
