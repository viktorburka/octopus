package netio

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
)

const DownloadSize = 1000

func TestSimpleDownloadConnectionInitError(t *testing.T) {

	downloader, err := getDownloader("http")
	if err != nil {
		t.Fatal(err)
	}
	_, ok := downloader.(DownloaderSimple)
	if !ok {
		t.Fatal(fmt.Errorf("error: expected DownloaderSimple instance"))
	}

	ctx := context.Background()
	opt := map[string]string{"contentLength": "1"}
	uri := "s3://amazon.aws.com/bucket/key.mp4"
	dtx := make(chan dlData)
	sdr := &mockReceiverSimple{}

	// set to not being able to start sending
	sdr.openError = fmt.Errorf("open error")

	downloadError := downloader.Download(ctx, uri, opt, dtx, sdr)

	if downloadError.Error() != sdr.openError.Error() {
		t.Fatalf("expected Download() to return '%v' error but got '%v'\n",
			sdr.openError, downloadError)
	}
}

func TestSimpleDownloaderHappyPath(t *testing.T)  {

	downloader, err := getDownloader("http")
	if err != nil {
		t.Fatal(err)
	}
	_, ok := downloader.(DownloaderSimple)
	if !ok {
		t.Fatal(fmt.Errorf("error: expected DownloaderSimple instance"))
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

func TestSimpleDownloaderInvalidContentLength(t *testing.T)  {

	downloader, err := getDownloader("http")
	if err != nil {
		t.Fatal(err)
	}
	_, ok := downloader.(DownloaderSimple)
	if !ok {
		t.Fatal(fmt.Errorf("error: expected DownloaderSimple instance"))
	}

	ctx := context.Background()
	opt := map[string]string{}
	uri := "s3://amazon.aws.com/bucket/key.mp4"
	dtx := make(chan dlData)
	sdr := &mockReceiverSimple{}

	downloadError := downloader.Download(ctx, uri, opt, dtx, sdr)

	if downloadError == nil {
		t.Fatalf("expected Download() to return invalid contentLength error but got nil\n")
	}
	if !strings.Contains(downloadError.Error(), "contentLength") {
		t.Fatalf("expected Download() to return invalid contentLength error but got '%v'\n",
			downloadError)
	}
}

func TestSimpleDownloaderReadPartError(t *testing.T) {

	downloader, err := getDownloader("http")
	if err != nil {
		t.Fatal(err)
	}
	_, ok := downloader.(DownloaderSimple)
	if !ok {
		t.Fatal("error: expected DownloaderSimple instance")
	}

	ctx := context.Background()
	opt := map[string]string{"contentLength": "100"}
	uri := "s3://amazon.aws.com/bucket/key.mp4"
	dtx := make(chan dlData)
	sdr := &mockReceiverSimple{}

	sdr.readPartError = fmt.Errorf("read part error")

	downloadError := downloader.Download(ctx, uri, opt, dtx, sdr)

	if downloadError.Error() != sdr.readPartError.Error() {
		t.Fatalf("expected Download() to return '%v' error but got '%v'\n",
			sdr.readPartError, downloadError)
	}
}

func TestSimpleDownloaderChanWriterSeek(t *testing.T) {
	cw := &chanWriter{}
	_, err := cw.Seek(0, io.SeekStart)
	if err == nil {
		t.Fatal("error: expected Seek to return error but got nil")
	}
}

type mockReceiverSimple struct {
	openError error
	readPartError error
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

	if r.readPartError != nil {
		return "", r.readPartError
	}

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
