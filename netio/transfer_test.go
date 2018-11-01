package netio

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func TestUploadSendsError(t *testing.T) {

	uploader, err := getUploader("s3")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	opt := map[string]string{}
	uri := "s3://amazon.aws.com/bucket/key.mp4"
	dtx := make(chan dlData)
	msg := make(chan dlMessage)
	sdr := &mockSender{}

	// set to not being able to start sending
	openError := fmt.Errorf("open error")
	sdr.openError = openError

	// expected error
	var uploadError error

	// to properly get the error from chan and unblock Upload()
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		message := <-msg
		uploadError = message.err
	}()

	wg.Add(1)
	go upload(&wg, uploader, ctx, uri, opt, dtx, msg, sdr)

	wg.Wait()

	if uploadError.Error() != openError.Error() {
		t.Fatalf("expected upload() to send '%v' error but got '%v'\n", openError, uploadError)
	}
}

func TestDownloadSendsError(t *testing.T) {

	downloader, err := getDownloader("s3")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	opt := map[string]string{"contentLength": "1"}
	uri := "s3://amazon.aws.com/bucket/key.mp4"
	dtx := make(chan dlData)
	msg := make(chan dlMessage)
	rcv := &mockReceiverRanged{}

	// set to not being able to start sending
	openError := fmt.Errorf("open error")
	rcv.openError = openError

	// expected error
	var downloadError error

	// to properly get the error from chan and unblock Upload()
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		message := <-msg
		downloadError = message.err
	}()

	wg.Add(1)
	go download(&wg, downloader, ctx, uri, opt, dtx, msg, rcv)

	wg.Wait()

	if downloadError.Error() != openError.Error() {
		t.Fatalf("expected download() to send '%v' error but got '%v'\n", openError, downloadError)
	}
}

func TestTransferReturnsError(t *testing.T) {

	ctx := context.Background()
	src := "s3://amazon.aws.com/src/key.mp4"
	dst := "s3://amazon.aws.com/dst/key.mp4"
	opt := map[string]string{}
	err := Transfer(ctx, src, dst, opt)
	if err == nil {
		t.Fatalf("expected Transfer() to return an error but got nil")
	}
}

func TestTransferInvalidParameters(t *testing.T) {

	var src, dst string
	var err error

	ctx := context.Background()
	opt := map[string]string{}

	// invalid src
	src = "s3://amazon!,aws,com/src/key.mp4"
	dst = "s3://amazon.aws.com/dst/key.mp4"

	err = Transfer(ctx, src, dst, opt)
	if err == nil {
		t.Fatalf("expected Transfer() to return an error but got nil")
	}

	// invalid dst
	src = "s3://amazon.aws.com/src/key.mp4"
	dst = "s3://amazon,!aws.com/dst/key.mp4"

	err = Transfer(ctx, src, dst, opt)
	if err == nil {
		t.Fatalf("expected Transfer() to return an error but got nil")
	}
}

func TestTransferUnsupportedScheme(t *testing.T) {

	var src, dst string
	var err error

	ctx := context.Background()
	opt := map[string]string{}

	// invalid src
	src = "unsupported://amazon.aws.com/src/key.mp4"
	dst = "unsupported://amazon.aws.com/dst/key.mp4"

	err = Transfer(ctx, src, dst, opt)
	if err == nil {
		t.Fatalf("expected Transfer() to return an error but got nil")
	}
}
