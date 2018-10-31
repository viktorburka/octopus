package netio

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
)

func TestOpenError(t *testing.T) {

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

	uploader.Upload(ctx, uri, opt, dtx, msg, sdr)

	wg.Wait()

	if uploadError != openError {
		t.Fatalf("expected Upload() to return '%v' error but got '%v'\n", openError, uploadError)
	}
}

type mockSender struct {
	openError error
	isOpen bool
}

func (s *mockSender) OpenWithContext(ctx context.Context, uri string, opt map[string]string) error {
	return s.openError
}

func (s *mockSender) IsOpen() bool {
	return s.isOpen
}

func (s *mockSender) WritePartWithContext(ctx context.Context, input io.ReadSeeker,
	opt map[string]string) (string, error) {

	return "", nil
}

func (s *mockSender) CancelWithContext(ctx context.Context) error {
	return nil
}

func (s *mockSender) CloseWithContext(ctx context.Context) error {
	return nil
}
