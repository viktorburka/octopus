package netio

import (
	"context"
	"fmt"
	"io"
	"log"
	"testing"
)

func TestConcurrentUploaderConnectionInitError(t *testing.T) {

	uploader, err := getUploader("s3")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	opt := map[string]string{}
	uri := "s3://amazon.aws.com/bucket/key.mp4"
	dtx := make(chan dlData)
	sdr := &mockSender{}

	// set to not being able to start sending
	sdr.openError = fmt.Errorf("open error")

	uploadError := uploader.Upload(ctx, uri, opt, dtx, sdr)

	if uploadError != sdr.openError {
		t.Fatalf("expected Upload() to return '%v' error but got '%v'\n",
			sdr.openError, uploadError)
	}
}

func TestConcurrentUploaderHappyPath(t *testing.T) {

	uploader, err := getUploader("s3")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	opt := map[string]string{}
	uri := "s3://amazon.aws.com/bucket/key.mp4"
	dtx := make(chan dlData)
	sdr := &mockSender{}

	// data provider goroutine
	go func() {
		buf := make([]byte, 256*1024) // 256KB file
		for i:=0; i<len(buf); i++ {
			buf[i] = 0xEF
		}
		dtx <- dlData{buf,true,int64(len(buf)),int64(len(buf))}
		close(dtx)
	}()

	uploadError := uploader.Upload(ctx, uri, opt, dtx, sdr)

	if uploadError != nil {
		t.Fatalf("expected Upload() to return '%v' error but got '%v'\n", nil, uploadError)
	}

	if len(sdr.buf) != 256*1024 {
		t.Fatalf("expected Upload() to write '%v' bytes but got '%v'\n", 256*1024, len(sdr.buf))
	}

	// check if content given matched content sent
	match := true
	for _, val := range sdr.buf {
		if val != 0xEF {
			match = false
			break
		}
	}
	if !match {
		t.Fatalf("invalid content")
	}
}

type mockSender struct {
	openError error
	isOpen bool
	buf []byte
}

func (s *mockSender) OpenWithContext(ctx context.Context, uri string, opt map[string]string) error {
	return s.openError
}

func (s *mockSender) IsOpen() bool {
	return s.isOpen
}

func (s *mockSender) WritePartWithContext(ctx context.Context, input io.ReadSeeker,
	opt map[string]string) (string, error) {

	size, err := input.Seek(0, io.SeekEnd)
	if err != nil {
		return "", err
	}
	_, err = input.Seek(0, io.SeekStart)
	if err != nil {
		return "", err
	}

	buf := make([]byte, size)

	br, err := input.Read(buf)
	log.Println("br:", br)
	if err != nil {
		return "", err
	}
	if int64(br) != size {
		return "", fmt.Errorf("incomplete write. expected %v but got %v", size, br)
	}

	s.buf = buf

	return "", nil
}

func (s *mockSender) CancelWithContext(ctx context.Context) error {
	return nil
}

func (s *mockSender) CloseWithContext(ctx context.Context) error {
	return nil
}
