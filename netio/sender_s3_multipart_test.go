package netio

import (
	"context"
	"fmt"
	"testing"
)

func TestS3MultipartSenderOpen(t *testing.T) {

	snd := &S3SenderMultipart{}
	ctx := context.Background()
	opt := map[string]string{}
	uri := "http://amazon.aws.com/bucket/key.mp4"

	if err := snd.OpenWithContext(ctx, uri, opt); err == nil {
		t.Fatalf("expected connection not to open due to lack f aws credentials")
	}

	if snd.IsOpen() {
		t.Fatalf("expected connection not to open")
	}
}

func TestS3MultipartSenderWritePartWithContext(t *testing.T) {

	snd := &S3SenderMultipart{}
	ctx := context.Background()
	opt := map[string]string{}
	rdr := &mockReadSeeker{}
	uri := "http://amazon.aws.com/bucket/key.mp4"

	if err := snd.OpenWithContext(ctx, uri, opt); err == nil {
		t.Fatalf("expected connection not to open due to lack f aws credentials")
	}

	if snd.IsOpen() {
		t.Fatalf("expected connection not to open")
	}

	_, err := snd.WritePartWithContext(ctx, rdr, opt)
	if err == nil {
		t.Fatalf("expected ReadPartWithContext() to return an error due to connection not opened")
	}
}


type mockReadSeeker struct {
}

func (ws *mockReadSeeker) Read(p []byte) (n int, err error) {
	return -1, fmt.Errorf("not implemented")
}

func (ws *mockReadSeeker) Seek(offset int64, whence int) (int64, error) {
	return -1, fmt.Errorf("not implemented")
}
