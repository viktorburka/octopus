package netio

import (
	"context"
	"testing"
)

func TestS3ReceiverOpen(t *testing.T) {

	rcv := &S3ReceiverRanged{}
	ctx := context.Background()
	opt := map[string]string{}
	uri := "http://amazon.aws.com/bucket/key.mp4"

	if err := rcv.OpenWithContext(ctx, uri, opt); err != nil {
		t.Fatalf("expected connection to open but received error: %v", err)
	}

	if !rcv.IsOpen() {
		t.Fatalf("expected connection to open")
	}
}

func TestS3ReceiverGetFileInfo(t *testing.T) {

	rcv := &S3ReceiverRanged{}
	ctx := context.Background()
	opt := map[string]string{}
	uri := "http://amazon.aws.com/bucket/key.mp4"

	_, err := rcv.GetFileInfo(ctx, uri, opt)
	if err == nil {
		t.Fatalf("expected GetFileInfo() to return error due to lack of aws credentials")
	}
}

func TestS3ReceiverReadPartWithContext(t *testing.T) {

	rcv := &S3ReceiverRanged{}
	ctx := context.Background()
	opt := map[string]string{}
	wrt := &mockWriteSeeker{}
	uri := "http://amazon.aws.com/bucket/key.mp4"

	if err := rcv.OpenWithContext(ctx, uri, opt); err != nil {
		t.Fatalf("expected connection to open but received error: %v", err)
	}

	if !rcv.IsOpen() {
		t.Fatalf("expected connection to open")
	}

	_, err := rcv.ReadPartWithContext(ctx, wrt, opt)
	if err == nil {
		t.Fatalf("expected ReadPartWithContext() to return an error due to invalid url")
	}
}

func TestS3ReceiverCancelWithContext(t *testing.T) {

	rcv := &S3ReceiverRanged{}
	ctx := context.Background()

	if err := rcv.CancelWithContext(ctx); err == nil {
		t.Fatalf("expected CancelWithContext() to return an error due to non implemened method")
	}
}

func TestS3ReceiverCloseWithContext(t *testing.T) {

	rcv := &S3ReceiverRanged{}
	ctx := context.Background()

	if err := rcv.CloseWithContext(ctx); err == nil {
		t.Fatalf("expected CloseWithContext() to return an error due to non implemened method")
	}
}
