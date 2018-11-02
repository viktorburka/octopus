package netio

import (
	"context"
	"testing"
)

func TestS3SimpleSenderOpen(t *testing.T) {

	snd := &S3SenderSimple{}
	ctx := context.Background()
	opt := map[string]string{}
	uri := "http://amazon.aws.com/bucket/key.mp4"

	if err := snd.OpenWithContext(ctx, uri, opt); err != nil {
		t.Fatalf("expected connection to open but received error: %v", err)
	}

	if !snd.IsOpen() {
		t.Fatalf("expected connection to open")
	}
}

func TestS3SimpleSenderWritePartWithContext(t *testing.T) {

	snd := &S3SenderSimple{}
	ctx := context.Background()
	opt := map[string]string{}
	rdr := &mockReadSeeker{}
	uri := "http://amazon.aws.com/bucket/key.mp4"

	if err := snd.OpenWithContext(ctx, uri, opt); err != nil {
		t.Fatalf("expected connection to open but received error: %v", err)
	}

	if !snd.IsOpen() {
		t.Fatalf("expected connection to open")
	}

	_, err := snd.WritePartWithContext(ctx, rdr, opt)
	if err == nil {
		t.Fatalf("expected ReadPartWithContext() to return an error due to connection not opened")
	}
}

func TestS3SimpleSenderCloseWithContext(t *testing.T) {

	snd := &S3SenderSimple{}
	ctx := context.Background()
	opt := map[string]string{}
	uri := "http://amazon.aws.com/bucket/key.mp4"

	if err := snd.OpenWithContext(ctx, uri, opt); err != nil {
		t.Fatalf("expected connection to open but received error: %v", err)
	}

	if !snd.IsOpen() {
		t.Fatalf("expected connection to open")
	}

	if err := snd.CloseWithContext(ctx); err != nil {
		t.Fatalf("expected connection close without an error")
	}

	if snd.IsOpen() {
		t.Fatalf("expected connection to close after calling CloseWithContext()")
	}
}

func TestS3SimpleSenderCancelWithContext(t *testing.T) {

	snd := &S3SenderSimple{}
	ctx := context.Background()
	opt := map[string]string{}
	uri := "http://amazon.aws.com/bucket/key.mp4"

	if err := snd.OpenWithContext(ctx, uri, opt); err != nil {
		t.Fatalf("expected connection to open but received error: %v", err)
	}

	if !snd.IsOpen() {
		t.Fatalf("expected connection to open")
	}

	if err := snd.CancelWithContext(ctx); err != nil {
		t.Fatalf("expected connection close without an error")
	}

	if snd.IsOpen() {
		t.Fatalf("expected connection to close after calling CancelWithContext()")
	}
}
