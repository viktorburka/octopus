package netio

import (
	"context"
	"fmt"
	"testing"
)

func TestHttpReceiverOpen(t *testing.T) {

	rcv := &HttpReceiver{}
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

func TestHttpReceiverGetFileInfo(t *testing.T) {

	rcv := &HttpReceiver{}
	ctx := context.Background()
	opt := map[string]string{}
	uri := "http://a.m.a.z.o.n.aws.com/bucket/key.mp4"

	_, err := rcv.GetFileInfo(ctx, uri, opt)
	if err == nil {
		t.Fatalf("expected GetFileInfo() to return an error due to non existing URL")
	}
}

func TestHttpReceiverReadPartWithContext(t *testing.T) {

	rcv := &HttpReceiver{}
	ctx := context.Background()
	opt := map[string]string{}
	wrt := &mockWriteSeeker{}
	uri := "http://a.m.a.z.o.n.aws.com/bucket/key.mp4"

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

func TestHttpReceiverCancelWithContext(t *testing.T) {

	rcv := &HttpReceiver{}
	ctx := context.Background()

	if err := rcv.CancelWithContext(ctx); err == nil {
		t.Fatalf("expected CancelWithContext() to return an error due to non implemened method")
	}
}

func TestHttpReceiverCloseWithContext(t *testing.T) {

	rcv := &HttpReceiver{}
	ctx := context.Background()

	if err := rcv.CloseWithContext(ctx); err == nil {
		t.Fatalf("expected CloseWithContext() to return an error due to non implemened method")
	}
}

type mockWriteSeeker struct {
}

func (ws *mockWriteSeeker) Write(p []byte) (n int, err error) {
	return -1, fmt.Errorf("not implemented")
}

func (ws *mockWriteSeeker) Seek(offset int64, whence int) (int64, error) {
	return -1, fmt.Errorf("not implemented")
}
