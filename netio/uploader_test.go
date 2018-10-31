package netio

import "testing"

func TestKnownUploaderScheme(t *testing.T) {

	// can't use table method here because of the types

	var iface Uploader
	var ok    bool

	iface, _ = getUploader("s3")
	if iface == nil {
		t.Fatal("expected s3 uploader instance but received nil")
	}
	_, ok = iface.(UploaderConcurrent)
	if !ok {
		t.Fatal("expected type UploaderS3")
	}

	iface, _ = getUploader("file")
	if iface == nil {
		t.Fatal("expected local file uploader instance but received nil")
	}
	_, ok = iface.(UploaderSimple)
	if !ok {
		t.Fatal("expected type UploaderSimple")
	}
}

func TestUnknownUploaderScheme(t *testing.T) {

	var iface Uploader

	iface, _ = getUploader("unknown")
	if iface != nil {
		t.Fatal("expected nil value")
	}
}

func TestKnownSenderScheme(t *testing.T) {

	// can't use table method here because of the types

	var iface sender
	var ok    bool

	iface, _ = getSender("s3", 0)
	if iface == nil {
		t.Fatal("expected s3 sender instance but received nil")
	}
	_, ok = iface.(*S3SenderSimple)
	if !ok {
		t.Fatal("expected type S3SenderSimple")
	}

	iface, _ = getSender("s3", MinAwsPartSize)
	if iface == nil {
		t.Fatal("expected s3 sender instance but received nil")
	}
	_, ok = iface.(*S3SenderMultipart)
	if !ok {
		t.Fatal("expected type S3SenderMultipart")
	}

	iface, _ = getSender("file", 0)
	if iface == nil {
		t.Fatal("expected local file sender instance but received nil")
	}
	_, ok = iface.(*LocalFileSender)
	if !ok {
		t.Fatal("expected type S3SenderSimple")
	}
}

func TestUnknownSenderScheme(t *testing.T) {

	var iface sender

	iface, _ = getSender("unknown", 0)
	if iface != nil {
		t.Fatal("expected nil value")
	}
}
