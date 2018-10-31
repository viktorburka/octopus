package netio

import "testing"

func TestKnownUploaderScheme(t *testing.T) {

	// can't use table method here because of the types

	var iface Uploader
	var ok    bool

	iface, _ = getUploader("s3")
	if iface == nil {
		t.Errorf("expected s3 uploader instance but received nil")
		return
	}
	_, ok = iface.(UploaderConcurrent)
	if !ok {
		t.Errorf("expected type UploaderS3")
		return
	}

	iface, _ = getUploader("file")
	if iface == nil {
		t.Errorf("expected local file uploader instance but received nil")
		return
	}
	_, ok = iface.(UploaderSimple)
	if !ok {
		t.Errorf("expected type UploaderSimple")
		return
	}
}

func TestUnknownUploaderScheme(t *testing.T) {

	var iface Uploader

	iface, _ = getUploader("unknown")
	if iface != nil {
		t.Errorf("expected nil value")
		return
	}
}

func TestKnownSenderScheme(t *testing.T) {

	// can't use table method here because of the types

	var iface sender
	var ok    bool

	iface, _ = getSender("s3", 0)
	if iface == nil {
		t.Errorf("expected s3 sender instance but received nil")
		return
	}
	_, ok = iface.(*S3SenderSimple)
	if !ok {
		t.Errorf("expected type S3SenderSimple")
		return
	}

	iface, _ = getSender("s3", MinAwsPartSize)
	if iface == nil {
		t.Errorf("expected s3 sender instance but received nil")
		return
	}
	_, ok = iface.(*S3SenderMultipart)
	if !ok {
		t.Errorf("expected type S3SenderMultipart")
		return
	}

	iface, _ = getSender("file", 0)
	if iface == nil {
		t.Errorf("expected local file sender instance but received nil")
		return
	}
	_, ok = iface.(*LocalFileSender)
	if !ok {
		t.Errorf("expected type S3SenderSimple")
		return
	}
}

func TestUnknownSenderScheme(t *testing.T) {

	var iface sender

	iface, _ = getSender("unknown", 0)
	if iface != nil {
		t.Errorf("expected nil value")
		return
	}
}
