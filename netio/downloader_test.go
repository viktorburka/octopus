package netio

import (
	"testing"
)

func TestKnownDownloaderScheme(t *testing.T) {

	// can't use table method here because of the types

	var iface Downloader
	var ok    bool

	iface, _ = getDownloader("http")
	if iface == nil {
		t.Errorf("expected http downloader instance but received nil")
		return
	}
	_, ok = iface.(DownloaderSimple)
	if !ok {
		t.Errorf("expected type DownloaderSimple")
		return
	}

	iface, _ = getDownloader("https")
	if iface == nil {
		t.Errorf("expected https downloader instance but received nil")
		return
	}
	_, ok = iface.(DownloaderSimple)
	if !ok {
		t.Errorf("expected type DownloaderSimple")
		return
	}

	iface, _ = getDownloader("s3")
	if iface == nil {
		t.Errorf("expected s3 downloader instance but received nil")
		return
	}
	_, ok = iface.(DownloaderConcurrent)
	if !ok {
		t.Errorf("expected type DownloaderConcurrent")
		return
	}
}

func TestUnknownDownloaderScheme(t *testing.T) {

	var iface Downloader

	iface, _ = getDownloader("unknown")
	if iface != nil {
		t.Errorf("expected nil value")
		return
	}
}

func TestKnownReceiverScheme(t *testing.T) {

	// can't use table method here because of the types

	var iface receiver
	var ok    bool

	iface, _ = getReceiver("http", 0)
	if iface == nil {
		t.Errorf("expected http downloader instance but received nil")
		return
	}
	_, ok = iface.(*HttpReceiver)
	if !ok {
		t.Errorf("expected type DownloaderSimple")
		return
	}

	iface, _ = getReceiver("https", 0)
	if iface == nil {
		t.Errorf("expected https downloader instance but received nil")
		return
	}
	_, ok = iface.(*HttpReceiver)
	if !ok {
		t.Errorf("expected type DownloaderSimple")
		return
	}

	iface, _ = getReceiver("s3", 0)
	if iface == nil {
		t.Errorf("expected s3 downloader instance but received nil")
		return
	}
	_, ok = iface.(*S3ReceiverRanged)
	if !ok {
		t.Errorf("expected type DownloaderConcurrent")
		return
	}
}

func TestUnknownReceiverScheme(t *testing.T) {

	var iface receiver

	iface, _ = getReceiver("unknown", 0)
	if iface != nil {
		t.Errorf("expected nil value")
		return
	}
}
