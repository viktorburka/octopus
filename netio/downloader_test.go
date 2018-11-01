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
		t.Fatal("expected http downloader instance but received nil")
	}
	_, ok = iface.(DownloaderSimple)
	if !ok {
		t.Fatal("expected type DownloaderSimple")
	}

	iface, _ = getDownloader("https")
	if iface == nil {
		t.Fatal("expected https downloader instance but received nil")
	}
	_, ok = iface.(DownloaderSimple)
	if !ok {
		t.Fatal("expected type DownloaderSimple")
	}

	iface, _ = getDownloader("s3")
	if iface == nil {
		t.Fatal("expected s3 downloader instance but received nil")
	}
	_, ok = iface.(DownloaderConcurrent)
	if !ok {
		t.Fatal("expected type DownloaderConcurrent")
	}
}

func TestUnknownDownloaderScheme(t *testing.T) {

	var iface Downloader

	iface, _ = getDownloader("unknown")
	if iface != nil {
		t.Fatal("expected nil value")
	}
}

func TestKnownReceiverScheme(t *testing.T) {

	// can't use table method here because of the types

	var iface receiver
	var ok    bool

	iface, _ = getReceiver("http", 0)
	if iface == nil {
		t.Fatal("expected http downloader instance but received nil")
	}
	_, ok = iface.(*HttpReceiver)
	if !ok {
		t.Fatal("expected type DownloaderSimple")
	}

	iface, _ = getReceiver("https", 0)
	if iface == nil {
		t.Fatal("expected https downloader instance but received nil")
	}
	_, ok = iface.(*HttpReceiver)
	if !ok {
		t.Fatal("expected type DownloaderSimple")
	}

	iface, _ = getReceiver("s3", 0)
	if iface == nil {
		t.Fatal("expected s3 downloader instance but received nil")
	}
	_, ok = iface.(*S3ReceiverRanged)
	if !ok {
		t.Fatal("expected type DownloaderConcurrent")
	}
}

func TestUnknownReceiverScheme(t *testing.T) {

	var iface receiver

	iface, _ = getReceiver("unknown", 0)
	if iface != nil {
		t.Fatal("expected nil value")
	}
}

func TestUnknownProbeScheme(t *testing.T) {

	var iface receiver

	iface, _ = getProbeForScheme("unknown")
	if iface != nil {
		t.Fatal("expected nil value")
	}
}
