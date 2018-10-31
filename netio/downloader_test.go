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
	_, ok = iface.(DownloaderHttp)
	if !ok {
		t.Errorf("expected type DownloaderHttp")
		return
	}

	iface, _ = getDownloader("https")
	if iface == nil {
		t.Errorf("expected https downloader instance but received nil")
		return
	}
	_, ok = iface.(DownloaderHttp)
	if !ok {
		t.Errorf("expected type DownloaderHttp")
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
