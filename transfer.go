package main

import (
	"context"
	"fmt"
	"net/url"
)

func transfer(ctx context.Context, srcUrl string, dstUrl string) error {

	var src *url.URL
	var dst *url.URL
	var err error

	if src, err = url.Parse(srcUrl); err != nil {
		return fmt.Errorf("invalid srcUrl %v", err)
	}

	if dst, err = url.Parse(dstUrl); err != nil {
		return fmt.Errorf("invalid dstUrl %v", err)
	}

	dnl, err := getDownloaderForScheme(src.Scheme)
	if err != nil {
		return fmt.Errorf("can't initialize downloader: %v", err)
	}

	upl, err := getUploaderForScheme(dst.Scheme)
	if err != nil {
		return fmt.Errorf("can't initialize uploader: %v", err)
	}

	dnl.download()

	upl.upload()

	return nil
}
