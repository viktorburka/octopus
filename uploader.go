package main

import (
	"context"
	"fmt"
	"log"
)

type Uploader interface {
	Upload(ctx context.Context, uri string, data chan dlData)
}

type S3Uploader struct {
}

func getUploaderForScheme(scheme string) (dl Uploader, err error) {
	switch scheme {
	case "s3":
		return &S3Uploader{}, nil
	default:
		return nil, fmt.Errorf("upload scheme %v is not supported", scheme)
	}
}

func (s *S3Uploader) Upload(ctx context.Context, uri string, data chan dlData) {
	log.Println("s3 upload done")
}
