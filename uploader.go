package main

import (
	"fmt"
	"log"
)

type Uploader interface {
	Upload() error
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

func (s *S3Uploader) Upload() error {
	log.Println("s3 upload done")
	return nil
}
