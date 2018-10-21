package main

import (
	"fmt"
	"log"
)

type uploader interface {
	upload() error
}

type s3Uploader struct {
}

func getUploaderForScheme(scheme string) (dl uploader, err error) {
	switch scheme {
	case "s3":
		return &s3Uploader{}, nil
	default:
		return nil, fmt.Errorf("upload scheme %v is not supported", scheme)
	}
}

func (s *s3Uploader) upload() error {
	log.Println("s3 upload done")
	return nil
}
