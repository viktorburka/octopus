package main

import (
	"context"
	"fmt"
)

type Uploader interface {
	Upload(ctx context.Context, uri string, options map[string]string, data chan dlData, msg chan dlMessage)
}

func getUploaderForScheme(scheme string) (dl Uploader, err error) {
	switch scheme {
	case "file":
		return &FileSaver{}, nil
	case "s3":
		return &S3UploaderMultipart{}, nil
	default:
		return nil, fmt.Errorf("upload scheme %v is not supported", scheme)
	}
}
