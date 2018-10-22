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

// FileSaver is mostly for testing purposes to store locally
// whats downloaded by different schemas to verify
type FileSaver struct {
}

func getUploaderForScheme(scheme string) (dl Uploader, err error) {
	switch scheme {
	case "file":
		return &FileSaver{}, nil
	case "s3":
		return &S3Uploader{}, nil
	default:
		return nil, fmt.Errorf("upload scheme %v is not supported", scheme)
	}
}

func (s *S3Uploader) Upload(ctx context.Context, uri string, data chan dlData) {
	log.Println("s3 upload done")
}

func (f *FileSaver) Upload(ctx context.Context, uri string, data chan dlData) {

	//tempDir, err := ioutil.TempDir(os.TempDir(), "")
	//if err != nil {
	//	data <- dlData{err:err}
	//}
	//
	//tempFilePath := filepath.Join(tempDir, "file.txt")

	log.Println("s3 upload done")
}
