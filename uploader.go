package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path"
	"path/filepath"
)

type Uploader interface {
	Upload(ctx context.Context, uri string, data chan dlData, msg chan dlMessage)
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

func (s *S3Uploader) Upload(ctx context.Context, uri string, data chan dlData, msg chan dlMessage) {
	log.Println("s3 upload done")
}

func (f *FileSaver) Upload(ctx context.Context, uri string, data chan dlData, msg chan dlMessage) {

	log.Println("Prepare upload. Constructing temp folder...")

	tempDir, err := ioutil.TempDir(os.TempDir(), "")
	if err != nil {
		msg<-dlMessage{sender:"uploader", err:err}
		return
	}

	log.Println("temp dir:", tempDir)

	uploadUrl, err := url.Parse(uri)
	if err != nil {
		msg<-dlMessage{sender:"uploader", err:err}
		return
	}

	fileName := path.Base(uploadUrl.Path)
	tempFilePath := filepath.Join(tempDir, fileName)

	file, err := os.Create(tempFilePath)
	if err != nil {
		msg<-dlMessage{sender:"uploader", err:err}
		return
	}
	defer file.Close()

	var totalBytes uint64

	log.Println("Start upload")
	for {
		select {
		case chunk, ok := <-data:
			if !ok { // channel closed
				log.Println("Upload finished. Total size:", totalBytes)
				msg<-dlMessage{sender:"uploader", err:nil}
				return
			}
			log.Printf("Uploading %v bytes...\n", len(chunk.data))
			bw, err := file.Write(chunk.data)
			if err != nil {
				msg<-dlMessage{sender:"uploader", err:err}
				return
			}
			if bw != len(chunk.data) {
				incompleteOp := fmt.Errorf("file write operation error. expected to write %v bytes but got %v", len(chunk.data), bw)
				msg<-dlMessage{sender:"uploader", err:incompleteOp}
				return
			}
			totalBytes += uint64(bw)
		case <-ctx.Done(): // there is cancellation
			msg<-dlMessage{sender:"uploader", err:ctx.Err()}
			return
		}
	}
}