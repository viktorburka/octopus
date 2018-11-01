package netio

import (
	"context"
	"fmt"
	"io"
)

type sender interface {
	OpenWithContext(ctx context.Context, uri string, opt map[string]string) error
	IsOpen() bool
	WritePartWithContext(ctx context.Context, input io.ReadSeeker, opt map[string]string) (string, error)
	CancelWithContext(ctx context.Context) error
	CloseWithContext(ctx context.Context) error
}

type Uploader interface {
	Upload(ctx context.Context, uri string, options map[string]string,
		data chan dlData, snd sender) error
}

const MinAwsPartSize = 5 * 1024 * 1024 // 5MB

func getUploader(scheme string) (dl Uploader, err error) {
	switch scheme {
	case "file":
		return UploaderSimple{}, nil
	case "s3":
		return UploaderConcurrent{}, nil
	default:
		return nil, fmt.Errorf("upload scheme %v is not supported", scheme)
	}
}

func getSender(scheme string, size int64) (s sender, err error) {
	switch scheme {
	case "file":
		return &LocalFileSender{}, nil
	case "s3":
		if size >= MinAwsPartSize {
			return &S3SenderMultipart{}, nil
		}
		return &S3SenderSimple{}, nil
	default:
		return nil, fmt.Errorf("sender scheme %v is not supported", scheme)
	}
}
