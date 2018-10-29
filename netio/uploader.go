package netio

import (
	"context"
	"fmt"
)

type Uploader interface {
	Upload(ctx context.Context, uri string, options map[string]string, data chan dlData, msg chan dlMessage)
}

const MinAwsPartSize = 5 * 1024 * 1024 // 5MB

func getUploaderForScheme(scheme string) (dl Uploader, err error) {
	switch scheme {
	case "file":
		return UploaderLocalFile{}, nil
	case "s3":
		return UploaderS3{}, nil
	default:
		return nil, fmt.Errorf("upload scheme %v is not supported", scheme)
	}
}
