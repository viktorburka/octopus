package netio

import (
	"bytes"
	"context"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path"
	"path/filepath"
)

// UploaderSimple is mostly for testing purposes to store locally
// whats downloaded by different schemas to verify
type UploaderSimple struct {
}

func (f UploaderSimple) Upload(ctx context.Context, uri string, options map[string]string,
	data chan dlData, s sender) error {

	tempDir, err := ioutil.TempDir(os.TempDir(), "")
	if err != nil {
		return err
	}

	uploadUrl, err := url.Parse(uri)
	if err != nil {
		return err
	}

	fileName := path.Base(uploadUrl.Path)
	tempFilePath := filepath.Join(tempDir, fileName)

	if err := s.OpenWithContext(context.Background(), tempFilePath, options); err != nil {
		return err
	}
	defer s.CloseWithContext(context.Background())

	var totalBytes uint64

	for {
		select {
		case chunk, ok := <-data:
			if !ok { // channel closed
				log.Println("Upload finished. Total size:", totalBytes)
				return nil
			}
			reader := bytes.NewReader(chunk.data)
			_, err := s.WritePartWithContext(context.Background(), reader, map[string]string{})
			if err != nil {
				return err
			}
			totalBytes += uint64(len(chunk.data))
		case <-ctx.Done(): // there is cancellation
			return ctx.Err()
		}
	}
}
