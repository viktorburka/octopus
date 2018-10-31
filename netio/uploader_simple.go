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
	data chan dlData, msg chan dlMessage, s sender) {

	tempDir, err := ioutil.TempDir(os.TempDir(), "")
	if err != nil {
		msg <- dlMessage{sender: "uploader", err: err}
		return
	}

	uploadUrl, err := url.Parse(uri)
	if err != nil {
		msg <- dlMessage{sender: "uploader", err: err}
		return
	}

	fileName := path.Base(uploadUrl.Path)
	tempFilePath := filepath.Join(tempDir, fileName)

	err = s.OpenWithContext(context.Background(), tempFilePath, options)
	if err != nil {
		msg <- dlMessage{sender: "uploader", err: err}
		return
	}
	defer s.CloseWithContext(context.Background())

	var totalBytes uint64

	for {
		select {
		case chunk, ok := <-data:
			if !ok { // channel closed
				log.Println("Upload finished. Total size:", totalBytes)
				return
			}
			reader := bytes.NewReader(chunk.data)
			_, err := s.WritePartWithContext(context.Background(), reader, map[string]string{})
			if err != nil {
				msg <- dlMessage{sender: "uploader", err: err}
				return
			}
			totalBytes += uint64(len(chunk.data))
		case <-ctx.Done(): // there is cancellation
			return
		}
	}
}
