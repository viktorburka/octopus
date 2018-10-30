package netio

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path"
	"path/filepath"
)

type LocalFileSender struct {
	opt map[string]string
	ptr *os.File
}

func (s *LocalFileSender) Init(opt map[string]string) {
	s.opt = opt
}

func (s *LocalFileSender) IsOpen() bool {
	return s.ptr != nil
}

func (s *LocalFileSender) OpenWithContext(ctx context.Context, uri string) error {
	var err error
	s.ptr, err = os.Create(uri)
	return err
}

func (s *LocalFileSender) WritePartWithContext(ctx context.Context, input io.ReadSeeker,
	opt map[string]string) (string, error) {

	var err error
	_, err = io.Copy(s.ptr, input)
	return "", err
}

func (s *LocalFileSender) CancelWithContext(ctx context.Context) error {
	err := s.ptr.Close()
	s.ptr = nil
	return err
}

func (s *LocalFileSender) CloseWithContext(ctx context.Context) error {
	err := s.ptr.Close()
	s.ptr = nil
	return err
}

// UploaderLocalFile is mostly for testing purposes to store locally
// whats downloaded by different schemas to verify
type UploaderLocalFile struct {
}

func (f UploaderLocalFile) Upload(ctx context.Context, uri string, options map[string]string,
	data chan dlData, msg chan dlMessage, s sender) {

	log.Println("Prepare upload. Constructing temp folder...")

	tempDir, err := ioutil.TempDir(os.TempDir(), "")
	if err != nil {
		msg <- dlMessage{sender: "uploader", err: err}
		return
	}

	log.Println("temp dir:", tempDir)

	uploadUrl, err := url.Parse(uri)
	if err != nil {
		msg <- dlMessage{sender: "uploader", err: err}
		return
	}

	fileName := path.Base(uploadUrl.Path)
	tempFilePath := filepath.Join(tempDir, fileName)

	err = s.OpenWithContext(context.Background(), tempFilePath)
	if err != nil {
		msg <- dlMessage{sender: "uploader", err: err}
		return
	}
	defer s.CloseWithContext(context.Background())

	var totalBytes uint64

	log.Println("Start upload")
	for {
		select {
		case chunk, ok := <-data:
			if !ok { // channel closed
				log.Println("Upload finished. Total size:", totalBytes)
				msg <- dlMessage{sender: "uploader", err: nil}
				return
			}
			log.Printf("Uploading %v bytes...\n", len(chunk.data))
			reader := bytes.NewReader(chunk.data)
			_, err := s.WritePartWithContext(context.Background(), reader, map[string]string{})
			if err != nil {
				msg <- dlMessage{sender: "uploader", err: err}
				return
			}
			totalBytes += uint64(len(chunk.data))
		case <-ctx.Done(): // there is cancellation
			msg <- dlMessage{sender: "uploader", err: ctx.Err()}
			return
		}
	}
}
