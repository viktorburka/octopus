package netio

import (
	"context"
	"io"
	"os"
)

type LocalFileSender struct {
	ptr *os.File
}

func (s *LocalFileSender) IsOpen() bool {
	return s.ptr != nil
}

func (s *LocalFileSender) OpenWithContext(ctx context.Context, uri string, opt map[string]string) error {
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
