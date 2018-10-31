package netio

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"log"
	"net/url"
	"strings"
	"sync"
)

type S3SenderSimple struct {
	m sync.Mutex
	key string
	bkt string
	isOpen   bool
	s3client *s3.S3
}

func (s *S3SenderSimple) IsOpen() bool {
	s.m.Lock()
	defer s.m.Unlock()
	return s.isOpen
}

func (s *S3SenderSimple) OpenWithContext(ctx context.Context, uri string, opt map[string]string) error {
	uploadUrl, err := url.Parse(uri)
	if err != nil {
		return err
	}

	s.m.Lock()
	style, ok := opt["bucketNameStyle"]
	if !ok {
		// set 'path-style' bucket name by default
		// see https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html#access-bucket-intro
		style = "path-style"
	}
	if style == "path-style" {
		idx := strings.Index(uploadUrl.Path, "/")
		s.bkt = uploadUrl.Path[:idx]
		s.key = uploadUrl.Path[idx+1:]
	} else {
		hostname := uploadUrl.Hostname()
		idx := strings.Index(hostname, ".")
		s.bkt = hostname[:idx]
		s.key = uploadUrl.Path[1:] // skip first '/' char
	}
	s.m.Unlock()

	sess, err := session.NewSession()
	if err != nil {
		return err
	}

	c := s3.New(sess)
	s.m.Lock()
	s.s3client = c
	s.m.Unlock()

	return nil
}

func (s *S3SenderSimple) WritePartWithContext(ctx context.Context, input io.ReadSeeker,
	opt map[string]string) (string, error) {

	s.m.Lock()
	bucket := s.bkt
	key    := s.key
	s.m.Unlock()

	log.Println("sending single chunk of data to S3 via PutObjectWithContext")
	result, err := s.s3client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Body:   input,
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", err
	}

	return *result.ETag, nil
}

func (s *S3SenderSimple) CancelWithContext(ctx context.Context) error {
	s.m.Lock()
	defer s.m.Unlock()
	s.isOpen   = false
	s.s3client = nil
	return nil
}

func (s *S3SenderSimple) CloseWithContext(ctx context.Context) error {
	s.m.Lock()
	defer s.m.Unlock()
	s.isOpen   = false
	s.s3client = nil
	return nil
}
