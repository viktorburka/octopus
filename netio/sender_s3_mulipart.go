package netio

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type S3SenderMultipart struct {
	m   sync.Mutex
	key string
	bkt string
	mpu      *s3.CreateMultipartUploadOutput
	s3client *s3.S3
	etags  []*s3.CompletedPart
}

func (s *S3SenderMultipart) OpenWithContext(ctx context.Context, uri string, opt map[string]string) error {

	uploadUrl, err := url.Parse(uri)
	if err != nil {
		return err
	}

	s.m.Lock()
	s.etags = make([]*s3.CompletedPart, 0)
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

	mpuInput := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bkt),
		Key:    aws.String(s.key),
	}

	// initiate multipart upload
	mpu, err := s.s3client.CreateMultipartUploadWithContext(ctx, mpuInput)
	if err != nil {
		return err
	}

	s.m.Lock()
	s.mpu = mpu
	s.m.Unlock()

	return nil
}

func (s *S3SenderMultipart) IsOpen() bool {
	s.m.Lock()
	defer s.m.Unlock()
	isOpen := s.s3client != nil && s.mpu != nil
	return isOpen
}

func (s *S3SenderMultipart) WritePartWithContext(ctx context.Context, input io.ReadSeeker,
	opt map[string]string) (string, error) {

	// helper function to allocate int64 and
	// initialize it in one function call
	var newInt64 = func(init int64) *int64 {
		val := new(int64)
		*val = init
		return val
	}

	pn, err := strconv.ParseInt(opt["partNumber"], 10, 64)
	if err != nil {
		return "", err
	}

	s.m.Lock()
	bucket := s.bkt
	key    := s.key
	uplid  := *s.mpu.UploadId // making sure we create a copy
	s.m.Unlock()

	result, err := s.s3client.UploadPartWithContext(ctx, &s3.UploadPartInput{
		Body:       input,
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		UploadId:   aws.String(uplid),
		PartNumber: aws.Int64(pn),
	})
	if err != nil {
		return "", err
	}

	s.m.Lock()
	s.etags = append(s.etags, &s3.CompletedPart{ETag: result.ETag, PartNumber: newInt64(pn)})
	s.m.Unlock()

	return *result.ETag, nil
}

func (s *S3SenderMultipart) CancelWithContext(ctx context.Context) error {
	s.m.Lock()
	bucket := s.bkt
	key    := s.key
	uplid  := *s.mpu.UploadId
	s.m.Unlock()
	// cancel multipart upload if any
	_, err := s.s3client.AbortMultipartUploadWithContext(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uplid),
	})
	if err != nil {
		err = fmt.Errorf("mulipart upload cancelation error: %v", err)
		return err
	}

	s.m.Lock()
	s.mpu      = nil
	s.s3client = nil
	s.etags    = nil
	s.m.Unlock()

	return nil
}

func (s *S3SenderMultipart) CloseWithContext(ctx context.Context) error {

	s.m.Lock()
	defer s.m.Unlock()

	sort.Slice(s.etags, func(i, j int) bool {
		return *s.etags[i].PartNumber < *s.etags[j].PartNumber
	})

	cmpuInput := &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(s.bkt),
		Key:    aws.String(s.key),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: s.etags,
		},
		UploadId: s.mpu.UploadId,
	}

	_, err := s.s3client.CompleteMultipartUploadWithContext(ctx, cmpuInput)
	if err != nil {
		return err
	}

	s.mpu      = nil
	s.s3client = nil
	s.etags    = nil

	return nil
}
