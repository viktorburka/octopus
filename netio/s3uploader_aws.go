package netio

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"log"
	"net/url"
	"strings"
)

type chanReader struct {
	ctx context.Context
	data chan dlData
	rem []byte
}

func (c *chanReader) Read(p []byte) (int, error) {
	if len(c.rem) > 0 {
		bc := copy(p, c.rem)
		c.rem = c.rem[bc:]
		return bc, nil
	}
	select {
	case chunk, ok := <-c.data:
		if !ok && chunk.data == nil { // channel closed and no data left
			return 0, io.EOF
		}
		bc := copy(p, chunk.data)
		c.rem = chunk.data[bc:]
		return bc, nil
	case <-c.ctx.Done(): // there is cancellation
		return 0, c.ctx.Err()
	}
}

func newChanReader(ctx context.Context, data chan dlData) *chanReader {
	return &chanReader{ctx: ctx, data: data, rem: make([]byte,0)}
}

type S3UploaderAws struct {
}

func (s *S3UploaderAws) Upload(ctx context.Context, uri string, options map[string]string, data chan dlData, msg chan dlMessage) {

	var bucket  string
	var keyName string

	uploadUrl, err := url.Parse(uri)
	if err != nil {
		msg<-dlMessage{sender:"uploader", err:err}
		return
	}

	style, ok := options["bucketNameStyle"]
	if !ok {
		// set 'path-style' bucket name by default
		// see https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html#access-bucket-intro
		style = "path-style"
	}

	if style == "path-style" {
		idx := strings.Index(uploadUrl.Path, "/")
		bucket = uploadUrl.Path[:idx]
		keyName = uploadUrl.Path[idx+1:]
	} else {
		hostname := uploadUrl.Hostname()
		idx := strings.Index(hostname, ".")
		bucket = hostname[:idx]
		keyName = uploadUrl.Path[1:] // skip first '/' char
	}

	sess, err := session.NewSession()
	if err != nil {
		msg<-dlMessage{sender:"uploader", err:err}
		return
	}

	uploader := s3manager.NewUploader(sess, func(u *s3manager.Uploader) {
		u.PartSize = 5*1024*1024
	})
	result, err := uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(keyName),
		Body:   newChanReader(ctx, data),
	})
	if err != nil {
		msg<-dlMessage{sender:"uploader", err:err}
		return
	}

	log.Println("Successfully uploaded object to", result.Location)
	msg<-dlMessage{sender:"uploader", err:nil}
}
