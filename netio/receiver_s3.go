package netio

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"log"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type S3ReceiverRanged struct {
	m sync.Mutex
	bkt string
	key string
	s3client *s3.S3
}


func (r *S3ReceiverRanged) GetFileInfo(ctx context.Context, uri string, options map[string]string) (FileInfo, error) {

	var info FileInfo
	var bucket string
	var keyName string

	downloadUrl, err := url.Parse(uri)
	if err != nil {
		return info, err
	}

	style, ok := options["bucketNameStyle"]
	if !ok {
		// set 'path-style' bucket name by default
		// see https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html#access-bucket-intro
		style = "path-style"
	}

	if style == "path-style" {
		idx := strings.Index(downloadUrl.Path, "/")
		bucket = downloadUrl.Path[:idx]
		keyName = downloadUrl.Path[idx+1:]
	} else {
		hostname := downloadUrl.Hostname()
		idx := strings.Index(hostname, ".")
		bucket = hostname[:idx]
		keyName = downloadUrl.Path[1:] // skip first '/' char
	}

	sess, err := session.NewSession()
	if err != nil {
		return info, err
	}

	s3client := s3.New(sess)
	hr, err := s3client.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(keyName),
	})
	if err != nil {
		return info, err
	}

	info.Size = *hr.ContentLength

	return info, nil
}

func (r *S3ReceiverRanged) OpenWithContext(ctx context.Context, uri string, opt map[string]string) error {

	downloadUrl, err := url.Parse(uri)
	if err != nil {
		return err
	}

	r.m.Lock()
	style, ok := opt["bucketNameStyle"]
	if !ok {
		// set 'path-style' bucket name by default
		// see https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html#access-bucket-intro
		style = "path-style"
	}
	if style == "path-style" {
		idx := strings.Index(downloadUrl.Path, "/")
		r.bkt = downloadUrl.Path[:idx]
		r.key = downloadUrl.Path[idx+1:]
	} else {
		hostname := downloadUrl.Hostname()
		idx := strings.Index(hostname, ".")
		r.bkt = hostname[:idx]
		r.key = downloadUrl.Path[1:] // skip first '/' char
	}
	r.m.Unlock()

	sess, err := session.NewSession()
	if err != nil {
		return err
	}

	c := s3.New(sess)
	r.m.Lock()
	r.s3client = c
	r.m.Unlock()

	return nil
}

func (r *S3ReceiverRanged) IsOpen() bool {
	r.m.Lock()
	defer r.m.Unlock()
	isOpen := r.s3client != nil
	return isOpen
}

func (r *S3ReceiverRanged) ReadPartWithContext(ctx context.Context, output io.WriteSeeker,
	opt map[string]string) (string, error) {

	partSize, err := strconv.ParseInt(opt["partSize"], 10, 64)
	if err != nil {
		return "", err
	}

	rangeStart := opt["rangeStart"]
	rangeEnd   := opt["rangeEnd"]
	rg         := fmt.Sprintf("bytes=%v-%v", rangeStart, rangeEnd)

	r.m.Lock()
	bucket := r.bkt
	key    := r.key
	r.m.Unlock()

	log.Printf("GetObjectWithContext for range '%v'\n", rg)
	result, err := r.s3client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(rg),
	})
	if err != nil {
		return "", err
	}
	defer result.Body.Close()

	bsaved := int64(0)
	buffer := make([]byte, partSize)

	now := time.Now().UTC()

	for {
		br, err := result.Body.Read(buffer)
		if err != nil && err != io.EOF { // its an error (io.EOF is fine)
			return "", err
		}
		// Looks like there might be a bug in AWS SDK when executing ranged request
		// It is supposed to return io.EOF but instead returns nil and 0 bytes
		// Therefore put this condition as temporary solution
		if br == 0 {
			break
		}
		bw, err := output.Write(buffer[:br])
		if err != nil {
			return "", err
		}
		bsaved += int64(bw)
		if err == io.EOF { // done reading
			break
		}
		// print bytes downloaded every second
		// to do not clutter the log file
		if time.Since(now) > 1 * time.Second {
			now = time.Now().UTC()
			log.Printf("downloaded %v bytes\n", bsaved)
		}
	}

	log.Printf("downloaded %v bytes\n", bsaved)

	// making sure all bytes have been read
	if bsaved != *result.ContentLength {
		return "", fmt.Errorf("incomplete read operation. extected %v but received %v",
			bsaved, *result.ContentLength)
	}

	// done reading - close response body
	if err := result.Body.Close(); err != nil {
		return "", err
	}

	return *result.ETag, nil
}

func (r *S3ReceiverRanged) CancelWithContext(ctx context.Context) error {
	return fmt.Errorf("not impelmented")
}

func (r *S3ReceiverRanged) CloseWithContext(ctx context.Context) error {
	return fmt.Errorf("not impelmented")
}
