package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

type Uploader interface {
    Upload(ctx context.Context, uri string, options map[string]string, data chan dlData, msg chan dlMessage)
}

type S3Uploader struct {
}

// FileSaver is mostly for testing purposes to store locally
// whats downloaded by different schemas to verify
type FileSaver struct {
}

func getUploaderForScheme(scheme string) (dl Uploader, err error) {
    switch scheme {
    case "file":
        return &FileSaver{}, nil
    case "s3":
        return &S3Uploader{}, nil
    default:
        return nil, fmt.Errorf("upload scheme %v is not supported", scheme)
    }
}

func (s *S3Uploader) Upload(ctx context.Context, uri string, options map[string]string, data chan dlData, msg chan dlMessage) {
	// TODO: add file size check here to perform
	// TODO: multipart or single upload based on AWS guidelines
	upload(ctx, uri, options, data, msg)
}

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

func upload(ctx context.Context, uri string, options map[string]string, data chan dlData, msg chan dlMessage) {

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

	s, err := session.NewSession()
	if err != nil {
		printAwsError(err)
		msg<-dlMessage{sender:"uploader", err:err}
		return
	}

	uploader := s3manager.NewUploader(s, func(u *s3manager.Uploader) {
		u.PartSize = 5*1024*1024 //TODO: this value has to be in sync with downloader buf
	})
	result, err := uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(keyName),
		Body:   newChanReader(ctx, data),
	})
	if err != nil {
		printAwsError(err)
		msg<-dlMessage{sender:"uploader", err:err}
		return
	}

	log.Println("Successfully uploaded object to", result.Location)
	msg<-dlMessage{sender:"uploader", err:nil}
}

// Performs concurrent multipart upload
func uploadMultiPart(ctx context.Context, uri string, options map[string]string, data chan dlData, msg chan dlMessage)  {

	// helper function to allocate int64 and
	// initialize it in one function call
	var newInt64 = func(init int64) *int64 {
		val := new(int64)
		*val = init
		return val
	}

	var bucket  string
	var keyName string

	var partNumber int64 = 1 // part number must start from 1 according to AWS SDK
	var totalBytes int64

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

	s, err := session.NewSession()
	if err != nil {
		printAwsError(err)
		msg<-dlMessage{sender:"uploader", err:err}
		return
	}

	s3client := s3.New(s)

	mpuInput := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(keyName),
	}

	// initiate multipart upload
	log.Println("Start upload")
	mpu, err := s3client.CreateMultipartUploadWithContext(ctx, mpuInput)
	if err != nil {
		printAwsError(err)
		msg<-dlMessage{sender:"uploader", err:err}
		return
	}

	etags := make([]*s3.CompletedPart, 0)

	for {
		select {
		case chunk, ok := <-data: {
			if !ok { // channel closed
				sort.Slice(etags, func(i, j int) bool {
					return *etags[i].PartNumber < *etags[j].PartNumber
				})

				cmpuInput := &s3.CompleteMultipartUploadInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(keyName),
					MultipartUpload: &s3.CompletedMultipartUpload{
						Parts: etags,
					},
					UploadId: mpu.UploadId,
				}

				log.Println("Finishing upload...")
				result, err := s3client.CompleteMultipartUploadWithContext(ctx, cmpuInput)
				if err != nil {
					printAwsError(err)
					msg<-dlMessage{sender:"uploader", err:err}
					return
				}

				log.Println("Successfully uploaded object", *result.Key, "to bucket. Etag:", *result.Bucket, *result.ETag)
				log.Println("Total size:", totalBytes)

				msg<-dlMessage{sender:"uploader", err:nil}
				return
			}
			log.Printf("Uploading %v bytes...\n", len(chunk.data))

			// put it in a separate function
			// to turn it into a goroutine
			func(buf []byte, pn int64) {
				bufReader := bytes.NewReader(buf)
				input := &s3.UploadPartInput{
					Body:       bufReader,
					Bucket:     aws.String(bucket),
					Key:        aws.String(keyName),
					PartNumber: aws.Int64(pn),
					UploadId:   mpu.UploadId,
				}
				result, err := s3client.UploadPartWithContext(ctx, input)
				if err != nil {
					printAwsError(err)
					msg<-dlMessage{sender:"uploader", err:err}
					return
				}
				etags = append(etags, &s3.CompletedPart{ETag:result.ETag, PartNumber:newInt64(pn)})
				totalBytes += bufReader.Size()
			}(chunk.data, partNumber)  // bytes read, not buffer size

			partNumber++
		}
		case <-ctx.Done(): // there is cancellation
			msg<-dlMessage{sender:"uploader", err:ctx.Err()}
			return
		}
	}
}

func printAwsError(err error) {
	if aerr, ok := err.(awserr.Error); ok {
		log.Printf("%v (code: %v)\n", aerr.Error(), aerr.Code())
	} else {
		log.Println(err.Error())
	}
}

func (f *FileSaver) Upload(ctx context.Context, uri string, options map[string]string, data chan dlData, msg chan dlMessage) {

    log.Println("Prepare upload. Constructing temp folder...")

    tempDir, err := ioutil.TempDir(os.TempDir(), "")
    if err != nil {
        msg<-dlMessage{sender:"uploader", err:err}
        return
    }

    log.Println("temp dir:", tempDir)

    uploadUrl, err := url.Parse(uri)
    if err != nil {
        msg<-dlMessage{sender:"uploader", err:err}
        return
    }

    fileName := path.Base(uploadUrl.Path)
    tempFilePath := filepath.Join(tempDir, fileName)

    file, err := os.Create(tempFilePath)
    if err != nil {
        msg<-dlMessage{sender:"uploader", err:err}
        return
    }
    defer file.Close()

    var totalBytes uint64

    log.Println("Start upload")
    for {
        select {
        case chunk, ok := <-data:
            if !ok { // channel closed
                log.Println("Upload finished. Total size:", totalBytes)
                msg<-dlMessage{sender:"uploader", err:nil}
                return
            }
            log.Printf("Uploading %v bytes...\n", len(chunk.data))
            bw, err := file.Write(chunk.data)
            if err != nil {
                msg<-dlMessage{sender:"uploader", err:err}
                return
            }
            if bw != len(chunk.data) {
                incompleteOp := fmt.Errorf("file write operation error. expected to write %v bytes but got %v", len(chunk.data), bw)
                msg<-dlMessage{sender:"uploader", err:incompleteOp}
                return
            }
            totalBytes += uint64(bw)
        case <-ctx.Done(): // there is cancellation
            msg<-dlMessage{sender:"uploader", err:ctx.Err()}
            return
        }
    }
}
