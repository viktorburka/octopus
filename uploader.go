package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
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
	upload(ctx, uri, options, data, msg)
}

func upload(ctx context.Context, uri string, options map[string]string, data chan dlData, msg chan dlMessage) {

	var bucket string
	var keyName string
	var counter int64 = 1

	uploadUrl, err := url.Parse(uri)
	if err != nil {
		msg <- dlMessage{sender: "uploader", err: err}
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
		msg <- dlMessage{sender: "uploader", err: err}
		return
	}

	s3client := s3.New(s)

	//buf := make([]byte, 5*1024*1024) // 5MB buffer matches AWS multipart requirement
	//ptr := 0
	//
	//for {
	//	select {
	//	case chunk, ok := <-data:
	//		if !ok && len(data) == 0 { // channel closed and no data left
	//			msg<-dlMessage{sender:"uploader", err:nil}
	//			return
	//		}
	//		chunkPtr := 0
	//		for {
	//			bc := copy(buf[ptr:], chunk.data[chunkPtr:])
	//			ptr += bc
	//			if ptr == len(buf) { // the buffer is full
	//				uploadPart(buf)
	//				buf = make([]byte, 5*1024*1024)
	//				ptr = 0
	//			}
	//			if bc == len(chunk.data[chunkPtr:]) { // all data copied to the buffer
	//				break
	//			}
	//			chunkPtr += bc
	//		}
	//	case <-ctx.Done(): // there is cancellation
	//		msg <- dlMessage{sender: "uploader", err: ctx.Err()}
	//		return
	//	}
	//}

	tempDir, err := ioutil.TempDir(os.TempDir(), "")
	if err != nil {
		msg <- dlMessage{sender: "uploader", err: err}
		return
	}

	opErrCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var initMultipartUpload = func() (*s3.CreateMultipartUploadOutput, error) {

		mpuInput := &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(keyName),
		}

		// initiate multipart upload
		mpu, err := s3client.CreateMultipartUploadWithContext(opErrCtx, mpuInput)
		if err != nil {
			return nil, err
		}

		return mpu, nil
	}

	var total int64
	var fpath string
	var file *os.File
	var mpu *s3.CreateMultipartUploadOutput

	const MinAwsPartSize = 5 * 1024 * 1024
	const MaxWorkers = 5 // can't be 0 !

	errchan   := make(chan error)
	workers   := make(chan struct{}, MaxWorkers)
	etagschan := make(chan *s3.CompletedPart)
	etags     := make([]*s3.CompletedPart, 0)

	isMultipart := false
	isLastChunk := false
	exitOnError := false

    var wg sync.WaitGroup

    go func() {
        for {
            select {
            case et, ok := <-etagschan:
                if !ok {
                    return
                }
                etags = append(etags, et)
            case <-opErrCtx.Done():
                return
            }
        }
    }()

    defer func() {
        // the purpose of this is to let the helper goroutine
        // exit once this function exists
        close(etagschan)
    }()

	for !isLastChunk || exitOnError {
		select {
		case chunk, ok := <-data:

			if file == nil {
				// build part file name
				fileName := fmt.Sprintf("%v.part", counter)
				fpath = filepath.Join(tempDir, fileName)
				// create part file
				file, err = os.Create(fpath)
				if err != nil {
					cancel()
					msg <- dlMessage{sender: "uploader", err: err}
					return
				}
			}

			isLastChunk = !ok && len(data) == 0 // channel closed and no data left

			if chunk.size > 0 {
				bw, err := file.Write(chunk.data)
				if err != nil {
					cancel()
					msg <- dlMessage{sender: "uploader", err: err}
					return
				}
				total += int64(bw)
			}

			readyUpload := isLastChunk

			if total > MinAwsPartSize {
				if mpu == nil { // this the very first part - init multipart upload
					mpu, err = initMultipartUpload() // Warning: use '=' to refer to outer scope var not ':=' !
					if err != nil {
						cancel()
						msg <- dlMessage{sender: "uploader", err: err}
						return
					}
					isMultipart = true
				}
				readyUpload = true
			}

			if readyUpload {
				if err := file.Close(); err != nil {
					cancel()
					msg <- dlMessage{sender: "uploader", err: err}
					return
				}
				file = nil
				if isMultipart {
					wg.Add(1)
					go uploadPart(opErrCtx, s3client, fpath, bucket, keyName,
						counter, mpu.UploadId, etagschan, errchan, &wg, workers)
					total = 0
					counter += 1
				}
			}

			if isLastChunk {
				break
			}

		case err := <-errchan:
			cancel()
			msg <- dlMessage{sender: "uploader", err: err}

		case <-ctx.Done(): // there is cancellation
			msg <- dlMessage{sender: "uploader", err: ctx.Err()}
			exitOnError = true
		}
	}

	wg.Wait() // make sure all goroutines are finished

	if exitOnError {
		return
	}

	if isMultipart {
		err = completeMultipart(opErrCtx, s3client, bucket, keyName, etags, mpu.UploadId)
	} else {
		err = uploadWhole(opErrCtx, s3client, fpath, bucket, keyName)
	}
	if err != nil {
		msg <- dlMessage{sender: "uploader", err: err}
		return
	}

	msg <- dlMessage{sender: "uploader", err: nil}
}

func uploadPart(ctx context.Context, s3client *s3.S3, filePath string, bucket string, keyName string,
	pn int64, uploadId *string, etags chan *s3.CompletedPart, errchan chan error, wg *sync.WaitGroup,
	workers chan struct{}) {

	workers <- struct{}{}
	defer func() { <-workers }()

	defer wg.Done()
	// helper function to allocate int64 and
	// initialize it in one function call
	var newInt64 = func(init int64) *int64 {
		val := new(int64)
		*val = init
		return val
	}

	file, err := os.Open(filePath)
	if err != nil {
		errchan <- err
		return
	}
	defer file.Close()

	input := &s3.UploadPartInput{
		Body:       file,
		Bucket:     aws.String(bucket),
		Key:        aws.String(keyName),
		PartNumber: aws.Int64(pn),
		UploadId:   uploadId,
	}
	result, err := s3client.UploadPartWithContext(ctx, input)
	if err != nil {
		errchan <- err
		return
	}

	if err := os.Remove(filePath); err != nil {
		errchan <- err
		return
	}

	etags <- &s3.CompletedPart{ETag: result.ETag, PartNumber: newInt64(pn)}
}

func completeMultipart(ctx context.Context, s3client *s3.S3, bucket string,
	keyName string, etags []*s3.CompletedPart, uploadId *string) error {

	sort.Slice(etags, func(i, j int) bool {
		return *etags[i].PartNumber < *etags[j].PartNumber
	})

	cmpuInput := &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(keyName),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: etags,
		},
		UploadId: uploadId,
	}

	_, err := s3client.CompleteMultipartUploadWithContext(ctx, cmpuInput)
	if err != nil {
		return err
	}

	return nil
}

func uploadWhole(ctx context.Context, s3client *s3.S3, filePath string, bucket string, keyName string) error {

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	input := &s3.PutObjectInput{
		Body:   file,
		Bucket: aws.String(bucket),
		Key:    aws.String(keyName),
	}

	_, err = s3client.PutObjectWithContext(ctx, input)
	if err != nil {
		return err
	}

	return nil
}

func (f *FileSaver) Upload(ctx context.Context, uri string, options map[string]string, data chan dlData, msg chan dlMessage) {

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

	file, err := os.Create(tempFilePath)
	if err != nil {
		msg <- dlMessage{sender: "uploader", err: err}
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
				msg <- dlMessage{sender: "uploader", err: nil}
				return
			}
			log.Printf("Uploading %v bytes...\n", len(chunk.data))
			bw, err := file.Write(chunk.data)
			if err != nil {
				msg <- dlMessage{sender: "uploader", err: err}
				return
			}
			if bw != len(chunk.data) {
				incompleteOp := fmt.Errorf("file write operation error. expected to write %v bytes but got %v", len(chunk.data), bw)
				msg <- dlMessage{sender: "uploader", err: incompleteOp}
				return
			}
			totalBytes += uint64(bw)
		case <-ctx.Done(): // there is cancellation
			msg <- dlMessage{sender: "uploader", err: ctx.Err()}
			return
		}
	}
}
