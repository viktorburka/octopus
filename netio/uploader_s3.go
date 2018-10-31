package netio

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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

type S3SenderMultipart struct {
	m   sync.Mutex
	key string
	bkt string
	mpu      *s3.CreateMultipartUploadOutput
	s3client *s3.S3
	etags  []*s3.CompletedPart
}

type UploaderS3Multipart struct {
}


func (s *S3SenderMultipart) IsOpen() bool {
	s.m.Lock()
	defer s.m.Unlock()
	isOpen := s.s3client != nil && s.mpu != nil
	return isOpen
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
	defer s.m.Unlock()
	s.etags = append(s.etags, &s3.CompletedPart{ETag: result.ETag, PartNumber: newInt64(pn)})

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

	s.mpu      = nil
	s.s3client = nil
	s.etags    = nil

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

func (s UploaderS3Multipart) Upload(ctx context.Context, uri string, options map[string]string,
	data chan dlData, msg chan dlMessage, snd sender) {

	tempDir, err := ioutil.TempDir(os.TempDir(), "")
	if err != nil {
		msg <- dlMessage{sender: "uploader", err: err}
		return
	}

	opErrCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	log.Println("open upload connection")

	if err := snd.OpenWithContext(opErrCtx, uri, options); err != nil {
		msg <- dlMessage{sender: "uploader", err: err}
		return
	}

	const MaxWorkers = 5 // can't be 0 !

	errchan := make(chan error)
	workers := make(chan struct{}, MaxWorkers)

	isLastChunk := false
	operationError := false

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		var counter int64 = 1
		var total int64
		var fpath string
		var file  *os.File

		var setErrorState = func(err error) {
			cancel()
			msg <- dlMessage{sender: "uploader", err: err}
			operationError = false
		}

		for !isLastChunk && !operationError {
			select {
			case chunk, ok := <-data:

				if file == nil {
					// build part file name
					fileName := fmt.Sprintf("%v.part", counter)
					fpath = filepath.Join(tempDir, fileName)
					// create part file
					file, err = os.Create(fpath)
					if err != nil {
						setErrorState(err)
						break
					}
				}

				isLastChunk = !ok || chunk.done // channel closed and no data left

				if len(chunk.data) > 0 {
					bw, err := file.Write(chunk.data)
					if err != nil {
						setErrorState(err)
						break
					}
					total += int64(bw)
				}

				if total >= MinAwsPartSize || isLastChunk { // ready to start upload
					// close buffer file
					if err := file.Close(); err != nil {
						setErrorState(err)
						break
					}
					file = nil

					workers <- struct{}{}
					wg.Add(1)
					go uploadPart(opErrCtx, fpath, counter, errchan, &wg, workers, snd)
					total = 0
					counter += 1
				}

				if isLastChunk {
					break
				}

			case err := <-errchan: // multipart upload error
				cancel()
				msg <- dlMessage{sender: "uploader", err: err}

			case <-opErrCtx.Done(): // there is cancellation
				operationError = true
			}
		}
	}()

	// make sure all goroutines are finished
	wg.Wait()

	if operationError {
		if err := snd.CancelWithContext(ctx); err != nil {
			msg <- dlMessage{sender: "uploader", err: err}
		}
		return
	}

	if err := snd.CloseWithContext(opErrCtx); err != nil {
		msg <- dlMessage{sender: "uploader", err: err}
		return
	}
}

func uploadPart(ctx context.Context, filePath string, pn int64, errchan chan error, wg *sync.WaitGroup,
	workers chan struct{}, snd sender) {

	defer func() { <-workers }()
	defer wg.Done()

	log.Println("start uploading part", pn)

	file, err := os.Open(filePath)
	if err != nil {
		errchan <- err
		return
	}
	defer file.Close()

	opt := map[string]string {
		"partNumber": strconv.FormatInt(pn, 10),
	}

	_, err = snd.WritePartWithContext(ctx, file, opt)
	if err != nil {
		errchan <- err
		return
	}

	log.Println("finished uploading part", pn)

	if err := file.Close(); err != nil {
		errchan <- err
		return
	}

	if err := os.Remove(filePath); err != nil {
		errchan <- err
		return
	}
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
	return nil
}

func (s *S3SenderSimple) CloseWithContext(ctx context.Context) error {
	return nil
}


type chanReader struct {
	ctx  context.Context
	data chan dlData
	rem  []byte
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
	return &chanReader{ctx: ctx, data: data, rem: make([]byte, 0)}
}


type UploaderS3Manager struct {
}

func (s *UploaderS3Manager) Upload(ctx context.Context, uri string, options map[string]string, data chan dlData, msg chan dlMessage) {

	var bucket string
	var keyName string

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

	sess, err := session.NewSession()
	if err != nil {
		msg <- dlMessage{sender: "uploader", err: err}
		return
	}

	uploader := s3manager.NewUploader(sess, func(u *s3manager.Uploader) {
		u.PartSize = 5 * 1024 * 1024
	})
	result, err := uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(keyName),
		Body:   newChanReader(ctx, data),
	})
	if err != nil {
		msg <- dlMessage{sender: "uploader", err: err}
		return
	}

	log.Println("Successfully uploaded object to", result.Location)
	msg <- dlMessage{sender: "uploader", err: nil}
}
