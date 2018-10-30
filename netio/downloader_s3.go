package netio

import (
	"bufio"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type DownloaderS3 struct {
}

type S3ReceiverMultipart struct {
	m sync.Mutex
	bkt string
	key string
	s3client *s3.S3

}


func (r *S3ReceiverMultipart) OpenWithContext(ctx context.Context, uri string, opt map[string]string) error {

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

func (r *S3ReceiverMultipart) IsOpen() bool {
	r.m.Lock()
	defer r.m.Unlock()
	isOpen := r.s3client != nil
	return isOpen
}

func (r *S3ReceiverMultipart) ReadPartWithContext(ctx context.Context, output io.WriteSeeker,
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
	}

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

func (r *S3ReceiverMultipart) CancelWithContext(ctx context.Context) error {
	return fmt.Errorf("not impelmented")
}

func (r *S3ReceiverMultipart) CloseWithContext(ctx context.Context) error {
	return fmt.Errorf("not impelmented")
}

func (r *S3ReceiverMultipart) GetFileInfo(ctx context.Context, uri string, options map[string]string) (FileInfo, error) {

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

func (s DownloaderS3) Download(ctx context.Context, uri string,
	options map[string]string, data chan dlData, msg chan dlMessage, rc receiver) {

	contentLength, err := strconv.ParseInt(options["contentLength"], 10, 64)
	if err != nil {
		msg <- dlMessage{sender: "downloader", err: err}
		return
	}

	if err := rc.OpenWithContext(ctx, uri, options); err != nil {
		msg <- dlMessage{sender: "downloader", err: err}
		return
	}

	var last = func(val int64) int64 {
		if val > 0 { return 1 }
		return 0
	}

	partSize := int64(MinAwsPartSize)

	if contentLength < partSize {
		err := fmt.Errorf("can't start ranged download: contentLength < partSize")
		msg <- dlMessage{sender: "downloader", err: err}
		return
	}

	tempDir, err := ioutil.TempDir(os.TempDir(), "")
	if err != nil {
		msg <- dlMessage{sender: "downloader", err: err}
		return
	}

	const MaxWorkers = 3 // can't be 0!

	partschan := make(chan int)
	workers   := make(chan struct{}, MaxWorkers)

	opErrCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup

	partsCount := contentLength/partSize+last(contentLength%partSize)

	// this func retrieves the object from S3 in parts
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(partschan)

		var wg2 sync.WaitGroup
		// helper min function that takes integer (Math.Min takes float)
		var min = func(v1 int64, v2 int64) int64 {
			if v1 <= v2 { return v1 }
			return v2
		}

		for partNumber := int64(0); partNumber < partsCount; partNumber++ {
			start := partNumber*partSize
			end   := min(start+partSize, contentLength)

			workers <- struct{}{}
			wg2.Add(1)
			go func(rangeStart int64, rangeEnd int64, curPart int64) {
				defer wg2.Done()
				defer func() {<-workers}()

				fileName := fmt.Sprintf("%v.part", curPart)
				filePath := filepath.Join(tempDir, fileName)

				file, err := os.Create(filePath)
				if err != nil {
					msg <- dlMessage{sender: "downloader", err: err}
					return
				}
				defer file.Close()

				opt := map[string]string {
					"rangeStart": strconv.FormatInt(rangeStart,10),
					"rangeEnd":   strconv.FormatInt(rangeEnd,10),
					"partSize":   strconv.FormatInt(partSize,10),
				}

				_, err = rc.ReadPartWithContext(opErrCtx, file, opt)
				if err != nil {
					msg <- dlMessage{sender: "downloader", err: err}
					return
				}

				// done writing - close file
				if err := file.Close(); err != nil {
					msg <- dlMessage{sender: "downloader", err: err}
					return
				}

				partschan <- int(curPart)

			}(start, end-1, partNumber)
		}

		wg2.Wait()
	}()

	// this function sends the received parts to uploader
	wg.Add(1)
	go func() {
		defer wg.Done()

		parts := make([]bool, partsCount)
		ptr   := 0
		sent  := int64(0)
		for {
			select {
			case part, ok := <-partschan:
				if !ok {
					return
				}
				if part >= len(parts) {
					// error
					cancel()
					return
				}
				parts[part] = true
				for ; ptr < len(parts); ptr++ {
					if !parts[ptr] {
						break
					}
					fileName := fmt.Sprintf("%v.part", ptr)
					filePath := filepath.Join(tempDir, fileName)

					bw, err := writePart(opErrCtx, filePath, sent, contentLength, data)
					if err != nil {
						msg <- dlMessage{sender: "downloader", err: err}
						return
					}
					if err := os.Remove(filePath); err != nil {
						msg <- dlMessage{sender: "downloader", err: err}
						return
					}
					sent += int64(bw)
				}
			case <-opErrCtx.Done():
				return
			}
		}
	}()

	wg.Wait()

	close(data)
}

func writePart(ctx context.Context, filePath string, totalSent int64, totalSize int64, data chan dlData) (int, error) {

	totalBytesRead := 0
	totalBytesSent := totalSent

	file, err := os.Open(filePath)
	if err != nil {
		return -1, err
	}
	defer file.Close()

	// read data
	reader := bufio.NewReader(file)
	buffer := make([]byte, 3*1024*1024)
	for {
		br, err := reader.Read(buffer)
		if err != nil && err != io.EOF { // its an error (io.EOF is fine)
			return -1, err
		}
		totalBytesRead += br
		totalBytesSent += int64(br)
		select {
		case data <- dlData{data: buffer[:br], br: totalBytesSent, total: totalSize}:
			break
		case <-ctx.Done():
			return -1, ctx.Err()
		}
		if err == io.EOF { // done reading
			break
		}
	}

	return totalBytesRead, nil
}
