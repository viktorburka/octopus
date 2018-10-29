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
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type DownloaderS3 struct {
}

func (s *DownloaderS3) Download(ctx context.Context, uri string,
	options map[string]string, data chan dlData, msg chan dlMessage) {

	var bucket string
	var keyName string

	downloadUrl, err := url.Parse(uri)
	if err != nil {
		msg <- dlMessage{sender: "downloader", err: err}
		return
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
		msg <- dlMessage{sender: "downloader", err: err}
		return
	}

	s3client := s3.New(sess)

	opErrCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	hr, err := s3client.HeadObjectWithContext(opErrCtx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(keyName),
	})
	if err != nil {
		msg <- dlMessage{sender: "downloader", err: err}
		return
	}

	if *hr.ContentLength >= MinAwsPartSize {
		rangedDownload(ctx, s3client, bucket, keyName, data, msg, MinAwsPartSize, *hr.ContentLength)
	} else {
		singleDownload(ctx, s3client, bucket, keyName, data, msg, *hr.ContentLength)
	}
}

func rangedDownload(ctx context.Context, s3client *s3.S3, bucket string, key string, data chan dlData,
	msg chan dlMessage, partSize int64, contentLength int64) {

	var last = func(val int64) int64 {
		if val > 0 { return 1 }
		return 0
	}

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

				rg    := fmt.Sprintf("bytes=%v-%v", rangeStart, rangeEnd)
				input := &s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
					Range:  aws.String(rg),
				}

				result, err := s3client.GetObjectWithContext(opErrCtx, input)
				if err != nil {
					msg <- dlMessage{sender: "downloader", err: err}
					return
				}
				defer result.Body.Close()

				fileName := fmt.Sprintf("%v.part", curPart)
				filePath := filepath.Join(tempDir, fileName)

				file, err := os.Create(filePath)
				if err != nil {
					msg <- dlMessage{sender: "downloader", err: err}
					return
				}
				defer file.Close()

				bsaved := int64(0)
				buffer := make([]byte, partSize)

				for {
					br, err := result.Body.Read(buffer)
					if err != nil && err != io.EOF { // its an error (io.EOF is fine)
						msg <- dlMessage{sender: "downloader", err: err}
						return
					}
					// Looks like there might be a bug in AWS SDK when executing ranged request
					// It is supposed to return io.EOF but instead returns nil and 0 bytes
					// Therefore put this condition as temporary solution
					if br == 0 {
						break
					}
					bw, err := file.Write(buffer[:br])
					if err != nil {
						msg <- dlMessage{sender: "downloader", err: err}
						return
					}
					bsaved += int64(bw)
					if err == io.EOF { // done reading
						break
					}
				}

				// making sure all bytes have been read
				if bsaved != *result.ContentLength {
					msg <- dlMessage{sender: "downloader", err: fmt.Errorf("incomplete write operation")}
					return
				}

				// done writing - close file
				if err := file.Close(); err != nil {
					msg <- dlMessage{sender: "downloader", err: err}
					return
				}

				// done reading - close response body
				if err := result.Body.Close(); err != nil {
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

func singleDownload(ctx context.Context, s3client *s3.S3, bucket string, key string, data chan dlData,
	msg chan dlMessage, contentLength int64) {

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	result, err := s3client.GetObjectWithContext(ctx, input)
	if err != nil {
		msg <- dlMessage{sender: "downloader", err: err}
		return
	}
	defer result.Body.Close()

	var totalBytesRead int64

	// read data
	reader := bufio.NewReader(result.Body)
	buffer := make([]byte, MinAwsPartSize)
	for {
		br, err := reader.Read(buffer)
		if err != nil && err != io.EOF { // its an error (io.EOF is fine)
			msg <- dlMessage{sender: "downloader", err: err}
			return
		}
		totalBytesRead += int64(br)
		log.Printf("downloader: received %v bytes\n", totalBytesRead)
		data <- dlData{data: buffer[:br], br: totalBytesRead, total: contentLength}
		if err == io.EOF { // done reading
			close(data)
			break
		}
	}

	log.Println("download finished. total size:", totalBytesRead)
	msg <- dlMessage{sender: "downloader", err: nil}
}
