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

	fmt.Println("File length:", *hr.ContentLength)

	if *hr.ContentLength >= MinAwsPartSize {
		rangedDownload(ctx, s3client, bucket, keyName, data, msg, MinAwsPartSize, *hr.ContentLength)
	} else {
		singleDownload(ctx, s3client, bucket, keyName, data, msg, *hr.ContentLength)
	}
}

func rangedDownload(ctx context.Context, s3client *s3.S3, bucket string, key string, data chan dlData,
	msg chan dlMessage, partSize int64, contentLength int64) {

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

	partschan := make(chan int)
	defer close(partschan)

	opErrCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup

	// this func retrieves the object from S3 in parts
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(partschan)
		// helper min function that takes integer (Math.Min takes float)
		var min = func(v1 int64, v2 int64) int64 {
			if v1 <= v2 {
				return v1
			}
			return v2
		}

		partNumber := 0
		totalBytes := int64(0)

		for totalBytes < contentLength {

			start := totalBytes
			end := min(totalBytes+partSize, contentLength)
			rg := fmt.Sprintf("bytes=%v-%v", start, end-1)
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
			defer result.Body.Close() //TODO: move from here !!!

			fileName := fmt.Sprintf("%v.part", partNumber)
			filePath := filepath.Join(tempDir, fileName)

			file, err := os.Create(filePath)
			if err != nil {
				msg <- dlMessage{sender: "downloader", err: err}
				return
			}

			reader := bufio.NewReader(result.Body)
			writer := bufio.NewWriter(file)

			//TODO: might be long and blocking operation - can't use it here
			bw, err := io.Copy(writer, reader)
			if err != nil {
				msg <- dlMessage{sender: "downloader", err: err}
				return
			}

			// making sure all bytes have been read
			if bw != *result.ContentLength {
				msg <- dlMessage{sender: "downloader", err: fmt.Errorf("incomplete write operation")}
				return
			}

			// done reading - close response body
			if err := result.Body.Close(); err != nil {
				msg <- dlMessage{sender: "downloader", err: err}
				return
			}

			partschan <- partNumber

			partNumber++
			totalBytes += bw
		}
	}()

	// this function sends the received parts to uploader
	wg.Add(1)
	go func() {
		defer wg.Done()
		var last = func(val int64) int64 {
			if val > 0 {
				return 1
			}
			return 0
		}
		parts := make([]bool, contentLength/partSize+last(contentLength%partSize))
		ptr := 0
		sent := int64(0)
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
					fileName := fmt.Sprintf("%v.part", part)
					filePath := filepath.Join(tempDir, fileName)

					bw, err := writePart(opErrCtx, filePath, sent, contentLength, data)
					if err != nil {
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
