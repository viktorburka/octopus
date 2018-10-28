package netio

import (
	"bufio"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"log"
	"net/url"
	"strings"
)

type S3Downloader struct {
}

func (s *S3Downloader) Download(ctx context.Context, uri string,
	options map[string]string, data chan dlData, msg chan dlMessage) {

	var bucket  string
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

	//tempDir, err := ioutil.TempDir(os.TempDir(), "")
	//if err != nil {
	//	msg <- dlMessage{sender: "downloader", err: err}
	//	return
	//}

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

	if *hr.ContentLength > MinAwsPartSize {
		rangedDownload(ctx, s3client, bucket, keyName, data, msg, MinAwsPartSize, *hr.ContentLength)
	} else {
		singleDownload(ctx, s3client, bucket, keyName, data, msg, *hr.ContentLength)
	}

	//msg <- dlMessage{sender: "downloader", err: nil}

	//result, err := s3client.GetObjectWithContext(opErrCtx, &s3.GetObjectInput{
	//	Bucket: aws.String(bucket),
	//	Key:    aws.String(keyName),
	//})
	//if err != nil {
	//	msg <- dlMessage{sender: "downloader", err: err}
	//	return
	//}
	//defer result.Body.Close()
	//
	//fmt.Println("")
}

func rangedDownload(ctx context.Context, s3client *s3.S3, bucket string, key string, data chan dlData,
	msg chan dlMessage, partSize int64, contentLength int64)  {

}

func singleDownload(ctx context.Context, s3client *s3.S3, bucket string, key string, data chan dlData,
	msg chan dlMessage, contentLength int64)  {

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
			msg<-dlMessage{sender:"downloader", err: err}
			return
		}
		totalBytesRead += int64(br)
		log.Printf("downloader: received %v bytes\n", totalBytesRead)
		data<-dlData{data:buffer[:br], br:totalBytesRead, total:contentLength}
		if err == io.EOF { // done reading
			close(data)
			break
		}
	}

	log.Println("download finished. total size:", totalBytesRead)
	msg<-dlMessage{sender:"downloader", err: nil}
}
