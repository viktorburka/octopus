package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"net/url"
	"strings"
)

type S3DownloaderAws struct {
}

func (s *S3DownloaderAws) Download(ctx context.Context, uri string,
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

	const MinLenForConcurrent   = 10*1024*1024 // 10MB
	const RangeDownloadPartSize = MinLenForConcurrent / 2

	fmt.Println("File length:", *hr.ContentLength)
	if hr.PartsCount != nil {
		multipartDownload(ctx, bucket, keyName, data, msg, *hr.PartsCount, *hr.ContentLength)
	} else if *hr.ContentLength > MinLenForConcurrent {
		rangeDownload(ctx, bucket, keyName, data, msg, RangeDownloadPartSize, *hr.ContentLength)
	} else {
		singlePartDownload(ctx, bucket, keyName, data, msg, *hr.ContentLength)
	}

	msg <- dlMessage{sender: "downloader", err: fmt.Errorf("this is not an error")}

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

func multipartDownload(ctx context.Context, bucket string, key string, data chan dlData,
	msg chan dlMessage, partsCount int64, contentLength int64) {

}

func rangeDownload(ctx context.Context, bucket string, key string, data chan dlData,
	msg chan dlMessage, partSize int64, contentLength int64)  {

}

func singlePartDownload(ctx context.Context, bucket string, key string, data chan dlData,
	msg chan dlMessage, contentLength int64)  {

}