package netio

import (
	"context"
	"testing"
)

func TestSimpleUploaderHappyPath(t *testing.T) {

	uploader, err := getUploader("file")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	opt := map[string]string{}
	uri := "s3://amazon.aws.com/bucket/key.mp4"
	dtx := make(chan dlData)
	sdr := &mockSender{}

	// data provider goroutine
	go func() {
		buf := make([]byte, 256*1024) // 256KB file
		for i:=0; i<len(buf); i++ {
			buf[i] = 0xEF
		}
		dtx <- dlData{buf,true,int64(len(buf)),int64(len(buf))}
		close(dtx)
	}()

	uploadError := uploader.Upload(ctx, uri, opt, dtx, sdr)

	if uploadError != nil {
		t.Fatalf("expected Upload() to return '%v' error but got '%v'\n", nil, uploadError)
	}

	if len(sdr.buf) != 256*1024 {
		t.Fatalf("expected Upload() to write '%v' bytes but got '%v'\n", 256*1024, len(sdr.buf))
	}

	// check if content given matched content sent
	match := true
	for _, val := range sdr.buf {
		if val != 0xEF {
			match = false
			break
		}
	}
	if !match {
		t.Fatalf("invalid content")
	}
}
