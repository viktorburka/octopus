package netio

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

type UploaderConcurrent struct {
}


func (s UploaderConcurrent) Upload(ctx context.Context, uri string, options map[string]string,
	data chan dlData, msg chan dlMessage, snd sender) {

	tempDir, err := ioutil.TempDir(os.TempDir(), "")
	if err != nil {
		msg <- dlMessage{sender: "uploader", err: err}
		return
	}
	defer os.RemoveAll(tempDir)

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

	if err := os.RemoveAll(tempDir); err != nil {
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


