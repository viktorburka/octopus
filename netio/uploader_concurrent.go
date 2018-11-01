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


func (s UploaderConcurrent) Upload(ctx context.Context, uri string,
	options map[string]string, data chan dlData, snd sender) error {

	tempDir, err := ioutil.TempDir(os.TempDir(), "")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)

	uplCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	log.Println("open upload connection")

	if err := snd.OpenWithContext(uplCtx, uri, options); err != nil {
		return err
	}

	const MaxWorkers = 5 // can't be 0 !

	errchan := make(chan error)
	workers := make(chan struct{}, MaxWorkers) // channel as semaphore

	isLastChunk := false

	var counter int64 = 1
	var total int64
	var fpath string
	var file  *os.File
	var opErr error
	var partUploadErr error
	var wg sync.WaitGroup
	var wg2 sync.WaitGroup

	wg2.Add(1)
	go func() {
		defer wg2.Done()
		for err := range errchan {
			partUploadErr = err
			cancel()
			break
		}
	}()

	for !isLastChunk && opErr == nil {
		select {
		case chunk, ok := <-data:

			if file == nil {
				// build part file name
				fileName := fmt.Sprintf("%v.part", counter)
				fpath = filepath.Join(tempDir, fileName)
				// create part file
				log.Println("create buffer file:", fileName)
				file, err = os.Create(fpath)
				if err != nil {
					opErr = err
					break
				}
			}

			isLastChunk = !ok || chunk.done // channel closed and no data left

			if len(chunk.data) > 0 {
				bw, err := file.Write(chunk.data)
				if err != nil {
					opErr = err
					break
				}
				total += int64(bw)
			}

			if total >= MinAwsPartSize || isLastChunk { // ready to start upload
				// close buffer file
				if err := file.Close(); err != nil {
					opErr = err
					break
				}
				file = nil

				workers <- struct{}{}
				wg.Add(1)
				go uploadPart(uplCtx, fpath, counter, errchan, &wg, workers, snd)
				total = 0
				counter += 1
			}

			if isLastChunk {
				log.Println("last chunk is being uploaded")
				break
			}

		case <-uplCtx.Done(): // there is cancellation
			opErr = uplCtx.Err()
			break
		}
	}

	log.Println("waiting for all parts upload to finish")

	// make sure all goroutines are finished
	wg.Wait()

	// close errchan to finish helper goroutine
	close(errchan)

	// wait until helper goroutine finishes
	wg2.Wait()

	log.Println("finished uploading all parts. opErr =", opErr, "partUploadErr =", partUploadErr)

	if partUploadErr != nil || opErr != nil {
		errMsg := ""
		if partUploadErr != nil {
			errMsg += partUploadErr.Error()
		}
		if opErr != nil {
			if partUploadErr != nil {
				errMsg += ": "
			}
			errMsg += opErr.Error()
		}

		log.Println("upload interrupted because of error. cancelling upload")
		if err := snd.CancelWithContext(uplCtx); err != nil {
			return fmt.Errorf("%v: error while cancelling upload: %v", errMsg, err)
		}

		return fmt.Errorf(errMsg)
	}

	log.Println("close upload connection")

	if err := snd.CloseWithContext(uplCtx); err != nil {
		return err
	}

	log.Println("remove buffer files and folder")

	if err := os.RemoveAll(tempDir); err != nil {
		return fmt.Errorf("error deleting temp folder: %v", err)
	}

	log.Println("upload done")
	return nil
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
		log.Println("write to errchan")
		errchan <- err
		log.Println("after write to errchan")
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


