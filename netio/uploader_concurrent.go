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

	log.Println("open upload connection")

	if err := snd.OpenWithContext(ctx, uri, options); err != nil {
		return err
	}

	const MaxWorkers = 5 // can't be 0 !

	errchan := make(chan error)
	workers := make(chan struct{}, MaxWorkers) // channel as semaphore

	isLastChunk := false

	var opErr error
	var wg    sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		var counter int64 = 1
		var total int64
		var fpath string
		var file  *os.File

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
					go uploadPart(ctx, fpath, counter, errchan, &wg, workers, snd)
					total = 0
					counter += 1
				}

				if isLastChunk {
					log.Println("last chunk is being uploaded")
					break
				}

			case err := <-errchan: // multipart upload error
				opErr = err
				break

			case <-ctx.Done(): // there is cancellation
				opErr = ctx.Err()
				break
			}
		}
	}()

	log.Println("waiting for all parts upload to finish")

	// make sure all goroutines are finished
	wg.Wait()

	log.Println("finished uploading all parts. operationError =", opErr)

	if opErr != nil {
		log.Println("upload interrupted because of error. cancelling upload")
		if err := snd.CancelWithContext(ctx); err != nil {
			return fmt.Errorf("%v: error while cancelling upload: %v", opErr, err)
		}
		return opErr
	}

	log.Println("close upload connection")

	if err := snd.CloseWithContext(ctx); err != nil {
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


