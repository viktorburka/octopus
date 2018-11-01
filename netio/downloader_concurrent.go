package netio

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

type DownloaderConcurrent struct {
}


func (s DownloaderConcurrent) Download(ctx context.Context, uri string,
	options map[string]string, data chan dlData, rc receiver) error {

	// helper min function that takes integer (Math.Min takes float)
	var min = func(v1 int64, v2 int64) int64 {
		if v1 <= v2 { return v1 }
		return v2
	}

	// helper function to detect last chunk
	var last = func(val int64) int64 {
		if val > 0 { return 1 }
		return 0
	}

	contentLength, err := strconv.ParseInt(options["contentLength"], 10, 64)
	if err != nil {
		return fmt.Errorf("error reading 'contentLength' value: %v", err)
	}

	// if contentLength is less than MinAwsPartSize,
	// to properly calculate parts count use min function
	partSize := min(int64(MinAwsPartSize), contentLength)

	if contentLength < partSize {
		err := fmt.Errorf("can't start ranged download: contentLength < partSize")
		return err
	}

	tempDir, err := ioutil.TempDir(os.TempDir(), "")
	if err != nil {
		return err
	}

	log.Println("open download connection")

	if err := rc.OpenWithContext(ctx, uri, options); err != nil {
		return err
	}

	const MaxWorkers = 3 // can't be 0!

	partschan := make(chan int)
	errorchan := make(chan error)
	workers   := make(chan struct{}, MaxWorkers) // channel as semaphore

	partsCount := contentLength/partSize+last(contentLength%partSize)

	var wg sync.WaitGroup

	log.Println("total download parts count:", partsCount)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(partschan)

		var wg2 sync.WaitGroup

		for partNumber := int64(0); partNumber < partsCount; partNumber++ {
			start := partNumber*partSize
			end   := min(start+partSize, contentLength)

			workers <- struct{}{}
			wg2.Add(1)
			go func(rangeStart int64, rangeEnd int64, curPart int64) {
				defer wg2.Done()
				defer func() {<-workers}()

				log.Println("start download part", curPart+1, "of", partsCount)

				fileName := fmt.Sprintf("%v.part", curPart)
				filePath := filepath.Join(tempDir, fileName)

				log.Println("open buffer file", fileName)

				file, err := os.Create(filePath)
				if err != nil {
					errorchan <- err
					return
				}
				defer file.Close()

				opt := map[string]string {
					"rangeStart": strconv.FormatInt(rangeStart,10),
					"rangeEnd":   strconv.FormatInt(rangeEnd,10),
					"partSize":   strconv.FormatInt(partSize,10),
				}

				log.Printf("start range download %v-%v\n", opt["rangeStart"], opt["rangeEnd"])

				_, err = rc.ReadPartWithContext(ctx, file, opt)
				if err != nil {
					errorchan <- err
					return
				}

				// done writing - close file
				if err := file.Close(); err != nil {
					errorchan <- err
					return
				}

				log.Printf("part %v download finished. passing to uploader\n", curPart+1)
				partschan <- int(curPart)

			}(start, end-1, partNumber)
		}

		wg2.Wait()
	}()

	var opErr error

	// this function sends the received parts to uploader
	wg.Add(1)
	go func() {
		defer wg.Done()

		parts := make([]bool, partsCount)
		ptr   := 0
		sent  := int64(0)

		for opErr == nil {
			select {
			case part, ok := <-partschan:
				if !ok {
					return
				}
				if part >= len(parts) { // prevent array index out of bound error
					opErr = fmt.Errorf("internal downloader error: part %v is out of range [0,%v)",
						part, len(parts))
					//cancel()
					break
				}
				parts[part] = true
				for ; ptr < len(parts); ptr++ {
					if !parts[ptr] {
						break
					}
					fileName := fmt.Sprintf("%v.part", ptr)
					filePath := filepath.Join(tempDir, fileName)

					log.Println("sending part", ptr+1, "to uploader")
					bw, err := writePart(ctx, filePath, sent, contentLength, data)
					if err != nil {
						opErr = err
						break
					}
					if err := os.Remove(filePath); err != nil {
						opErr = err
						break
					}
					sent += int64(bw)
				}
			case err := <-errorchan:
				opErr = err
				break
			case <-ctx.Done():
				opErr = ctx.Err()
				break
			}
		}
	}()

	wg.Wait()

	close(data)

	return opErr
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
			log.Println("sent", totalBytesSent, "bytes to uploader")
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
