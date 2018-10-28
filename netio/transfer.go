package netio

import (
    "context"
    "fmt"
    "net/url"
	"sync"
)

func Transfer(ctx context.Context, srcUrl string, dstUrl string, options map[string]string) error {

    var src *url.URL
    var dst *url.URL

    if _, err := url.Parse(srcUrl); err != nil {
        return fmt.Errorf("invalid srcUrl %v", err)
    }

    if _, err := url.Parse(dstUrl); err != nil {
        return fmt.Errorf("invalid dstUrl %v", err)
    }

    dnl, err := getDownloaderForScheme(src.Scheme)
    if err != nil {
        return fmt.Errorf("can't initialize downloader: %v", err)
    }

    upl, err := getUploaderForScheme(dst.Scheme)
    if err != nil {
        return fmt.Errorf("can't initialize uploader: %v", err)
    }

    ioctx, cancel := context.WithCancel(ctx)

    datachan := make(chan dlData)
    commchan := make(chan dlMessage)

    var wg sync.WaitGroup
	var transferError error

	// helper goroutine reads errors from commchan;
    // call close(commchan) to finish it
    go func() {
		// for loop on chan will finish after channel is closed
		for msg := range commchan {
			if msg.err != nil {
				transferError = msg.err
				cancel()
				break
			}
		}
	}()
	defer close(commchan) // to finish helper goroutine in case close() is not called

    go download(&wg, dnl, ioctx, srcUrl, options, datachan, commchan)
	go upload(&wg, upl, ioctx, dstUrl, options, datachan, commchan)

	// wait until transfer complete or error
    wg.Wait()

    // close commchan to finish helper goroutine
    close(commchan)

    return transferError
}

func download(wg *sync.WaitGroup, dnl Downloader, ioctx context.Context, srcUrl string,
	options map[string]string, datachan chan dlData, commchan chan dlMessage) {

	wg.Add(1)
	dnl.Download(ioctx, srcUrl, options, datachan, commchan)
	wg.Done()
}

func upload(wg *sync.WaitGroup, upl Uploader, ioctx context.Context, dstUrl string,
	options map[string]string, datachan chan dlData, commchan chan dlMessage) {

	wg.Add(1)
	upl.Upload(ioctx, dstUrl, options, datachan, commchan)
	wg.Done()
}
