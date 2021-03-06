package netio

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"sync"
)

func Transfer(ctx context.Context, srcUrl string, dstUrl string, options map[string]string) error {

	src, err := url.Parse(srcUrl)
	if err != nil {
		return fmt.Errorf("invalid srcUrl %v", err)
	}

	dst, err := url.Parse(dstUrl)
	if err != nil {
		return fmt.Errorf("invalid dstUrl %v", err)
	}

	info, err := probe(ctx, src.Scheme, srcUrl, options)
	if err != nil {
		return fmt.Errorf("can't collect src file info: %v", err)
	}

	dnl, err := getDownloader(src.Scheme)
	if err != nil {
		return fmt.Errorf("can't initialize downloader: %v", err)
	}

	upl, err := getUploader(dst.Scheme)
	if err != nil {
		return fmt.Errorf("can't initialize uploader: %v", err)
	}

	receiver, err := getReceiver(src.Scheme, info.Size)
	if err != nil {
		return fmt.Errorf("can't initialize receiver: %v", err)
	}

	sender, err := getSender(dst.Scheme, info.Size)
	if err != nil {
		return fmt.Errorf("can't initialize sender: %v", err)
	}

	options["contentLength"] = strconv.FormatInt(info.Size, 10)

	ioctx, cancel := context.WithCancel(ctx)
	defer cancel()

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
			}
		}
	}()

	wg.Add(1)
	go download(&wg, dnl, ioctx, srcUrl, options, datachan, commchan, receiver)

	wg.Add(1)
	go upload(&wg, upl, ioctx, dstUrl, options, datachan, commchan, sender)

	// wait until transfer complete or error
	wg.Wait()

	// to finish helper goroutine
	close(commchan)

	return transferError
}

func download(wg *sync.WaitGroup, dnl Downloader, ioctx context.Context, srcUrl string,
	options map[string]string, datachan chan dlData, commchan chan dlMessage, rc receiver) {

	defer wg.Done()
	if err := dnl.Download(ioctx, srcUrl, options, datachan, rc); err != nil {
		commchan <- dlMessage{"downloader", err }
	}
}

func upload(wg *sync.WaitGroup, upl Uploader, ioctx context.Context, dstUrl string,
	options map[string]string, datachan chan dlData, commchan chan dlMessage, snd sender) {

	defer wg.Done()
	if err := upl.Upload(ioctx, dstUrl, options, datachan, snd); err != nil {
		commchan <- dlMessage{"uploader", err }
	}
}

func probe(ctx context.Context, scheme string, uri string, options map[string]string) (FileInfo, error) {
	var info FileInfo
	dl, err := getProbeForScheme(scheme)
	if err != nil {
		return info, err
	}
	return dl.GetFileInfo(ctx, uri, options)
}
