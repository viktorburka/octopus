package netio

import (
    "context"
    "fmt"
    "net/url"
)

func Transfer(ctx context.Context, srcUrl string, dstUrl string, options map[string]string) error {

    var src *url.URL
    var dst *url.URL
    var err error

    if src, err = url.Parse(srcUrl); err != nil {
        return fmt.Errorf("invalid srcUrl %v", err)
    }

    if dst, err = url.Parse(dstUrl); err != nil {
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

    go dnl.Download(ioctx, srcUrl, options, datachan, commchan)
    go upl.Upload(ioctx, dstUrl, options, datachan, commchan)

    // wait until transfer complete
    for {
        msg := <-commchan
        if msg.err != nil {
            cancel()
            return msg.err
        }
        if msg.sender == "uploader" { // upload part is done
            break
        }
    }

    return nil
}
