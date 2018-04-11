package wikidump

import (
	"context"
	"crypto/sha1"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/pkg/errors"
)

// Wikidump represent a hub from which request particular dump files of wikipedia.
type Wikidump struct {
	file2Info map[string][]fileInfo
	tmpDir    string
}

type fileInfo struct {
	URL  string
	SHA1 string
}

//CheckFor checks for file existence in the wikidump
func (w Wikidump) CheckFor(files ...string) error {
	for _, file := range files {
		if _, ok := w.file2Info[file]; !ok {
			return errors.New(file + " not found")
		}
	}
	return nil
}

//Open open a dump ... TODO
/*func (w Wikidump) Open(ctx context.Context, file string) <-chan io.ReadCloser {
	fi, err := w.file2Info[file], w.CheckFor(file)
	result := make(chan io.ReadCloser, len(fi)+1)
	switch {
	case err != nil:
		//Do nothing
	case len(fi) == 1:
		var f io.ReadCloser
		f, err = download(ctx, fi[0].URL)
		if err == nil {
			result <- f
			close(result)
		}
	default: //multifile case: download files sequentially
		go w.multiDownload(ctx, fi, result)
	}

	if err != nil {
		result <- makeerrorRReader{err}
		close(result)
	}

	return result
}*/

func (w Wikidump) stubbornStore(ctx context.Context, fi fileInfo) (f io.ReadCloser, err error) {
	for t := time.Second; t < time.Hour; t = t * 2 { //exponential backoff
		if f, err = w.store(ctx, fi); err == nil {
			return
		}
		select {
		case <-ctx.Done():
			return nil, errors.Wrap(ctx.Err(), "Error: change in context state")
		case <-time.After(t):
			//do nothing
		}
	}
	return
}

func (w Wikidump) store(ctx context.Context, fi fileInfo) (f io.ReadCloser, err error) {
	tempFile, err := ioutil.TempFile(w.tmpDir, path.Base(fi.URL))
	if err != nil {
		return nil, errors.Wrap(err, "Error: unable to create temporary file in "+w.tmpDir)
	}
	defer func() {
		if err != nil {
			os.Remove(tempFile.Name())
		}
	}()

	body, err := stream(ctx, fi)
	if err != nil {
		return
	}
	defer body.Close()

	hash := sha1.New()
	_, err = io.Copy(io.MultiWriter(tempFile, hash), body)
	if err != nil {
		return nil, errors.Wrap(err, "Error: unable to copy to file the following url: "+fi.URL)
	}

	if string(hash.Sum(nil)) != fi.SHA1 {
		return nil, errors.New("Error: mismatched SHA1 for the file downloaded from the following url: " + fi.URL)
	}

	return readClose{tempFile, func() error {
		err1 := tempFile.Close()
		err0 := os.Remove(tempFile.Name())
		if err1 != nil {
			return err1
		}
		return err0
	}}, nil
}

func stream(ctx context.Context, fi fileInfo) (f io.ReadCloser, err error) {
	req, err := http.NewRequest("GET", fi.URL, nil)
	if err != nil {
		err = errors.Wrap(err, "Error: unable create a request with the following url: "+fi.URL)
		return
	}

	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		err = errors.Wrap(err, "Error: unable do a request with the following url: "+fi.URL)
		return
	}

	f = resp.Body
	return
}
