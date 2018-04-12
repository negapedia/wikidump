// Package wikidump provides utility functions for downloading and extracting wikipedia dumps.
package wikidump

import (
	"context"
	"crypto/sha1"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// Wikidump represent a hub from which request particular dump files of wikipedia.
type Wikidump struct {
	file2Info map[string][]fileInfo
	tmpDir    string
}

type fileInfo struct {
	URL, SHA1 string
}

//CheckFor checks for file existence in the wikidump
func (w Wikidump) CheckFor(filenames ...string) error {
	for _, filename := range filenames {
		if _, ok := w.file2Info[filename]; !ok {
			return errors.New(filename + " not found")
		}
	}
	return nil
}

//Open return an iterator over the resources associated with the current filename,
//the download can be stopped by the context. Once the iterator is depleted, it returns an io.EOF error.
//Once an error is returned by the iterator, any subsequent call will return the same error.
//Open take care of decompressing files on the fly.
//It is the caller's responsibility to call Close on the Reader when done.
func (w Wikidump) Open(filename string) func(context.Context) (io.ReadCloser, error) {
	ffi, err := w.file2Info[filename], w.CheckFor(filename)
	return func(ctx context.Context) (io.ReadCloser, error) {
		if err != nil {
			return nil, err
		}
		if len(ffi) == 0 {
			err = io.EOF
			return nil, err
		}
		var r io.ReadCloser
		r, err = w.open(ctx, ffi[0])
		ffi = ffi[1:]
		return r, err
	}
}

func (w Wikidump) open(ctx context.Context, fi fileInfo) (r io.ReadCloser, err error) {
	r, err = w.stubbornStore(ctx, fi)
	switch {
	case err != nil:
		//do nothing
	case strings.HasSuffix(fi.URL, ".7z"):
		r, err = un7Zip(r)
	case strings.HasSuffix(fi.URL, ".bz2"):
		r, err = unBZip2(r)
	case strings.HasSuffix(fi.URL, ".gz"):
		r, err = unGZip(r)
	}

	return
}

func (w Wikidump) stubbornStore(ctx context.Context, fi fileInfo) (r io.ReadCloser, err error) {
	for t := time.Second; t < time.Hour; t = t * 2 { //exponential backoff
		if r, err = w.store(ctx, fi); err == nil {
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

func (w Wikidump) store(ctx context.Context, fi fileInfo) (r io.ReadCloser, err error) {
	tempFile, err := ioutil.TempFile(w.tmpDir, path.Base(fi.URL))
	if err != nil {
		return nil, errors.Wrap(err, "Error: unable to create temporary file in "+w.tmpDir)
	}
	fclose := func() error {
		err1 := tempFile.Close()
		err0 := os.Remove(tempFile.Name())
		if err1 != nil {
			return err1
		}
		return err0
	}
	fail := func(e error) (io.ReadCloser, error) {
		fclose()
		r, err = nil, e
		return r, err
	}

	body, err := stream(ctx, fi)
	if err != nil {
		return fail(err)
	}
	defer body.Close()

	hash := sha1.New()
	_, err = io.Copy(io.MultiWriter(tempFile, hash), body)
	if err != nil {
		return fail(errors.Wrap(err, "Error: unable to copy to file the following url: "+fi.URL))
	}

	if fmt.Sprintf("%x", hash.Sum(nil)) != fi.SHA1 {
		return fail(errors.New("Error: mismatched SHA1 for the file downloaded from the following url: " + fi.URL))
	}

	tempFile.Seek(0, 0)
	return readClose{tempFile, fclose}, nil
}

func stream(ctx context.Context, fi fileInfo) (r io.ReadCloser, err error) {
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

	r = resp.Body
	return
}
