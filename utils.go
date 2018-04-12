package wikidump

import (
	"bufio"
	"compress/bzip2"
	"compress/gzip"
	"io"
	"os"
	"os/exec"
	"syscall"

	"github.com/kjk/lzmadec"
	"github.com/pkg/errors"
)

func unGZip(ri io.ReadCloser) (io.ReadCloser, error) {
	ro, err := gzip.NewReader(ri)
	if err != nil {
		ri.Close()
		return nil, err
	}
	return readClose{ro, func() error {
		err1 := ro.Close()
		err0 := ri.Close()
		if err1 != nil {
			return err1
		}
		return err0
	}}, nil
}

func unBZip2(r io.ReadCloser) (io.ReadCloser, error) {
	return readClose{bzip2.NewReader(bufio.NewReader(r)), r.Close}, nil
}

func un7Zip(ri io.ReadCloser) (ro io.ReadCloser, err error) {
	f, ok := ri.(*os.File) //Hackity hackity - it doesn't exist a fully proofed golang version
	if !ok {
		err = errors.New("Unable to reach the underlying file")
		return
	}
	f.Close()
	archive, err := lzmadec.NewArchive(f.Name())
	if err != nil {
		err = errors.Wrapf(err, "%v while listing content of file %v", lzmadecErr2Meaning(err), f.Name())
		return
	}

	if len(archive.Entries) != 1 {
		err = errors.Errorf("Error entries count differs from one - %v - for file %v", len(archive.Entries), f.Name())
		return
	}

	ro, err = archive.GetFileReader(archive.Entries[0].Path)
	if err != nil {
		err = errors.Wrapf(err, "%v while opening file %v", lzmadecErr2Meaning(err), f.Name())
	}
	return
}

func lzmadecErr2Meaning(err error) (defaultM string) {
	if err == nil {
		return
	}

	defaultM = "Error"

	exiterr, ok := err.(*exec.ExitError)
	if !ok {
		return
	}

	// This works on both Unix and Windows. Although package
	// syscall is generally platform dependent, WaitStatus is
	// defined for both Unix and Windows and in both cases has
	// an ExitStatus() method with the same signature.
	status, ok := exiterr.Sys().(syscall.WaitStatus)
	if !ok {
		return
	}

	m, ok := code2Meaning[status.ExitStatus()]
	if !ok {
		return
	}

	return m
}

var code2Meaning = map[int]string{
	1:   "Warning",
	2:   "Fatal error",
	3:   "Change identified",
	7:   "Command line error",
	8:   "Not enough memory for operation",
	255: "User stopped the process",
}

type readClose struct {
	io.Reader
	Closer func() error
}

func (r readClose) Close() error {
	return r.Closer()
}
