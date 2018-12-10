package wikidump

import (
	"bufio"
	"compress/bzip2"
	"compress/gzip"
	"io"
	"os/exec"
	"syscall"

	"github.com/kjk/lzmadec"
	"github.com/pkg/errors"
)

func unGZip(ri virtualFile) (virtualFile, error) {
	ro, err := gzip.NewReader(ri)
	if err != nil {
		ri.Close()
		return virtualFile{}, err
	}
	return virtualFile{ro, func() error {
		err1 := errors.Wrapf(ro.Close(), "Error while closing gzip reader of file %v", ri.Name())
		err0 := ri.Close()
		if err1 != nil {
			return err1
		}
		return err0
	}, ri.Name()}, nil
}

func unBZip2(r virtualFile) (virtualFile, error) {
	return virtualFile{bzip2.NewReader(bufio.NewReader(r)), r.Close, r.Name()}, nil
}

func un7Zip(ri virtualFile) (ro virtualFile, err error) {
	fail := func(e error) (virtualFile, error) {
		ri.Close()
		ro, err = virtualFile{}, e
		return ro, err
	}

	fname := ri.Name()
	archive, err := lzmadec.NewArchive(fname)
	if err != nil {
		return fail(errors.Wrapf(err, "%v while listing content of file %v", lzmadecErr2Meaning(err), fname))
	}

	if len(archive.Entries) != 1 {
		return fail(errors.Errorf("Error entries count differs from one - %v - for file %v", len(archive.Entries), fname))
	}

	r, err := archive.GetFileReader(archive.Entries[0].Path)
	if err != nil {
		return fail(errors.Wrapf(err, "%v while opening file %v", lzmadecErr2Meaning(err), fname))
	}

	return virtualFile{r, func() error {
		err1 := errors.Wrapf(r.Close(), "Error while closing 7zip reader of file %v", fname)
		err0 := ri.Close()
		if err1 != nil {
			return err1
		}
		return err0
	}, ri.Name()}, nil
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

type virtualFile struct {
	io.Reader
	Closer func() error
	name   string
}

func (f virtualFile) Close() error {
	return f.Closer()
}

func (f virtualFile) Name() string {
	return f.name
}
