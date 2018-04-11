package wikidump

import "io"

type readClose struct {
	io.Reader
	Closer func() error
}

func (r readClose) Close() error {
	return r.Closer()
}

type errorRReader struct {
	err error
}

func (r errorRReader) Read() (record []string, err error) {
	if r.err == nil {
		r.err = io.ErrUnexpectedEOF
	}
	return nil, r.err
}

func (r errorRReader) Close() error {
	if r.err == nil {
		r.err = io.ErrUnexpectedEOF
	}
	return r.err
}
