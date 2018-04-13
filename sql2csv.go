package wikidump

import (
	"bufio"
	"bytes"
	"io"

	"github.com/pkg/errors"
)

//SQL2CSV transforms on the fly a SQL data dump from dumps.wikimedia.org into a clean CSV
func SQL2CSV(r io.Reader) io.Reader {
	return &_SQL2CSV{file: bufio.NewReader(r)}
}

type _SQL2CSV struct {
	file   *bufio.Reader
	buffer []byte
	err    error
}

func (r *_SQL2CSV) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return n, nil
	}
	//len(p)>0

	if len(r.buffer) == 0 && r.refill() != nil {
		return 0, r.err
	}
	//len(r.buffer)>0

	min := len(p)
	if len(r.buffer) < len(p) {
		min = len(r.buffer)
	}
	//m>0

	copy(p, r.buffer[:min])
	n, r.buffer = min, r.buffer[min:]

	return n, nil
}

func (r *_SQL2CSV) refill() (err error) {
	if r.err != nil {
		return r.err
	}

	defer func() {
		r.err = err
	}()

	b, rawBuffer, err := r.nextRawBuffer()
	if err != nil {
		return err
	}

	inString := false
	for i, c := range rawBuffer {
		switch {
		case !inString && bytes.HasSuffix(rawBuffer[:i+1], []byte("),(")):
			b = append(b[:len(b)-2], '\n')
		case c == '\'' && isEnabled(rawBuffer[:i]):
			b = append(b, '"')
			inString = !inString
		case c == '\'' /*&& !isEnabled(rawBuffer[:i])*/ :
			b = append(b[:len(b)-1], '\'')
		case c == '"' && isEnabled(rawBuffer[:i]):
			return errors.Errorf("SQL2CSV: error invalid \" in input.")
		case c == '"' /*&& !isEnabled(rawBuffer[:i])*/ :
			b = append(b[:len(b)-1], '"', '"')
		default:
			b = append(b, c)
		}
	}
	r.buffer = append(b, '\n')

	return nil
}

func isEnabled(b []byte) bool {
	count := 0
	for i := len(b) - 1; i >= 0 && b[i] == '\\'; i-- {
		count++
	}
	return count%2 == 0
}

func (r *_SQL2CSV) nextRawBuffer() (buffer, rawBuffer []byte, err error) {
	//fetch next line
	for !bytes.HasPrefix(rawBuffer, []byte("INSERT INTO")) && err == nil {
		rawBuffer, err = r.file.ReadBytes('\n')
	}

	if len(rawBuffer) == 0 {
		return nil, nil, err
	}

	buffer = rawBuffer[:0]
	begin := bytes.Index(rawBuffer, []byte("("))
	end := bytes.LastIndex(rawBuffer, []byte(")"))
	if begin == -1 || end == -1 || begin > end {
		return nil, nil, errors.Errorf("SQL2CSV: invalid input error.")
	}
	rawBuffer = rawBuffer[begin+1 : end]
	return
}
