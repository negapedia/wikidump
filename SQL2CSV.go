package wikidump

import (
	"bufio"
	"bytes"
	"io"

	"github.com/pkg/errors"
)

//SQL2CSV transforms on the fly a SQL data dump from dumps.wikimedia.org into a clean CSV
func SQL2CSV(file io.Reader) io.Reader {
	return &_SQL2CSV{file: bufio.NewReader(file)}
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

	var buffer []byte
	for !bytes.HasPrefix(buffer, []byte("INSERT INTO")) && err == nil {
		buffer, err = r.file.ReadBytes('\n')
	}

	if len(buffer) == 0 {
		return err
	}

	b := buffer[:0]
	begin := bytes.Index(buffer, []byte("("))
	end := bytes.LastIndex(buffer, []byte(")"))
	if begin == -1 || end == -1 || begin > end {
		return errors.Errorf("_SQL2CSV: invalid input error.")
	}
	buffer = buffer[begin+1 : end]
	inString := false
	for i, c := range buffer {
		switch {
		case !inString && bytes.HasSuffix(buffer[:i+1], []byte("),(")):
			b = append(b[:len(b)-2], '\n')
		case c == '\'' && isEnabled(buffer[:i]):
			b = append(b, '"')
			inString = !inString
		case c == '\'' /*&& !isEnabled(buffer[:i])*/ :
			b = append(b[:len(b)-1], '\'')
		case c == '"' && isEnabled(buffer[:i]):
			return errors.Errorf("_SQL2CSV: error invalid \" in input.")
		case c == '"' /*&& !isEnabled(buffer[:i])*/ :
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
