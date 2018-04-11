package wikidump

import (
	"github.com/pkg/errors"
)

// Wikidump represent a hub from which request particular dump files of wikipedia.
type Wikidump struct {
	file2Info map[string][]fileInfo
}

type fileInfo struct {
	URL string
	//Sha1 string
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
