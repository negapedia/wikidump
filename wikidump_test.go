package wikidump

import (
	"context"
	"encoding/csv"
	"io"
	"testing"
)

func TestUnity(t *testing.T) {
	testingFilename := "usergroupstable"
	w, err := Latest("", "en", testingFilename)
	if err != nil {
		t.Error("Latest returns ", err)
	}
	next := w.Open(testingFilename)
	r, err := next(context.Background())
	if err != nil {
		t.Error("Open iterator returns ", err)
	}
	defer r.Close()

	_, err = csv.NewReader(SQL2CSV(r)).ReadAll()
	if err != nil {
		t.Error("ReadAll on csv returns ", err)
	}

	if r, err = next(context.Background()); err != io.EOF {
		t.Error("Open should return only one file for "+testingFilename+" while it returns ", err)
	}
}
