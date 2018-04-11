package wikidump

import (
	"testing"
)

func TestLatest(t *testing.T) {
	w, err := Latest("en", "metahistory7zdump")
	if err != nil {
		t.Error("Latest returns ", err)
	}
	if len(w.file2Info) == 0 {
		t.Error("Latest returns an empty file index")
	}
}
