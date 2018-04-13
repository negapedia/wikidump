package wikidump

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
)

func TestUnit(t *testing.T) {
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

	if _, err = next(context.Background()); err != io.EOF {
		t.Error("Open should return only one file for "+testingFilename+" while it returns ", err)
	}
}

func TestOpen(t *testing.T) {
	ffi := make([]fileInfo, 0, len(name2MyInfo))
	for name, info := range name2MyInfo {
		ffi = append(ffi, fileInfo{"http://" + address + name, info.SHA1})
	}
	tDump := Wikidump{map[string][]fileInfo{"helloword": ffi}, ""}
	next := tDump.Open("helloword")
	r, err := next(context.Background())
	for ; err == nil; r, err = next(context.Background()) {
		defer r.Close()
		data, err := ioutil.ReadAll(r)
		switch {
		case err != nil:
			t.Error("Open iterator returns ", err)
		case string(data) != helloword:
			t.Error("Data should be " + helloword + " but it's " + string(data))
		}
	}
	if err != nil && err != io.EOF {
		t.Error("Open iterator returns ", err)
	}
	next = tDump.Open("nothing")
	if _, err := next(context.Background()); err == nil {
		t.Error("Error should be not null")
	}
}

const helloword = "Hello, World!"
const address = ":8080"

var name2MyInfo = map[string]myInfo{
	"/helloword.7z": base642MyInfo("N3q8ryccAAT5z0JlEQAAAAAAAABqAAAAAAAAACkoIPIBAAxIZWxsbywgV29ybGQhAAEEBgABCREA" +
		"BwsBAAEhIQEADA0ACAoB0MNK7AAABQEZDAAAAAAAAAAAAAAAABEfAGgAZQBsAGwAbwB3AG8AcgBs" +
		"AGQALgB0AHgAdAAAABkEAAAAABQKAQCAOPxYCNPTARUGAQAggKSBAAA="),
	"/helloword.bz2": base642MyInfo("QlpoOTFBWSZTWebY/t8AAAGXgGAEAEAAgAYEkAAgACIDIyEAMLKAWt5D7xdyRThQkObY/t8="),
	"/helloword.gz":  base642MyInfo("H4sICNV10FoAA2hlbGxvd29ybGQudHh0APNIzcnJ11EIzy/KSVEEANDDSuwNAAAA"),
}

type myInfo struct {
	Data []byte
	SHA1 string
}

func base642MyInfo(s string) myInfo {
	data, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return myInfo{data, fmt.Sprintf("%x", sha1.Sum(data))}
}

func TestMain(m *testing.M) {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write(name2MyInfo[r.URL.Path].Data)
	})
	go func() {
		err := http.ListenAndServe(address, nil)
		if err != nil {
			panic(err)
		}
	}()

	os.Exit(m.Run())
}
