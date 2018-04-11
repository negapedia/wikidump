// Package wikidump provides primitives for get the latest wikipedia dump
package wikidump

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"time"

	"github.com/pkg/errors"
)

// Latest creates a new wikidump from the latest valid wikipedia dump.
func Latest(lang string, checkFor ...string) (w Wikidump, err error) {
	dates, err := dumpDates(lang)
	if err != nil {
		return
	}

	for i := len(dates) - 1; i >= 0; i-- {
		w, err = From(lang, dates[i])
		if err == nil && w.CheckFor(checkFor...) == nil {
			return
		}
	}
	return
}

// From creates a new wikidump from the specified date.
func From(lang string, t time.Time) (w Wikidump, err error) {
	fail := func(e error) (Wikidump, error) {
		w, err = Wikidump{}, e
		return w, err
	}

	indexURL := fmt.Sprintf("https://dumps.wikimedia.org/%vwiki/%v/dumpstatus.json", lang, t.Format("20060102"))
	resp, err := http.Get(indexURL)
	if err != nil {
		return fail(errors.Wrap(err, "Error: unable to get page: "+indexURL))
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fail(errors.Wrap(err, "Error: unable to read all the page: "+indexURL))
	}

	var data struct {
		Jobs map[string]struct {
			Status string
			Files  map[string]fileInfo
		}
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return fail(errors.Wrap(err, "Error: unable to Unmarshal the JSON in the page: "+indexURL))
	}

	w.file2Info = make(map[string][]fileInfo, len(data.Jobs))
	for file, statusFiles := range data.Jobs {
		if statusFiles.Status != "done" {
			continue
		}

		infos := make([]fileInfo, 0, len(statusFiles.Files))
		for _, info := range statusFiles.Files {
			info.URL = "https://dumps.wikimedia.org" + info.URL
			infos = append(infos, info)
		}
		w.file2Info[file] = infos
	}
	return
}

func dumpDates(lang string) (dates []time.Time, err error) {
	fail := func(e error) ([]time.Time, error) {
		dates, err = nil, e
		return nil, e
	}
	nameExp := regexp.MustCompile(`<a href="(\d+)/">[^\n]+\n`)
	indexURL := fmt.Sprintf("https://dumps.wikimedia.org/%vwiki/", lang)
	resp, err := http.Get(indexURL)
	if err != nil {
		return fail(errors.Wrap(err, "Error: unable to get page: "+indexURL))
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fail(errors.Wrap(err, "Error: unable to read all the page: "+indexURL))
	}
	bodyString := string(body)

	for _, m := range nameExp.FindAllStringSubmatch(bodyString, -1) {
		t, err := time.Parse("20060102", m[1])
		if err != nil {
			return fail(errors.Wrap(err, "Error: unable to parse date: "+m[1]))
		}
		dates = append(dates, t)
	}
	return
}
