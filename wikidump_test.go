package wikidump

import (
	"testing"
)

func TestLatest(t *testing.T) {
	files := []string{"articlesmultistreamdump", "langlinkstable", "pagelinkstable", "imagetable", "iwlinkstable", "metacurrentdumprecombine", "usergroupstable", "flaggedpagestable", "xmlpagelogsdump", "changetagstable", "flaggedrevstable", "xmlpagelogsdumprecombine", "imagelinkstable", "pagetable", "articlesdumprecombine", "metacurrentdump", "pagepropstable", "metahistory7zdump", "sitestatstable", "articlesdump", "templatelinkstable", "geotagstable", "categorylinkstable", "pagerestrictionstable", "namespaces", "abstractsdumprecombine", "allpagetitlesdump", "abstractsdump", "xmlstubsdumprecombine", "pagetitlesdump", "externallinkstable", "xmlstubsdump", "categorytable", "wbcentityusagetable", "metahistorybz2dump", "sitestable", "redirecttable", "protectedtitlestable"}
	w, err := Latest("", "en", files...)
	if err != nil {
		t.Error("Latest returns ", err)
	}
	if err := w.CheckFor(files...); err != nil {
		t.Error("CheckFor on Latest returns ", err)
	}

}

func TestOpen(t *testing.T) {
	//TO DO with a local http fileserver
}
