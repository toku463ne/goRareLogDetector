package rarelogdetector

import (
	"goRareLogDetector/pkg/utils"
	"testing"
)

func Test_tokenizeLine(t *testing.T) {
	var err error
	cnt := -1

	// openvpn log
	pattern := `^(?P<timestamp>\w+ \d+ \d+:\d+:\d+) (?P<host_ip>\d+\.\w+\.\w+\.\d+) openvpn\[\d+\]: (?P<source_ip>\d+\.\w+\.\w+\.\d+):\d+ (?P<message>.+)$`
	dateFormat := "Jan 2 15:04:05"
	tr, err := newTrans("", pattern, dateFormat, 0, 1000, false, false)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	line := "Jul 31 20:24:33 192.168.67.51 openvpn[12781]: 125.30.90.192:1194 peer info: IV_LZ4=1"
	if cnt, err = tr.tokenizeLine(line, 0, false, false); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := utils.GetGotExpErr("item count", cnt, 1); err != nil {
		t.Errorf("%v", err)
		return
	}

	itemID := tr.terms.getItemID("peer")
	lastUpdate := tr.terms.lastUpdates[itemID]

	dt, err := utils.Str2date(dateFormat, "Jul 31 20:24:33")
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	epoch := dt.Unix()

	if err := utils.GetGotExpErr("openvpn timestamp", lastUpdate, epoch); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := utils.GetGotExpErr("openvpn message", tr.lastMessage, "peer info: IV_LZ4=1"); err != nil {
		t.Errorf("%v", err)
		return
	}

	// nginx log
	pattern = `^(?P<timestamp>.+)\+09\:00 from:"(.*)" user:"(.*)" via:"(.*)" to:"(.*):\d+" (?P<message>.+)$`
	dateFormat = "2006-01-02T15:04:05"
	tr, err = newTrans("", pattern, dateFormat, 0, 1000, false, false)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	line = `2024-08-01T21:51:08+09:00 from:"x.x.x.11" user:"-" via:"node01:8081" to:"x.x.x.41:81" r:"GET / HTTP/1.1" st:"401" srv:"qaapi.test.com"`
	if cnt, err = tr.tokenizeLine(line, 0, false, false); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := utils.GetGotExpErr("item count", cnt, 1); err != nil {
		t.Errorf("%v", err)
		return
	}

	itemID = tr.terms.getItemID("http")
	lastUpdate = tr.terms.lastUpdates[itemID]

	dt, err = utils.Str2date(dateFormat, "2024-08-01T21:51:08")
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	epoch = dt.Unix()

	if err := utils.GetGotExpErr("nginx timestamp", lastUpdate, epoch); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := utils.GetGotExpErr("nginx message", tr.lastMessage, `r:"GET / HTTP/1.1" st:"401" srv:"qaapi.test.com"`); err != nil {
		t.Errorf("%v", err)
		return
	}

	// other
	tr, err = newTrans("", "", "", 0, 1000, false, false)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	line = "Jul 31 20:24:33 192.168.67.51 openvpn[12781]: 125.30.90.192:1194 peer info: IV_LZ4=1"
	if cnt, err = tr.tokenizeLine(line, 0, false, false); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := utils.GetGotExpErr("item count", cnt, 1); err != nil {
		t.Errorf("%v", err)
		return
	}

	//itemID = tr.terms.getItemID("openvpn")
	if err := utils.GetGotExpErr("other message", tr.lastMessage, `Jul 31 20:24:33 192.168.67.51 openvpn[12781]: 125.30.90.192:1194 peer info: IV_LZ4=1`); err != nil {
		t.Errorf("%v", err)
		return
	}
}
