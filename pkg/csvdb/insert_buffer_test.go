package csvdb

import (
	"goRareLogDetector/pkg/utils"
	"strconv"
	"testing"
)

func TestCsvTableInsertBuffer(t *testing.T) {
	regData := func(b *insertBuff, cnt int) int {
		for i := 0; i < cnt; i++ {
			if b.register([]string{strconv.Itoa(i)}) {
				return i
			}
		}
		return b.pos
	}

	b := newInsertBuffer(3)
	data := [][]string{
		{"test1", "sest1", "usest1"},
		{"test2", "sest2", "usest2"},
	}

	for _, row := range data {
		b.register(row)
	}

	if err := utils.GetGotExpErr("not full", b.isFull, false); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := utils.GetGotExpErr("full",
		b.register([]string{"test3", "sest3", "usest3"}), true); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := utils.GetGotExpErr("full",
		b.register([]string{"test4", "sest4", "usest4"}), true); err != nil {
		t.Errorf("%v", err)
		return
	}

	b = newInsertBuffer(0)
	if err := utils.GetGotExpErr("10000 recs",
		regData(b, 10000), 9999); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := utils.GetGotExpErr("plus 10000 recs",
		regData(b, 10000), 19999); err != nil {
		t.Errorf("%v", err)
		return
	}
}
