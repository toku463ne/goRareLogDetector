package rarelogdetector

import (
	"math"
	"sort"

	"goRareLogDetector/pkg/csvdb"

	"github.com/pkg/errors"
)

type items struct {
	*csvdb.CircuitDB
	name             string
	maxItemID        int
	members          map[string]int
	memberMap        map[int]string
	counts           map[int]int
	createEpochs     map[int]int64
	lastUpdates      map[int]int64
	lastUpdate       int64
	lastValues       map[int]string
	currCounts       map[int]int
	currUpdates      map[int]int64
	currCreateEpochs map[int]int64
	currItemCount    int
	totalCount       int
}

func newItems(dataDir, name string, maxBlocks int,
	retention int64, frequency string, useGzip bool) (*items, error) {
	i := new(items)
	// Now: maxRowsInBlock=0 TODO: support rotation and change this
	d, err := csvdb.NewCircuitDB(dataDir, name, tableDefs["items"], maxBlocks, 0, retention, frequency, useGzip)
	if err != nil {
		return nil, err
	}
	i.CircuitDB = d
	i.name = name
	i.memberMap = make(map[int]string, 10000)
	i.counts = make(map[int]int, 10000)
	i.members = make(map[string]int, 10000)
	i.currCounts = make(map[int]int, 10000)
	i.lastUpdates = make(map[int]int64, 10000)
	i.createEpochs = make(map[int]int64, 10000)
	i.currUpdates = make(map[int]int64, 10000)
	i.currCreateEpochs = make(map[int]int64, 10000)
	i.lastValues = make(map[int]string, 10000)
	i.maxItemID = 0

	return i, nil
}

func (i *items) load() error {
	if i.DataDir != "" {
		if err := i.loadDB(); err != nil {
			return err
		}
	}
	return nil
}

func (i *items) register(item string, addCount int,
	createEpoch, lastUpdate int64,
	lastValue string, isNew bool) int {
	if item == "" {
		return -1
	}
	itemID, ok := i.members[item]
	if ok {
		if lastUpdate > i.lastUpdates[itemID] {
			i.lastUpdates[itemID] = lastUpdate
		}
		if createEpoch < i.createEpochs[itemID] {
			i.createEpochs[itemID] = createEpoch
		}

	} else {
		i.maxItemID++
		itemID = i.maxItemID
		i.members[item] = itemID
		i.memberMap[itemID] = item
		i.lastUpdates[itemID] = lastUpdate
		i.createEpochs[itemID] = createEpoch

		if isNew {
			i.currItemCount++
		}
	}

	if lastUpdate > i.lastUpdate {
		i.lastUpdate = lastUpdate
	}

	if isNew {
		_, ok = i.currUpdates[itemID]
		if ok {
			if lastUpdate > i.currUpdates[itemID] {
				i.currUpdates[itemID] = lastUpdate
			}
			if createEpoch > 0 && createEpoch < i.currCreateEpochs[itemID] {
				i.currCreateEpochs[itemID] = createEpoch
			}
		} else {
			i.currUpdates[itemID] = lastUpdate
			i.currCreateEpochs[itemID] = createEpoch
		}
	}

	if addCount == 0 {
		return itemID
	}

	i.lastValues[itemID] = lastValue

	i.counts[itemID] += addCount
	if isNew {
		i.currCounts[itemID] += addCount
	}
	i.totalCount += addCount
	return itemID
}

func (i *items) getMember(itemID int) string {
	if itemID < 0 {
		return "*"
	}
	return i.memberMap[itemID]
}

func (i *items) getCreateEpoch(itemID int) int64 {
	if itemID < 0 {
		return 0
	}
	return i.createEpochs[itemID]
}

func (i *items) getLastUpdate(itemID int) int64 {
	if itemID < 0 {
		return 0
	}
	return i.lastUpdates[itemID]
}

func (i *items) getLastValue(itemID int) string {
	if itemID < 0 {
		return ""
	}
	return i.lastValues[itemID]
}

func (i *items) getIdf(itemID int) float64 {
	if i.totalCount == 0 {
		return 0
	}
	count := i.counts[itemID]
	if count == 0 {
		return 0
	}
	score := math.Log(float64(i.totalCount)/float64(count)) + 1
	return score
}

func (i *items) getCount(itemID int) int {
	return i.counts[itemID]
}

func (i *items) getItemID(term string) int {
	itemID, ok := i.members[term]
	if !ok {
		return -1
	}
	return itemID
}

func (i *items) clearCurrCount() {
	i.currCounts = make(map[int]int, 10000)
	i.currUpdates = make(map[int]int64, 10000)
	i.currCreateEpochs = make(map[int]int64, 10000)
	i.currItemCount = 0
}

func (i *items) loadDB() error {
	if i.DataDir == "" {
		return nil
	}
	cnt := i.CountFromStatusTable(nil)
	if cnt <= 0 {
		return nil
	}

	if err := i.LoadCircuitDBStatus(); err != nil {
		return err
	}

	rows, err := i.SelectRows(nil, nil, tableDefs["items"])
	if err != nil {
		return err
	}
	if rows == nil {
		return nil
	}

	for rows.Next() {
		var item string
		var itemCount int
		var createEpoch int64
		var lastUpdate int64
		var lastValue string
		err = rows.Scan(&itemCount, &createEpoch, &lastUpdate, &item, &lastValue)
		if err != nil {
			return err
		}
		i.register(item, itemCount, createEpoch, lastUpdate, lastValue, !rows.BlockCompleted)
	}
	return nil
}

// expected to be called from trans.go
func (i *items) next() error {
	//i.RowNo++
	if err := i.flush(); err != nil {
		return err
	}

	i.clearCurrCount()
	i.NextBlock(i.lastUpdate)

	// in case the block table already exists and will be overrided
	// we subtract counts in the block table from total item counts
	rows, err := i.SelectFromCurrentTable(nil, tableDefs["items"])
	if err != nil {
		return err
	}
	if rows == nil {
		return nil
	}

	for rows.Next() {
		var item string
		var itemCount int
		var createEpoch int64
		var lastUpdate int64
		var lastValue string
		err = rows.Scan(&itemCount, &createEpoch, &lastUpdate, &item, &lastValue)
		if err != nil {
			return err
		}
		itemID := i.getItemID(item)
		i.counts[itemID] -= itemCount
		i.currCreateEpochs[itemID] = createEpoch
		i.currUpdates[itemID] = lastUpdate
		if lastUpdate > i.lastUpdates[itemID] {
			i.lastUpdates[itemID] = lastUpdate
		}
		if createEpoch > 0 && createEpoch < i.createEpochs[itemID] {
			i.createEpochs[itemID] = createEpoch
		}
		i.lastValues[itemID] = lastValue
	}

	return nil
}

func (i *items) commit(completed bool) error {
	if i.DataDir == "" {
		return nil
	}
	if err := i.flush(); err != nil {
		return err
	}
	if err := i.UpdateBlockStatus(completed); err != nil {
		return err
	}
	return nil
}

func (i *items) flush() error {
	if i.DataDir == "" {
		return nil
	}
	for itemID, cnt := range i.currCounts {
		if cnt <= 0 {
			continue
		}
		member := i.getMember(itemID)
		createEpoch := i.currCreateEpochs[itemID]
		lastUpdate := i.currUpdates[itemID]
		lastValue := i.getLastValue(itemID)
		if err := i.InsertRow(tableDefs["items"],
			cnt, createEpoch, lastUpdate, member, lastValue); err != nil {
			return err
		}
	}
	if err := i.FlushOverwriteCurrentTable(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (i *items) DeepCopy() *items {
	copyItems := &items{
		name:             i.name,
		maxItemID:        i.maxItemID,
		members:          make(map[string]int),
		memberMap:        make(map[int]string),
		counts:           make(map[int]int),
		createEpochs:     make(map[int]int64),
		lastUpdates:      make(map[int]int64),
		lastValues:       make(map[int]string),
		currCounts:       make(map[int]int),
		currItemCount:    i.currItemCount,
		currUpdates:      make(map[int]int64),
		currCreateEpochs: make(map[int]int64),
		totalCount:       i.totalCount,
	}

	for k, v := range i.members {
		copyItems.members[k] = v
	}

	for k, v := range i.memberMap {
		copyItems.memberMap[k] = v
	}

	for k, v := range i.counts {
		copyItems.counts[k] = v
	}

	for k, v := range i.createEpochs {
		copyItems.createEpochs[k] = v
	}

	for k, v := range i.lastUpdates {
		copyItems.lastUpdates[k] = v
	}

	for k, v := range i.currUpdates {
		copyItems.currUpdates[k] = v
	}

	for k, v := range i.currCreateEpochs {
		copyItems.currCreateEpochs[k] = v
	}

	for k, v := range i.lastValues {
		copyItems.lastValues[k] = v
	}

	for k, v := range i.currCounts {
		copyItems.currCounts[k] = v
	}

	return copyItems
}

func (i *items) getCountBorder(rate float64) int {
	n := len(i.counts)
	counts := make([]int, n)
	j := 0
	for _, cnt := range i.counts {
		counts[j] = cnt
		j++
	}
	sort.Slice(counts, func(i, j int) bool {
		return counts[i] > counts[j]
	})
	total := i.totalCount
	sum := 0
	cnt := 0
	preCnt := 0
	oldCnt := 0
	for _, cnt = range counts {
		sum += cnt
		if float64(sum)/float64(total) >= rate {
			break
		}
		if cnt < oldCnt {
			preCnt = oldCnt
		}
		oldCnt = cnt
	}
	if preCnt == 0 {
		if oldCnt > 0 {
			preCnt = oldCnt
		} else {
			preCnt = cnt
		}
	}

	return preCnt
}

func (i *items) OLDgetCountBorder(rate float64) int {
	counts := i.counts

	maxCnt := 0
	for _, cnt := range counts {
		if cnt > maxCnt {
			maxCnt = cnt
		}
	}
	return int(math.Ceil(float64(maxCnt) * rate))
}

func (i *items) biggestNItems(N int) []int {
	// Create a slice of key-value pairs
	type kv struct {
		ItemID int
		Count  int
	}

	counts := i.counts

	var kvs []kv
	for k, v := range counts {
		kvs = append(kvs, kv{k, v})
	}

	// Sort the slice by Count in descending order
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Count > kvs[j].Count
	})

	// Extract the top N itemIDs
	var topN []int
	if N > 0 {
		for i := 0; i < N && i < len(kvs); i++ {
			topN = append(topN, kvs[i].ItemID)
		}
	} else {
		for i := range kvs {
			topN = append(topN, kvs[i].ItemID)
		}
	}

	return topN
}
