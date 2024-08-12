package rarelogdetector

import (
	"container/heap"
	"math"

	"goRareLogDetector/pkg/csvdb"

	"github.com/pkg/errors"
)

type items struct {
	*csvdb.CircuitDB
	name          string
	maxItemID     int
	members       map[string]int
	memberMap     map[int]string
	counts        map[int]int
	lastUpdates   map[int]int64
	lastValues    map[int]string
	currCounts    map[int]int
	currItemCount int
	totalCount    int
}

func newItems(dataDir, name string, maxBlocks int, useGzip bool) (*items, error) {
	i := new(items)
	// Now: maxRowsInBlock=0 TODO: support rotation and change this
	d, err := csvdb.NewCircuitDB(dataDir, name, tableDefs["items"], maxBlocks, 0, useGzip)
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

func (i *items) register(item string, addCount int, lastUpdate int64, lastValue string, isNew bool) int {
	if item == "" {
		return -1
	}
	itemID, ok := i.members[item]
	if ok {
		if lastUpdate > i.lastUpdates[itemID] {
			i.lastUpdates[itemID] = lastUpdate
		}
	} else {
		i.maxItemID++
		itemID = i.maxItemID
		i.members[item] = itemID
		i.memberMap[itemID] = item
		i.lastUpdates[itemID] = lastUpdate
		if isNew {
			i.currItemCount++
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
		return "-"
	}
	return i.memberMap[itemID]
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

func (i *items) getFrequency(itemID int) float64 {
	if i.totalCount == 0 {
		return 0.0
	}
	return float64(i.counts[itemID]) / float64(i.totalCount)
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
		var lastUpdate int64
		var lastValue string
		err = rows.Scan(&itemCount, &lastUpdate, &item, &lastValue)
		if err != nil {
			return err
		}
		i.register(item, itemCount, lastUpdate, lastValue, !rows.BlockCompleted)
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
	i.NextBlock()

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
		var lastUpdate int64
		err = rows.Scan(&itemCount, &lastUpdate, &item)
		if err != nil {
			return err
		}
		itemID := i.getItemID(item)
		i.counts[itemID] -= itemCount
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
		lastUpdate := i.getLastUpdate(itemID)
		lastValue := i.getLastValue(itemID)
		if err := i.InsertRow(tableDefs["items"],
			cnt, lastUpdate, member, lastValue); err != nil {
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
		name:          i.name,
		maxItemID:     i.maxItemID,
		members:       make(map[string]int),
		memberMap:     make(map[int]string),
		counts:        make(map[int]int),
		lastUpdates:   make(map[int]int64),
		lastValues:    make(map[int]string),
		currCounts:    make(map[int]int),
		currItemCount: i.currItemCount,
		totalCount:    i.totalCount,
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

	for k, v := range i.lastUpdates {
		copyItems.lastUpdates[k] = v
	}

	for k, v := range i.lastValues {
		copyItems.lastValues[k] = v
	}

	for k, v := range i.currCounts {
		copyItems.currCounts[k] = v
	}

	return copyItems
}

// An Item represents an element in the priority queue
type Item struct {
	itemID     int
	lastUpdate int64
}

// A MinHeap is a priority queue for Items based on lastUpdate (min-heap)
type MinHeap []Item

func (h MinHeap) Len() int           { return len(h) }
func (h MinHeap) Less(i, j int) bool { return h[i].lastUpdate < h[j].lastUpdate }
func (h MinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *MinHeap) Push(x interface{}) {
	*h = append(*h, x.(Item))
}

func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// Method of items to get the last N itemIDs with count M
func (i *items) getLastNItemsWithCountM(N int, M int) []int {
	h := &MinHeap{}
	heap.Init(h)

	for itemID, count := range i.counts {
		if count == M {
			lastUpdate := i.lastUpdates[itemID]
			heap.Push(h, Item{itemID: itemID, lastUpdate: lastUpdate})
			if h.Len() > N {
				heap.Pop(h)
			}
		}
	}

	// Collect results from the heap
	result := make([]int, h.Len())
	for k := len(result) - 1; k >= 0; k-- {
		result[k] = heap.Pop(h).(Item).itemID
	}
	return result
}
