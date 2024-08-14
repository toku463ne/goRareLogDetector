package rarelogdetector

import (
	"goRareLogDetector/pkg/utils"
	"regexp"
	"sort"
	"strings"
	"time"
)

type trans struct {
	terms             *items
	preTerms          *items
	phrases           *items
	phraseScores      map[int]float64
	replacer          *strings.Replacer
	logFormatRe       *regexp.Regexp
	timestampLayout   string
	timestampPos      int
	messagePos        int
	blockSize         int
	lastMessage       string
	preTermRegistered bool
	readOnly          bool
	totalLines        int
	minLineToDetect   int
	latestUpdate      int64
	filterRe          []*regexp.Regexp
	xFilterRe         []*regexp.Regexp
	currYearDay       int
	countByDay        int
	maxCountByDay     int
	termCountBorder   int
}

type phraseScore struct {
	phraseID int
	Count    int
	Score    float64
	Text     string
}

func newTrans(dataDir, logFormat, timestampLayout string,
	maxBlocks, blockSize, daysToKeep int,
	filterRe, xFilterRe []*regexp.Regexp,
	useGzip, readOnly bool) (*trans, error) {
	t := new(trans)
	te, err := newItems(dataDir, "terms", maxBlocks, daysToKeep, useGzip)
	if err != nil {
		return nil, err
	}
	pte, err := newItems("", "terms", 0, daysToKeep, false)
	if err != nil {
		return nil, err
	}
	p, err := newItems(dataDir, "phrases", maxBlocks, daysToKeep, useGzip)
	if err != nil {
		return nil, err
	}

	t.terms = te
	t.preTerms = pte
	t.phrases = p
	t.blockSize = blockSize
	t.replacer = getDelimReplacer()
	t.parseLogFormat(logFormat)
	t.timestampLayout = timestampLayout
	t.preTermRegistered = false
	t.readOnly = readOnly
	t.totalLines = 0
	t.minLineToDetect = 0
	t.phraseScores = make(map[int]float64, 10000)
	t.filterRe = filterRe
	t.xFilterRe = xFilterRe
	t.countByDay = 0
	t.maxCountByDay = 0

	//t.maxDays = maxDays

	return t, nil
}

func (t *trans) setMaxBlocks(maxBlocks int) {
	if t.terms != nil {
		t.terms.SetMaxBlocks(maxBlocks)
	}
	if t.phrases != nil {
		t.phrases.SetMaxBlocks(maxBlocks)
	}
}
func (t *trans) setBlockSize(blockSize int) {
	t.blockSize = blockSize
	if t.terms != nil {
		t.terms.SetBlockSize(blockSize)
	}
	if t.phrases != nil {
		t.phrases.SetBlockSize(blockSize)
	}
}
func (t *trans) calcCountBorder() {
	t.termCountBorder = t.preTerms.getCountBorder(cTermCountBorderRate)
}

func (t *trans) parseLogFormat(logFormat string) {
	re := regexp.MustCompile(logFormat)
	names := re.SubexpNames()
	t.timestampPos = -1
	t.messagePos = -1
	for i, name := range names {
		switch {
		case name == "timestamp":
			t.timestampPos = i
		case name == "message":
			t.messagePos = i
		}
	}
	t.logFormatRe = re
}

func (t *trans) close() {
	if t.terms.CircuitDB != nil {
		t.terms = nil
	}
	if t.phrases.CircuitDB != nil {
		t.phrases = nil
	}
}

func (t *trans) load() error {
	if err := t.terms.load(); err != nil {
		return err
	}
	if err := t.phrases.load(); err != nil {
		return err
	}
	if err := t.calcPhrasesScore(); err != nil {
		return err
	}

	t.latestUpdate = t.phrases.lastUpdate

	// merge to t.preTerms
	t.preTerms = t.terms.DeepCopy()

	return nil
}

func (t *trans) commit(completed bool) error {
	if t.readOnly {
		return nil
	}
	if err := t.terms.commit(completed); err != nil {
		return err
	}
	if err := t.phrases.commit(completed); err != nil {
		return err
	}
	return nil
}

/*
func (t *trans) calcStats() {
	if !t.preTermRegistered {
		return
	}

	te := t.preTerms
	scores := make([]float64, te.maxItemID+1)
	sum := 0.0

	for termID, cnt := range te.counts {
		scores[termID] = te.getIdf(termID)
		sum += scores[termID] * float64(cnt)
	}
	average := sum / float64(te.totalCount)

	varianceSum := 0.0
	for i, score := range scores {
		varianceSum += (score - average) * float64(te.counts[i]) * (score - average)
	}
	variance := varianceSum / float64(te.totalCount)
	std := math.Sqrt(variance)

	t.scoreAvg = average
	t.scoreStd = std
}
*/

func (t *trans) calcPhrasesScore() error {
	var te *items
	if t.preTermRegistered {
		te = t.preTerms
	} else {
		te = t.terms
	}
	for phraseID, line := range t.phrases.memberMap {
		if tokens, err := t.toTermList(line, 0, false, false); err == nil {
			scores := make([]float64, len(tokens))
			for i, itemID := range tokens {
				scores[i] = te.getIdf(itemID)
			}
			ss := 0.0
			for _, v := range scores {
				ss += v
			}
			score := 0.0
			if len(scores) > 0 {
				score = ss / float64(len(scores))
			}
			t.phraseScores[phraseID] = score
		} else {
			return err
		}
	}
	return nil
}

func (t *trans) registerPhrase(tokens []int, lastUpdate int64, lastValue string,
	registerItem bool, matchRate float64) int {
	var te *items
	if t.preTermRegistered {
		te = t.preTerms
	} else {
		te = t.terms
	}
	n := len(tokens)
	counts := make([]int, n)
	phrase := make([]int, 0)

	for i, itemID := range tokens {
		counts[i] = te.getCount(itemID)
	}

	if n <= 3 || matchRate == 1.0 {
		phrase = tokens
	} else {
		rate := 0.0
		if matchRate > 0 {
			rate = matchRate
		} else {
			for _, tmr := range tranMatchRates {
				if n >= tmr.matchLen {
					rate = tmr.matchRate
				}
			}
		}
		minLen := int(float64(n) * rate)
		indexes := utils.SortIndexByIntValue(counts, false)
		newLen := minLen
		for i := minLen; i < n; i++ {
			if counts[indexes[i]] <= t.termCountBorder {
				if counts[indexes[i]] < counts[indexes[i-1]] {
					newLen = i //apply "i-1" as index, as len it is "i"
					break
				}
			} else {
				newLen = i + 1
			}
		}

		indexes = indexes[:newLen]
		for j, termID := range tokens {
			contains := false
			for _, k := range indexes {
				if j == k {
					contains = true
					break
				}
			}
			if contains {
				phrase = append(phrase, termID)
			}
		}

	}

	phrasestr := ""
	word := ""
	for _, termId := range phrase {
		word = te.getMember(termId)
		phrasestr += " " + word
	}
	phrasestr = strings.TrimSpace(phrasestr)
	addCnt := 0
	if registerItem {
		addCnt = 1
	}
	phraseID := t.phrases.register(phrasestr, addCnt, lastUpdate, lastValue, registerItem)

	if lastUpdate > t.latestUpdate {
		t.latestUpdate = lastUpdate
	}

	return phraseID
}

/*
func (t *trans) OLD_registerPhrase(tokens []int, lastUpdate int64, lastValue string,
	registerItem bool, matchRate float64) int {
	var te *items
	if t.preTermRegistered {
		te = t.preTerms
	} else {
		te = t.terms
	}
	scores := make([]float64, len(tokens))
	gaps := make([]float64, len(tokens))
	phrase := make([]int, 0)
	average := t.scoreAvg
	s := t.scoreStd

	for i, itemID := range tokens {
		scores[i] = te.getIdf(itemID)
		gaps[i] = (scores[i] - average) / s
	}

	n := len(tokens)
	if n <= 3 || matchRate == 1.0 {
		phrase = tokens
	} else {
		rate := 0.0
		if matchRate > 0 {
			rate = matchRate
		} else {
			for _, tmr := range tranMatchRates {
				if n >= tmr.matchLen {
					rate = tmr.matchRate
				}
			}
		}
		minLen := int(float64(n) * rate)
		indexes := utils.SortIndexByValue(gaps, true)
		newLen := minLen
		for i := minLen; i < n; i++ {
			// cannot be bigger than cMaxGapToRegister
			if gaps[indexes[i]] < cMaxGapToRegister {
				newLen = i
				break
			}
			if gaps[indexes[i]] > cGapInPhrases {
				if gaps[indexes[i]] > gaps[indexes[i-1]] {
					newLen = i //apply "i-1" as index, as len it is "i"
					break
				}
			} else {
				newLen = i + 1
			}
		}

		indexes = indexes[:newLen]
		for j, termID := range tokens {
			contains := false
			for _, k := range indexes {
				if j == k {
					contains = true
					break
				}
			}
			if contains {
				phrase = append(phrase, termID)
			}
		}

	}
	phrasestr := ""
	word := ""
	for _, termId := range phrase {
		word = te.getMember(termId)
		phrasestr += " " + word
	}
	phrasestr = strings.TrimSpace(phrasestr)
	addCnt := 0
	if registerItem {
		addCnt = 1
	}
	phraseID := t.phrases.register(phrasestr, addCnt, lastUpdate, lastValue, registerItem)
	ss := 0.0
	for _, v := range scores {
		ss += v
	}
	score := ss / float64(n)
	t.phraseScores[phraseID] = score

	if lastUpdate > t.latestUpdate {
		t.latestUpdate = lastUpdate
	}

	return phraseID
}
*/

func (t *trans) toTermList(line string, lastUpdate int64, registerItem, registerPreTerms bool) ([]int, error) {
	line = t.replacer.Replace(line)
	words := strings.Split(line, " ")
	tokens := make([]int, 0)

	termID := -1

	for _, w := range words {
		if _, ok := enStopWords[w]; ok {
			continue
		}

		word := strings.ToLower(w)
		lenw := len(word)
		if lenw > cMaxWordLen {
			word = word[:cMaxWordLen]
			lenw = cMaxWordLen
		}
		//remove '.' in the end
		if lenw > 1 && string(word[lenw-1]) == "." {
			word = word[:lenw-1]
		}

		addCnt := 0
		if registerItem {
			addCnt = 1
		}
		if len(word) > 2 || word == " " {
			if utils.IsInt(word) && len(word) > cMaxNumDigits {
				continue
			}
			if registerPreTerms {
				termID = t.preTerms.register(word, addCnt, lastUpdate, "", registerItem)
			} else {
				termID = t.terms.register(word, addCnt, lastUpdate, "", registerItem)
			}
			tokens = append(tokens, termID)
		}
	}

	return tokens, nil
}

func (t *trans) tokenizeLine(line string, fileEpoch int64, registerItem, registerPreTerms bool) (int, error) {
	var lastdt time.Time
	var err error

	orgLine := line
	phraseCnt := -1
	yearDay := 0
	lastUpdate := fileEpoch
	if t.timestampPos >= 0 || t.messagePos >= 0 {
		match := t.logFormatRe.FindStringSubmatch(line)
		if len(match) > 0 {
			if t.timestampPos >= 0 && t.timestampLayout != "" && len(match) > t.timestampPos {
				lastdt, err = utils.Str2date(t.timestampLayout, match[t.timestampPos])
				yearDay = lastdt.Year()*1000 + lastdt.YearDay()
			}
			if err == nil {
				lastUpdate = lastdt.Unix()
			}
			if lastUpdate > 0 {
				if t.messagePos >= 0 && len(match) > t.messagePos {
					line = match[t.messagePos]
				}
			}
		}
	}

	if !registerPreTerms {
		if t.phrases.DataDir != "" && !t.readOnly && t.blockSize > 0 {
			if t.phrases.currItemCount >= t.blockSize || (t.currYearDay > 0 && yearDay > t.currYearDay) {
				if err := t.next(); err != nil {
					return -1, err
				}
			}
		}
	}

	t.lastMessage = line

	tokens, err := t.toTermList(line, lastUpdate, registerItem, registerPreTerms)
	if err != nil {
		return -1, err
	}

	t.countByDay++

	if registerPreTerms {
		t.totalLines++
	} else {
		phraseID := t.registerPhrase(tokens, lastUpdate, orgLine, registerItem, 0)
		phraseCnt = t.phrases.getCount(phraseID)
	}

	if registerPreTerms {
		if t.currYearDay > 0 && yearDay > t.currYearDay {
			if t.countByDay > t.maxCountByDay {
				t.maxCountByDay = t.countByDay
			}
			t.countByDay = 0
		}
	}
	t.currYearDay = yearDay

	return phraseCnt, nil
}

// Rotate phrases and terms together to remove oldest items in the same timeline
func (t *trans) next() error {
	if t.readOnly {
		return nil
	}
	if err := t.phrases.next(); err != nil {
		return err
	}
	if err := t.terms.next(); err != nil {
		return err
	}
	return nil
}

func (t *trans) match(text string) bool {
	b := []byte(text)
	matched := true
	for _, filterRe := range t.filterRe {
		if !filterRe.Match(b) {
			matched = false
			break
		}
	}
	if !matched {
		return false
	}

	matched = false
	for _, xFilterRe := range t.xFilterRe {
		if xFilterRe.Match(b) {
			matched = true
			break
		}
	}
	return !matched
}

func (t *trans) getTopNScores(N, minCnt int, maxLastUpdate int64) []phraseScore {
	phraseScores := t.phraseScores

	var scores []phraseScore
	for phraseID, score := range phraseScores {
		text := t.phrases.getLastValue(phraseID)

		if !t.match(text) {
			continue
		}

		cnt := t.phrases.getCount(phraseID)
		lastUpdate := t.phrases.getLastUpdate(phraseID)
		if cnt <= minCnt && (maxLastUpdate == 0 || lastUpdate >= maxLastUpdate) {
			scores = append(scores, phraseScore{phraseID, cnt, score, text})
		}
	}

	// Sort the slice by score in descending order
	sort.Slice(scores, func(i, j int) bool {
		return scores[i].Score > scores[j].Score
	})

	// Get the top N key-score pairs
	if len(scores) > N {
		scores = scores[:N]
	}

	return scores
}
