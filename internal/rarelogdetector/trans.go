package rarelogdetector

import (
	"goRareLogDetector/pkg/utils"
	"math"
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
	scoreAvg          float64
	scoreStd          float64
	readOnly          bool
	totalLines        int
	minLineToDetect   int
	latestUpdate      int64
	filterRe          *regexp.Regexp
	xFilterRe         *regexp.Regexp
}

type phraseScore struct {
	phraseID int
	Count    int
	Score    float64
	Text     string
}

func newTrans(dataDir, logFormat, timestampLayout string,
	maxBlocks, blockSize, daysToKeep int,
	filterRe, xFilterRe *regexp.Regexp,
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

	//t.maxDays = maxDays

	return t, nil
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
	if err := t.calcPhraseScore(); err != nil {
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

func (t *trans) calcPhraseScore() error {
	for _, line := range t.phrases.memberMap {
		if tokens, err := t.toTermList(line, 0, false, false); err == nil {
			t.registerPhrase(tokens, 0, "", false, 1.0)
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
	//yearDay := 0
	lastUpdate := fileEpoch
	if t.timestampPos >= 0 || t.messagePos >= 0 {
		match := t.logFormatRe.FindStringSubmatch(line)
		if t.timestampPos >= 0 && t.timestampLayout != "" {
			lastdt, err = utils.Str2date(t.timestampLayout, match[t.timestampPos])
		}
		if err == nil {
			lastUpdate = lastdt.Unix()
		}
		if t.messagePos >= 0 {
			line = match[t.messagePos]
		}
		//yearDay = lastdt.Year()*1000 + lastdt.YearDay()
	}

	t.lastMessage = line

	tokens, err := t.toTermList(line, lastUpdate, registerItem, registerPreTerms)
	if err != nil {
		return -1, err
	}

	if registerPreTerms {
		t.totalLines++
	} else {
		phraseID := t.registerPhrase(tokens, lastUpdate, orgLine, registerItem, 0)
		if t.phrases.DataDir != "" && !t.readOnly && t.phrases.currItemCount >= t.blockSize {
			if err := t.next(); err != nil {
				return -1, err
			}
		}
		phraseCnt = t.phrases.getCount(phraseID)
	}

	//t.currYearDay = yearDay

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

func (t *trans) getTopNScores(N, minCnt int, maxLastUpdate int64) []phraseScore {
	phraseScores := t.phraseScores
	filterRe := t.filterRe
	xFilterRe := t.xFilterRe

	var scores []phraseScore
	for phraseID, score := range phraseScores {
		text := t.phrases.getLastValue(phraseID)
		b := []byte(text)
		if filterRe != nil && !filterRe.Match(b) {
			continue
		}
		if xFilterRe != nil && xFilterRe.Match(b) {
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
