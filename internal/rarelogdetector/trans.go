package rarelogdetector

import (
	"errors"
	"fmt"
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
	ptRegistered      bool
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
	pt                *phraseTree
}

type phraseScore struct {
	phraseID int
	Count    int
	Score    float64
	Text     string
}

type phraseTree struct {
	childNodes map[int]*phraseTree
	parent     *phraseTree
	count      int
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
	t.ptRegistered = false
	t.readOnly = readOnly
	t.totalLines = 0
	t.minLineToDetect = 0
	t.phraseScores = make(map[int]float64, 10000)
	t.filterRe = filterRe
	t.xFilterRe = xFilterRe
	t.countByDay = 0
	t.maxCountByDay = 0

	t.pt = &phraseTree{
		count:      0,
		childNodes: nil,
	}

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
func (t *trans) calcCountBorder(rate float64) {
	t.termCountBorder = t.preTerms.getCountBorder(rate)
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
	t.createPtFromPhrases()

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
	p := *t.phrases
	phraseScores := make(map[int]float64, 0)
	for phraseID, line := range p.memberMap {
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
			phraseScores[phraseID] = score
		} else {
			return err
		}
	}
	t.phraseScores = phraseScores

	return nil
}

func (t *trans) registerPtNode(termID int, pt *phraseTree) (*phraseTree, bool) {
	if pt.childNodes == nil {
		pt.childNodes = make(map[int]*phraseTree)
	}

	childPT, ok := pt.childNodes[termID]
	if !ok {
		childPT = &phraseTree{
			count:  0,
			parent: pt,
		}
		pt.childNodes[termID] = childPT
	}
	childPT.count++
	if ok && childPT.count <= 0 {
		delete(pt.childNodes, termID)
	}
	return childPT, ok
}

func (t *trans) registerPt(tokens []int) {
	pt := t.pt
	_, sortedTerms, sortedCounts := t.sortTokensByCount(tokens)
	for i, termID := range sortedTerms {
		if sortedCounts[i] > t.termCountBorder {
			pt, _ = t.registerPtNode(termID, pt)
		}
	}
}

func (t *trans) searchPt(tokens []int, minLen, maxLen int) (int, int) {
	pt := t.pt
	ok := false
	_, sortedTerms, sortedCounts := t.sortTokensByCount(tokens)
	for i, termID := range sortedTerms {
		pt, ok = pt.childNodes[termID]
		if !ok {
			if i == 0 {
				return 0, 0
			}
			return sortedCounts[i-1], i
		}
		if pt.count <= 1 {
			if i+1 > minLen {
				return sortedCounts[i-1], i
			}
		}
		if maxLen >= minLen && i+1 >= maxLen {
			return sortedCounts[i], i
		}
	}
	return sortedCounts[len(sortedCounts)-1], -1
}

func (t *trans) createPtFromPhrases() {
	for p := range t.phrases.members {
		words := strings.Split(p, " ")
		tokens := make([]int, len(words))
		for i, term := range words {
			termID := t.terms.getItemID(term)
			tokens[i] = termID
		}
		t.registerPt(tokens)
	}
}

func (t *trans) sortTokensByCount(tokens []int) ([]int, []int, []int) {
	n := len(tokens)
	counts := make([]int, n)
	for i, termID := range tokens {
		counts[i] = t.preTerms.getCount(termID)
	}
	sortedTerms := make([]int, n)
	sortedCounts := make([]int, n)
	copy(sortedTerms, tokens)
	copy(sortedCounts, counts)
	sort.Slice(sortedCounts, func(i, j int) bool {
		return sortedCounts[i] > sortedCounts[j]
	})
	sort.Slice(sortedTerms, func(i, j int) bool {
		if sortedCounts[i] == sortedCounts[j] {
			return tokens[i] < tokens[j]
		}
		return sortedCounts[i] > sortedCounts[j]
	})
	return counts, sortedTerms, sortedCounts
}

func (t *trans) registerPhrase(tokens []int, lastUpdate int64, lastValue string,
	registerItem bool, minMatchRate, maxMatchRate float64) (int, string) {

	var te *items
	if t.preTermRegistered {
		te = t.preTerms
	} else {
		te = t.terms
	}
	n := len(tokens)

	phrase := make([]int, 0)
	counts := make([]int, n)
	for i, itemID := range tokens {
		counts[i] = te.getCount(itemID)
	}

	maxLen := 0
	minLen := 3
	if n > 3 {
		minLen = int(math.Floor(float64(n) * minMatchRate))
		maxLen = int(math.Floor(float64(n) * maxMatchRate))
	}
	minCnt, pos := t.searchPt(tokens, minLen, maxLen)

	if pos >= minLen {
		for i, count := range counts {
			if count >= minCnt {
				phrase = append(phrase, tokens[i])
			} else {
				phrase = append(phrase, cAsteriskItemID)
			}
		}
	} else {
		for i, count := range counts {
			if count > t.termCountBorder {
				phrase = append(phrase, tokens[i])
			} else {
				phrase = append(phrase, tokens[i])
			}
		}
		if len(phrase) <= 3 {
			phrase = tokens
		}
	}

	addCnt := 0
	if registerItem {
		addCnt = 1
	}

	phrasestr := ""
	word := ""
	for _, termId := range phrase {
		word = te.getMember(termId)
		phrasestr += " " + word
	}
	phrasestr = strings.TrimSpace(phrasestr)
	phraseID := t.phrases.register(phrasestr, addCnt, lastUpdate, lastValue, registerItem)
	if lastUpdate > t.latestUpdate {
		t.latestUpdate = lastUpdate
	}

	return phraseID, phrasestr
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

func (t *trans) tokenizeLine(line string, fileEpoch int64,
	registerItem, registerPreTerms, registerPT bool,
	minMatchRate, maxMatchRate float64) (int, []int, string, error) {
	var lastdt time.Time
	var err error
	phrasestr := ""

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

	if !registerPreTerms && !registerPT {
		if t.phrases.DataDir != "" && !t.readOnly && t.blockSize > 0 {
			if t.phrases.currItemCount >= t.blockSize || (t.currYearDay > 0 && yearDay > t.currYearDay) {
				if err := t.next(); err != nil {
					return -1, nil, "", err
				}
			}
		}
	}

	t.lastMessage = line

	tokens, err := t.toTermList(line, lastUpdate, registerItem, registerPreTerms)
	if err != nil {
		return -1, nil, "", err
	}

	t.countByDay++

	if registerPreTerms {
		t.totalLines++
	} else if registerPT {
		t.registerPt(tokens)
	} else {
		//if strings.Contains(line, "[knobayashi] Inactivity timeout") {
		//	println("here")
		//}
		if !t.ptRegistered {
			return -1, nil, "", errors.New("phrase tree not registered")
		}
		phraseID := -1
		phraseID, phrasestr = t.registerPhrase(tokens, lastUpdate, orgLine, registerItem, minMatchRate, maxMatchRate)
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

	return phraseCnt, tokens, phrasestr, nil
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

func (t *trans) getTopNScores(N, minCnt int, maxLastUpdate int64,
	showLastText bool, termCountBorderRate float64) []phraseScore {
	if termCountBorderRate > 0 {
		if err := t.rearangePhrases(termCountBorderRate); err != nil {
			return nil
		}
	}

	phraseScores := t.phraseScores
	p := t.phrases

	var scores []phraseScore
	var text string
	for phraseID, score := range phraseScores {
		if showLastText {
			text = p.getLastValue(phraseID)
		} else {
			text = p.getMember(phraseID)
		}

		if !t.match(text) {
			continue
		}

		cnt := p.getCount(phraseID)
		lastUpdate := p.getLastUpdate(phraseID)
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

func (t *trans) analyzeLine(line string) error {
	te := t.terms

	_, token, phrasestr, err := t.tokenizeLine(line, 0, false, false, false, 1.0, 0.0)
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", phrasestr)
	for _, termID := range token {
		word := te.getMember(termID)
		cnt := te.getCount(termID)
		fmt.Printf("%s: %d\n", word, cnt)
	}

	return nil
}

func (t *trans) rearangePhrases(termCountBorderRate float64) error {
	p, err := newItems("", "phrase2", 0, 0, false)
	if err != nil {
		return err
	}

	// if new termCountBorder is equal or less than current, there will be no change
	oldTermCountBorder := t.termCountBorder
	t.calcCountBorder(termCountBorderRate)
	if t.termCountBorder <= oldTermCountBorder {
		return nil
	}

	for phraseID, line := range t.phrases.memberMap {
		cnt := t.phrases.getCount(phraseID)
		lastUpdate := t.phrases.getLastUpdate(phraseID)
		lastValue := t.phrases.getLastValue(phraseID)

		len := 0
		words := strings.Split(line, " ")
		phrasestr := ""
		for _, term := range words {
			termID := t.terms.getItemID(term)
			termCnt := t.terms.getCount(termID)
			if termCnt >= t.termCountBorder {
				phrasestr += " " + term
				len++
			} else {
				phrasestr += " *"
			}
		}
		if len < 3 {
			phrasestr = line
		}
		phrasestr = strings.TrimSpace(phrasestr)
		p.register(phrasestr, cnt, lastUpdate, lastValue, false)
	}

	t.phrases = p

	if err := t.calcPhrasesScore(); err != nil {
		return err
	}

	return nil
}
