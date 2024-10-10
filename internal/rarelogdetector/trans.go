package rarelogdetector

import (
	"encoding/csv"
	"errors"
	"fmt"
	"goRareLogDetector/pkg/utils"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type trans struct {
	terms               *items
	phrases             *items
	orgPhrases          *items
	customPhrases       *items
	phraseScores        map[int]float64
	subjects            map[int]string
	replacer            *strings.Replacer
	logFormatRe         *regexp.Regexp
	timestampLayout     string
	timestampPos        int
	messagePos          int
	blockSize           int
	lastMessage         string
	ptRegistered        bool
	readOnly            bool
	totalLines          int
	minLineToDetect     int
	latestUpdate        int64
	filterRe            []*regexp.Regexp
	xFilterRe           []*regexp.Regexp
	currRetentionPos    int
	countByBlock        int
	maxCountByBlock     int
	termCountBorderRate float64
	termCountBorder     int
	retention           int64
	frequency           string
	keywords            map[string]string
	keyTermIds          map[int]string
	ignorewords         map[string]string
	pt                  *phraseTree
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
	depth      int
}

func newTrans(dataDir, logFormat, timestampLayout string,
	maxBlocks, blockSize int,
	retention int64, frequency string,
	termCountBorderRate float64,
	filterRe, xFilterRe []*regexp.Regexp,
	_keywords []string, _ignorewords []string, _customPhrases []string,
	useGzip, readOnly bool) (*trans, error) {
	t := new(trans)
	te, err := newItems(dataDir, "terms", maxBlocks, retention, frequency, useGzip)
	if err != nil {
		return nil, err
	}

	p, err := newItems(dataDir, "phrases", maxBlocks, retention, frequency, useGzip)
	if err != nil {
		return nil, err
	}

	t.terms = te
	t.phrases = p
	t.blockSize = blockSize
	t.replacer = getDelimReplacer()
	t.parseLogFormat(logFormat)
	t.timestampLayout = timestampLayout
	t.ptRegistered = false
	t.readOnly = readOnly
	t.totalLines = 0
	t.minLineToDetect = 0
	t.phraseScores = make(map[int]float64, 10000)
	t.subjects = make(map[int]string, 0)
	t.filterRe = filterRe
	t.xFilterRe = xFilterRe
	t.countByBlock = 0
	t.maxCountByBlock = 0
	t.retention = retention
	t.frequency = frequency
	t.termCountBorderRate = termCountBorderRate
	t.keywords = make(map[string]string)
	t.ignorewords = make(map[string]string)
	t.keyTermIds = make(map[int]string)
	for _, word := range _keywords {
		t.keywords[word] = ""
	}
	for _, word := range _ignorewords {
		t.ignorewords[word] = ""
	}

	t.pt = &phraseTree{
		count:      0,
		childNodes: nil,
	}

	t.customPhrases, err = newItems("", "customPhrases", 0, 0, "", false)
	if err != nil {
		return nil, err
	}
	for _, phrase := range _customPhrases {
		if err := t.registerCustomPhrase(phrase, 0, 0, 0, ""); err != nil {
			return nil, err
		}
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
func (t *trans) calcCountBorder(rate float64, termCountBorder int) {
	if termCountBorder > 0 {
		t.termCountBorder = termCountBorder
		return
	}
	if rate == 0 {
		rate = cTermCountBorderRate
	}
	t.termCountBorder = t.terms.getCountBorder(rate)

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

func (t *trans) calcPhrasesScore() error {
	te := t.terms
	p := *t.phrases
	phraseScores := make(map[int]float64, 0)
	for phraseID, line := range p.memberMap {
		if tokens, err := t.toTermList(line, 0, false); err == nil {
			//scores := make([]float64, len(tokens))
			scores := make([]float64, 0)
			for _, itemID := range tokens {
				//scores[i] = te.getIdf(itemID)
				if itemID >= 0 {
					scores = append(scores, te.getIdf(itemID))
				}
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

func (t *trans) registerCustomPhrase(phrase string, addCount int,
	createEpoch int64, lastUpdate int64, lastValue string) error {
	cp := t.customPhrases
	if cpID, ok := cp.members[phrase]; ok {
		cp.counts[cpID] += addCount
		return nil
	}
	if tokens, err := t.toTermList(phrase, 0, false); err != nil {
		return err
	} else {
		cID := cp.register(phrase, addCount, createEpoch, lastUpdate, lastValue, false)
		cp.tokensMap[cID] = tokens
	}
	return nil
}

func (t *trans) resetCustomPhrases() {
	cp := t.customPhrases
	for cID := range cp.memberMap {
		cp.counts[cID] = 0
	}
}

func (t *trans) searchCustomPhrase(tokens []int) int {
	cp := t.customPhrases
	if cp.maxItemID <= 0 {
		return -1
	}
	for cpID, ctokens := range cp.tokensMap {
		found := true
		if len(ctokens) == len(tokens) {
			for i, termID := range tokens {
				if ctokens[i] != cAsteriskItemID && ctokens[i] != termID {
					found = false
					break
				}
			}
		} else {
			found = false
		}
		if found {
			return cpID
		}
	}
	return -1
}

func (t *trans) registerPtNode(termID int, pt *phraseTree, addCnt int) (*phraseTree, bool) {
	if pt.childNodes == nil {
		pt.childNodes = make(map[int]*phraseTree)
	}

	childPT, ok := pt.childNodes[termID]
	if !ok {
		childPT = &phraseTree{
			count:  0,
			parent: pt,
			depth:  pt.depth + 1,
		}
		pt.childNodes[termID] = childPT
	}
	childPT.count += addCnt
	if ok && childPT.count <= 0 {
		delete(pt.childNodes, termID)
	}
	return childPT, ok
}

func (t *trans) registerPt(tokens []int, addCnt int) {
	pt := t.pt
	sortedTerms, _ := t.sortTokensByCount(tokens)
	for _, termID := range sortedTerms {
		if termID == cAsteriskItemID {
			continue
		}
		pt, _ = t.registerPtNode(termID, pt, addCnt)
	}
}

func (t *trans) OLDregisterPt(tokens []int, addCnt int) {
	pt := t.pt
	sortedTerms, sortedCounts := t.sortTokensByCount(tokens)
	for i, termID := range sortedTerms {
		if termID == cAsteriskItemID {
			continue
		}
		_, ok := t.keyTermIds[termID]
		if ok || sortedCounts[i] >= t.termCountBorder {
			pt, _ = t.registerPtNode(termID, pt, addCnt)
		}
	}
}

func (t *trans) searchPt(tokens []int, minLen, maxLen int) (int, int) {
	pt := t.pt
	ok := false
	if minLen == 0 {
		minLen = 3
	}
	sortedTerms, sortedCounts := t.sortTokensByCount(tokens)
	for i, termID := range sortedTerms {
		pt, ok = pt.childNodes[termID]
		if !ok {
			if i == 0 {
				return 0, 0
			}
			return sortedCounts[i-1], i
		}
		if pt.count <= 1 || sortedCounts[i] < t.termCountBorder {
			if i+1 > minLen {
				if i == 0 {
					return 0, 0
				}
				return sortedCounts[i-1], i
			}
		}
		if maxLen >= minLen && i+1 >= maxLen {
			return sortedCounts[i], i
		}
	}
	if len(sortedCounts) > 0 {
		return sortedCounts[len(sortedCounts)-1], -1
	} else {
		return -1, -1
	}
}

func (t *trans) sortTokensByCount(tokens []int) ([]int, []int) {
	te := t.terms
	n := len(tokens)
	counts := make(map[int]int, 0)
	for _, termID := range tokens {
		counts[termID] = te.getCount(termID)
	}
	sortedTerms := make([]int, n)
	sortedCounts := make([]int, n)
	copy(sortedTerms, tokens)

	// Sort both sortedTerms and sortedCounts together based on counts
	sort.SliceStable(sortedTerms, func(i, j int) bool {
		ti := counts[sortedTerms[i]]
		tj := counts[sortedTerms[j]]

		if ti > tj {
			return true
		}
		if ti == tj {
			return sortedTerms[i] < sortedTerms[j] // Keep younger term first if counts are the same
		}
		return false
	})

	for i, termID := range sortedTerms {
		sortedCounts[i] = counts[termID]
	}

	return sortedTerms, sortedCounts
}

func (t *trans) registerSubject(phraseID int, line string, replaces map[int]string) string {
	if subject, ok := t.subjects[phraseID]; ok {
		return subject
	}
	if len(replaces) == 0 {
		t.subjects[phraseID] = line
		return line
	}

	for _, word := range replaces {
		// Use capturing groups to capture delimiters and replace only the word
		pattern := `(?i)(^|` + cDelimiters + `)(` + regexp.QuoteMeta(word) + `)($|` + cDelimiters + `)`
		reg := regexp.MustCompile(pattern)
		line = reg.ReplaceAllString(line, `$1`+"*"+`$3`)
	}
	// Combine multiple consecutive "*" into a single "*"
	reg := regexp.MustCompile(`\*[\s\t]*\*`)
	line = reg.ReplaceAllString(line, `*`)
	t.subjects[phraseID] = line
	return line
}

func (t *trans) registerPhrase(tokens []int, lastUpdate int64, lastValue string,
	addCnt int, minMatchRate, maxMatchRate float64, useCustomPhrase bool) (int, string) {
	te := t.terms
	n := len(tokens)
	excludesMap := make(map[int]string)

	phrase := make([]int, 0)
	counts := make([]int, n)
	for i, itemID := range tokens {
		counts[i] = te.getCount(itemID)
	}

	cID := -1
	if useCustomPhrase {
		cID = t.searchCustomPhrase(tokens)
		if cID > 0 {
			//t.customPhrases.update(cID, addCnt, lastUpdate, lastValue, false)
			phrase = t.customPhrases.tokensMap[cID]
		}
	}

	if cID == -1 {
		maxLen := 0
		minLen := 3
		if n > 3 {
			if minMatchRate > 0 {
				minLen = int(math.Floor(float64(n) * minMatchRate))
			}
			maxLen = int(math.Floor(float64(n) * maxMatchRate))
		}
		minCnt, pos := t.searchPt(tokens, minLen, maxLen)

		//lastToken := 0
		if pos >= minLen {
			for i, count := range counts {
				termID := tokens[i]
				_, ok := t.keyTermIds[termID]
				if ok || count >= minCnt {
					phrase = append(phrase, termID)
					//lastToken = termID
				} else {
					//if lastToken != cAsteriskItemID {
					//	phrase = append(phrase, cAsteriskItemID)
					//	lastToken = cAsteriskItemID
					//}
					phrase = append(phrase, cAsteriskItemID)
					if termID != cAsteriskItemID {
						excludesMap[termID] = t.terms.getMember(termID)
					}
				}
			}
		} else {
			phrase = tokens
		}
	}

	registerItem := false
	if addCnt > 0 {
		registerItem = true
	}

	phrasestr := ""
	word := ""
	for _, termId := range phrase {
		word = te.getMember(termId)
		phrasestr += " " + word
	}
	phrasestr = strings.TrimSpace(phrasestr)
	phraseID := t.phrases.register(phrasestr, addCnt, lastUpdate, lastUpdate, lastValue, registerItem)
	if lastUpdate > t.latestUpdate {
		t.latestUpdate = lastUpdate
	}
	if lastValue != "" {
		t.registerSubject(phraseID, lastValue, excludesMap)
	}

	return phraseID, phrasestr
}

func (t *trans) toTermList(line string,
	lastUpdate int64,
	registerItem bool) ([]int, error) {
	line = t.replacer.Replace(line)
	words := strings.Split(line, " ")
	tokens := make([]int, 0)

	termID := -1

	for _, w := range words {
		if w == "" {
			continue
		}

		if _, ok := t.ignorewords[w]; ok {
			w = "*"
		}
		_, keyOK := t.keywords[w]
		if _, ok := enStopWords[w]; ok {
			if !keyOK {
				w = "*"
			}
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
		if keyOK || len(word) > 2 {
			if !keyOK && utils.IsInt(word) && len(word) > cMaxNumDigits {
				continue
			}
			termID = t.terms.register(word, addCnt, lastUpdate, lastUpdate, "", registerItem)

			tokens = append(tokens, termID)
			if keyOK {
				t.keyTermIds[termID] = ""
			}
			//} else if word == "*" && len(tokens) > 1 && tokens[len(tokens)-1] != cAsteriskItemID {
		} else if word == "*" {
			tokens = append(tokens, cAsteriskItemID)
		}
	}

	return tokens, nil
}

/*
stage: 1=registerTerm 2=registerPT 3=registerPhrase
*/
func (t *trans) tokenizeLine(line string, fileEpoch int64, stage int,
	minMatchRate, maxMatchRate float64) (int, []int, string, error) {
	var lastdt time.Time
	var err error
	phrasestr := ""

	if !t.match(line) {
		return -1, nil, "", nil
	}

	orgLine := line
	phraseCnt := -1
	retentionPos := 0
	lastUpdate := fileEpoch
	if t.timestampPos >= 0 || t.messagePos >= 0 {
		match := t.logFormatRe.FindStringSubmatch(line)
		if len(match) > 0 {
			if t.timestampPos >= 0 && t.timestampLayout != "" && len(match) > t.timestampPos {
				lastdt, err = utils.Str2date(t.timestampLayout, match[t.timestampPos])
				switch t.frequency {
				case "hour":
					retentionPos = lastdt.Year()*100000 + lastdt.YearDay()*100 + lastdt.Hour()
				case "day":
					retentionPos = lastdt.Year()*1000 + lastdt.YearDay()
				default:
					retentionPos = 0
				}
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

	if stage == cStageRegisterPhrases {
		if t.phrases.DataDir != "" && !t.readOnly {
			if (t.blockSize > 0 && t.phrases.currItemCount >= t.blockSize) || (t.currRetentionPos > 0 && retentionPos > t.currRetentionPos) {
				if err := t.next(); err != nil {
					return -1, nil, "", err
				}
			}
		}
	}

	registerItem := false
	if stage == cStageRegisterTerms {
		registerItem = true
	}

	t.lastMessage = line

	tokens, err := t.toTermList(line, lastUpdate, registerItem)
	if err != nil {
		return -1, nil, "", err
	}

	t.countByBlock++

	switch stage {
	case cStageRegisterTerms:
		if t.currRetentionPos > 0 && retentionPos > t.currRetentionPos {
			if t.countByBlock > t.maxCountByBlock {
				t.maxCountByBlock = t.countByBlock
			}
			t.countByBlock = 0
		}
		t.totalLines++
	case cStageRegisterPT:
		t.registerPt(tokens, 1)
	case cStageRegisterPhrases:
		if !t.ptRegistered {
			return -1, nil, "", errors.New("phrase tree not registered")
		}
		phraseID := -1
		phraseID, phrasestr = t.registerPhrase(tokens, lastUpdate, orgLine, 1, minMatchRate, maxMatchRate, false)
		phraseCnt = t.phrases.getCount(phraseID)
	default:
		phraseID := -1
		phraseID, phrasestr = t.registerPhrase(tokens, lastUpdate, orgLine, 0, minMatchRate, maxMatchRate, false)
		phraseCnt = t.phrases.getCount(phraseID)
	}

	t.currRetentionPos = retentionPos

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
	if t.filterRe == nil && t.xFilterRe == nil {
		return true
	}

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
	showLastText bool,
	termCountBorderRate float64, termCountBorder int,
	minMatchRate, maxMatchRate float64) []phraseScore {

	if err := t.rearangePhrases(termCountBorderRate, termCountBorder,
		minMatchRate, maxMatchRate); err != nil {
		return nil
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

	_, token, phrasestr, err := t.tokenizeLine(line, 0, cStageElse, 1.0, 0.0)
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

func (t *trans) OLDstr2Tokens(line string) []int {
	words := strings.Split(line, " ")
	//phrasestr := ""
	tokens := make([]int, 0)
	longtokens := make([]int, 0)
	excluded_words := make(map[string]string, 0)

	for _, term := range words {
		termID := t.terms.getItemID(term)
		termCnt := t.terms.getCount(termID)

		//if termCnt >= t.termCountBorder || (len(tokens) > 0 && termID == cAsteriskItemID && tokens[len(tokens)-1] != cAsteriskItemID) {
		if termCnt >= t.termCountBorder || (len(tokens) > 0 && termID == cAsteriskItemID) {
			tokens = append(tokens, termID)
			//} else if len(tokens) > 0 && termCnt < t.termCountBorder && tokens[len(tokens)-1] != cAsteriskItemID {
		} else if len(tokens) > 0 && termCnt < t.termCountBorder {
			tokens = append(tokens, cAsteriskItemID)
		} else {
			excluded_words[term] = ""
		}
		longtokens = append(longtokens, termID)
	}

	if len(tokens) < 3 {
		tokens = longtokens
	}
	return tokens
}

func (t *trans) rearangePhrases(termCountBorderRate float64, termCountBorder int,
	minMatchRate, maxMatchRate float64) error {

	if termCountBorderRate <= 0 && termCountBorder <= 0 {
		return nil
	}

	p, err := newItems("", "rearranged_phrase", 0, 0, "", false)
	if err != nil {
		return err
	}

	t.subjects = make(map[int]string)

	// if new termCountBorder is equal or less than current, there will be no change
	oldTermCountBorder := t.termCountBorder
	t.calcCountBorder(termCountBorderRate, termCountBorder)
	if t.termCountBorder <= oldTermCountBorder {
		return nil
	}

	t.pt = &phraseTree{
		count:      0,
		childNodes: nil,
		depth:      0,
	}

	t.orgPhrases = t.phrases
	t.phrases = p

	t.resetCustomPhrases()

	for _, stage := range []int{cStageRegisterPT, cStageRegisterPhrases} {
		for phraseID, line := range t.orgPhrases.memberMap {
			cnt := t.orgPhrases.getCount(phraseID)
			lastUpdate := t.orgPhrases.getLastUpdate(phraseID)
			lastValue := t.orgPhrases.getLastValue(phraseID)

			tokens, err := t.toTermList(line, 0, false)
			if err != nil {
				return err
			}
			if len(tokens) == 0 {
				return errors.New("found null phrase")
			}

			switch stage {
			case cStageRegisterPT:
				t.registerPt(tokens, cnt)
			case cStageRegisterPhrases:
				t.registerPhrase(tokens, lastUpdate, lastValue, cnt, minMatchRate, maxMatchRate, true)
				//_, phrasestr := t.registerPhrase(tokens, lastUpdate, lastValue, cnt, 0, 0)
				//expected := "invite sip * user phone * udp sip 2.0 * application * sip * user phone from * sip * user phone tag * sip * user phone * max-forwards allow invite ack options bye cancel update * supported * timer * call-id * cseq invite user-agent tbsip contact sip * via sip 2.0 udp * branch * content-length * ip4 * ip4 * rtpmap * --- sip 2.0 * via sip 2.0 udp * branch * sip * user phone from * sip * user phone tag * call-id * cseq invite server * content-length --- sip 2.0 * via sip 2.0 udp * branch * sip 127.0.0.1 ftag * did * sip * ftag * did * sip * from * sip * user phone tag * sip * user phone tag * call-id * cseq invite contact sip * udp user-agent * application * allow invite ack bye cancel options * update * timer supported timer * application * content-length * sip * ip4 * ip4 * rtpmap * --- ack sip * udp sip 2.0 sip * user phone tag * from * sip * user phone tag * max-forwards cseq ack call-id * sip * sip * ftag * did * sip 127.0.0.1 ftag * did * user-agent tbsip via sip 2.0 udp * branch * content-length --- bye sip * udp sip 2.0 sip * user phone tag * from * sip * user phone tag * call-id * cseq bye * sip * sip * ftag * did * sip 127.0.0.1 ftag * did * max-forwards user-agent tbsip via sip 2.0 udp * branch * content-length --- sip 2.0 * via sip 2.0 udp * branch * from * sip * user phone tag * sip * user phone tag * call-id * cseq bye user-agent * allow invite ack bye cancel options * update * supported timer * content-length"
				//if phrasestr == expected {
				//	println(t.orgPhrases.memberMap[phraseID])
				//}
			}
		}

		if cStageRegisterPT == 1 {
			t.ptRegistered = true
		}
	}

	//t.orgPhrases = t.phrases
	//t.phrases = p

	if err := t.calcPhrasesScore(); err != nil {
		return err
	}

	return nil
}

func (t *trans) outputPhrases(termCountBorderRate float64, termCountBorder int,
	biggestN int,
	minMatchRate, maxMatchRate float64,
	delim, outfile string) error {

	if err := t.rearangePhrases(termCountBorderRate, termCountBorder,
		minMatchRate, maxMatchRate); err != nil {
		return err
	}

	phraseRanks := t.phrases.biggestNItems(biggestN)
	rankMap := make(map[int]int)
	for _, phraseID := range phraseRanks {
		rankMap[phraseID] = t.phrases.getCount(phraseID)
	}

	var writer *csv.Writer
	if outfile == "" {
		writer = csv.NewWriter(os.Stdout)
	} else {
		file, err := os.Create(outfile)
		if err != nil {
			return fmt.Errorf("error creating file: %w", err)
		}
		defer file.Close()
		writer = csv.NewWriter(file)
	}
	if delim == "" {
		delim = ","
	}
	writer.Comma = rune(delim[0])
	defer writer.Flush()

	// Add the header row
	header := []string{"Created", "Updated", "Count", "Member"}
	if outfile == "" {
		// Print header to stdout
		fmt.Printf("%-15s %-15s %-10s %-30s\n", header[0], header[1], header[2], header[3])
		fmt.Printf("%-15s %-15s %-10s %-30s\n",
			strings.Repeat("-", 15),
			strings.Repeat("-", 15),
			strings.Repeat("-", 10),
			strings.Repeat("-", 30))
	} else {
		writer.Write(header)
	}

	format := "2006-01-02 15:04:05"
	for _, phraseID := range phraseRanks {
		line := t.phrases.memberMap[phraseID]

		if !t.match(line) {
			continue
		}

		var row []string

		//time.Unix(ep, 0).Format(format)
		row = append(row, time.Unix(t.phrases.getCreateEpoch(phraseID), 0).Format(format))
		row = append(row, time.Unix(t.phrases.getLastUpdate(phraseID), 0).Format(format))
		row = append(row, strconv.Itoa(int(t.phrases.getCount(phraseID))))
		//row = append(row, t.phrases.getMember(phraseID))
		row = append(row, t.subjects[phraseID])

		if outfile == "" {
			// Pretty print each row to stdout
			fmt.Printf("%-15s %-15s %-10s %-30s\n", row[0], row[1], row[2], row[3])
		} else {
			writer.Write(row)
		}
	}

	return nil
}

func (t *trans) outputPhrasesHistory(
	termCountBorderRate float64, termCountBorder int,
	minMatchRate, maxMatchRate float64,
	biggestN int,
	delim, outdir string) error {

	unitsecs := utils.GetUnitsecs(t.frequency)
	format := utils.GetDatetimeFormat(t.frequency)

	if err := t.rearangePhrases(termCountBorderRate, termCountBorder,
		minMatchRate, maxMatchRate); err != nil {
		return err
	}

	phraseRanks := t.phrases.biggestNItems(biggestN)
	rankMap := make(map[int]int)
	for _, phraseID := range phraseRanks {
		rankMap[phraseID] = t.phrases.getCount(phraseID)
	}

	attrs := make(map[int]map[int64]int, 0)

	// phrase item to read database
	var p *items
	if t.orgPhrases != nil {
		p = t.orgPhrases
	} else {
		p = t.phrases
	}
	rows, err := p.SelectRows(nil, nil, tableDefs["items"])
	if err != nil {
		return err
	}
	if rows == nil {
		return nil
	}

	minTime := int64(0)
	maxTime := int64(0)
	first := true
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

		//expected := "invite sip * user phone transport udp sip 2.0 content-type application sdp sip * user phone from * sip * user phone tag * x-pai sip cpc * user phone tel cpc ordinary max-forwards allow invite ack options bye cancel update prack supported 100rel timer session-expires 300 min-se 300 call-id * cseq invite user-agent tbsip contact sip * via sip 2.0 udp 202.173.5.114 branch * content-length 137 * ip4 202.173.5.114 * ip4 202.173.5.114 audio rtp avp * sendrecv * rtpmap pcmu --- sip 2.0 100 giving * try via sip 2.0 udp 202.173.5.114 rport branch * sip * user phone from * sip * user phone tag * call-id * cseq invite server opensips 3.2.9 x86_64 linux content-length --- sip 2.0 200 via sip 2.0 udp 202.173.5.114 rport branch * record-route sip 127.0.0.1 ftag * did * record-route sip 202.173.5.209 ftag * did * record-route sip 202.173.5.198 fend yes from * sip * user phone tag * sip * user phone tag * call-id * cseq invite contact sip * transport udp user-agent freeswitch accept application sdp allow invite ack bye cancel options message info update notify require timer supported timer path replaces allow-events talk hold conference refer session-expires 300 refresher uac content-type application sdp content-disposition session content-length 166 remote-party-id * sip * party calling privacy off screen freeswitch ip4 202.173.5.209 freeswitch ip4 202.173.5.209 audio rtp avp * rtpmap pcmu * ptime --- ack sip * transport udp sip 2.0 sip * user phone tag * from * sip * user phone tag * max-forwards cseq ack call-id * route sip 202.173.5.198 fend yes route sip 202.173.5.209 ftag * did * route sip 127.0.0.1 ftag * did * user-agent tbsip via sip 2.0 udp 202.173.5.114 branch * content-length --- bye sip * transport udp sip 2.0 sip * user phone tag * from * sip * user phone tag * call-id * cseq bye route sip 202.173.5.198 fend yes route sip 202.173.5.209 ftag * did * route sip 127.0.0.1 ftag * did * max-forwards user-agent tbsip via sip 2.0 udp 202.173.5.114 branch * content-length --- sip 2.0 200 via sip 2.0 udp 202.173.5.114 rport branch * from * sip * user phone tag * sip * user phone tag * call-id * cseq bye user-agent freeswitch allow invite ack bye cancel options message info update notify supported timer path replaces content-length"
		//if item == expected {
		//	println("here")
		//}

		tokens, err := t.toTermList(item, lastUpdate, false)
		if err != nil {
			return err
		}

		phraseID, _ := t.registerPhrase(tokens, lastUpdate, lastValue, 0, minMatchRate, maxMatchRate, true)
		if _, ok := rankMap[phraseID]; !ok {
			continue
		}

		maxTime = createEpoch / unitsecs * unitsecs
		if _, ok := attrs[phraseID]; !ok {
			attrs[phraseID] = make(map[int64]int, 0)
		}
		if _, ok := attrs[phraseID][maxTime]; !ok {
			attrs[phraseID][maxTime] = 0
		}
		attrs[phraseID][maxTime] += itemCount
		if first {
			minTime = maxTime
			first = false
		}
	}

	// test if the count is correct
	for i, phraseID := range phraseRanks {
		attr := attrs[phraseID]
		phraseCnt := rankMap[phraseID]
		sum := 0
		for _, cnt := range attr {
			sum += cnt
		}
		if phraseCnt != sum {
			//return fmt.Errorf("count of phrase does not much. rank index=%d", i)
			fmt.Printf("count of phrase does not much. rank index=%d", i)
		}
	}

	var writer *csv.Writer
	if outdir == "" {
		writer = csv.NewWriter(os.Stdout)
	} else {
		if err := utils.EnsureDir(outdir); err != nil {
			return err
		}
		file, err := os.Create(fmt.Sprintf("%s/history.csv", outdir))
		if err != nil {
			return fmt.Errorf("error creating file: %w", err)
		}
		defer file.Close()
		writer = csv.NewWriter(file)
	}
	if delim == "" {
		delim = ","
	}
	writer.Comma = rune(delim[0])
	defer writer.Flush()

	// Pretty print header (adjust for pretty output to stdout)
	if outdir == "" {
		fmt.Printf("%-20s", "time")
		for i := range phraseRanks {
			fmt.Printf("%-15s", strconv.Itoa(i+1)+".count")
		}
		fmt.Println()
	} else {
		// Write CSV header for file output
		header := []string{"time"}
		for i, phraseID := range phraseRanks {
			header = append(header, fmt.Sprintf("%d(%d)", i+1, t.phrases.getCount(phraseID)))
		}
		writer.Write(header)
	}

	ep := minTime
	if outdir == "" {
		for ep <= maxTime {
			row := []string{time.Unix(ep, 0).Format(format)}
			fmt.Printf("%-20s", row[0])
			for _, phraseID := range phraseRanks {
				count, ok := attrs[phraseID][ep]
				if ok {
					fmt.Printf("%-15s", strconv.Itoa(count))
				} else {
					fmt.Printf("%-15s", "")
				}
			}
			fmt.Print("\n")
			ep += unitsecs
		}
	} else {
		for ep <= maxTime {
			row := []string{time.Unix(ep, 0).Format(format)}
			for _, phraseID := range phraseRanks {
				count, ok := attrs[phraseID][ep]
				if ok {
					row = append(row, strconv.Itoa(count))
				} else {
					row = append(row, "")
				}
			}
			writer.Write(row)
			ep += unitsecs
		}
	}

	if outdir == "" {
		fmt.Printf("\n")
		for i, phraseID := range phraseRanks {
			subject := t.subjects[phraseID]
			if len(subject) > 200 {
				subject = subject[:200]
			}
			cnt := t.phrases.getCount(phraseID)
			//fmt.Printf("%d.phrase: %s\n", i+1, phrase)
			fmt.Printf("%d(%d): %s\n", i+1, cnt, subject)
		}
	} else {
		file, err := os.Create(fmt.Sprintf("%s/subjects.txt", outdir))
		if err != nil {
			return fmt.Errorf("error creating file: %w", err)
		}
		defer file.Close()
		writer = csv.NewWriter(file)
		if delim == "" {
			delim = ","
		}
		writer.Comma = rune(delim[0])
		defer writer.Flush()
		for i, phraseID := range phraseRanks {
			subject := t.subjects[phraseID]
			cnt := t.phrases.getCount(phraseID)
			row := []string{fmt.Sprintf("%d(%d)", i+1, cnt), subject}
			writer.Write(row)
		}

	}

	return nil
}
