package rarelogdetector

import (
	"goRareLogDetector/pkg/utils"
	"math"
	"regexp"
	"strings"
	"time"
)

type trans struct {
	terms           *items
	phrases         *items
	replacer        *strings.Replacer
	logFormatRe     *regexp.Regexp
	timestampLayout string
	timestampPos    int
	messagePos      int
}

func newTrans(dataDir, logFormat, timestampLayout string,
	maxBlocks, blockSize int, useGzip bool) (*trans, error) {
	t := new(trans)
	te, err := newItems(dataDir, "terms", maxBlocks, useGzip)
	if err != nil {
		return nil, err
	}
	p, err := newItems(dataDir, "phrases", maxBlocks, useGzip)
	if err != nil {
		return nil, err
	}

	t.terms = te
	t.phrases = p
	t.replacer = getDelimReplacer()
	t.parseLogFormat(logFormat)
	t.timestampLayout = timestampLayout
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
	return nil
}

func (t *trans) commit(completed bool) error {
	if err := t.terms.commit(completed); err != nil {
		return err
	}
	if err := t.phrases.commit(completed); err != nil {
		return err
	}
	return nil
}

func (t *trans) registerPhrase(tokens []int, lastUpdate int64, registerItem bool) []int {
	te := t.terms
	scores := make([]float64, len(tokens))
	phrase := make([]int, 0)

	sum := 0.0
	for i, termID := range tokens {
		scores[i] = te.getIdf(termID)
		sum += scores[i]
	}
	average := sum / float64(len(scores))

	varianceSum := 0.0
	for _, score := range scores {
		varianceSum += (score - average) * (score - average)
	}
	variance := varianceSum / float64(len(scores))
	s := math.Sqrt(variance)

	if s == 0 {
		phrase = tokens
	} else {
		for i, score := range scores {
			if (score-average)/s <= cGapInPhrases {
				phrase = append(phrase, tokens[i])
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
	t.phrases.register(phrasestr, addCnt, lastUpdate, registerItem)
	return phrase
}

func (t *trans) toTermList(line string, lastUpdate int64, registerItem bool) ([]int, error) {
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
			termID = t.terms.register(word, addCnt, lastUpdate, registerItem)
			tokens = append(tokens, termID)
		}
	}

	return tokens, nil
}

func (t *trans) tokenizeLine(line string, registerItem bool) error {
	lastUpdate := int64(0)
	if t.timestampPos >= 0 || t.messagePos >= 0 {
		match := t.logFormatRe.FindStringSubmatch(line)
		if t.timestampPos >= 0 && t.timestampLayout != "" {
			lastUpdate, _ = utils.Str2Epoch(t.timestampLayout, match[t.timestampPos])
		}
		if t.messagePos >= 0 {
			line = match[t.messagePos]
		}
	}
	if lastUpdate == 0 {
		lastUpdate = time.Now().Unix()
	}

	tokens, err := t.toTermList(line, lastUpdate, registerItem)
	if err != nil {
		return err
	}
	t.registerPhrase(tokens, lastUpdate, registerItem)
	return nil
}

// Rotate phrases and terms together to remove oldest items in the same timeline
func (t *trans) next() error {
	if err := t.phrases.next(); err != nil {
		return err
	}
	if err := t.terms.next(); err != nil {
		return err
	}
	return nil
}
