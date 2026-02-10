package bm25

import (
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/kljensen/snowball"
)

// nonAlphanumericRegex matches non-letter/non-number characters.
// MustCompile is safe here: the pattern is a compile-time constant that will never fail.
var nonAlphanumericRegex = regexp.MustCompile(`[^\p{L}\p{N}]+`)

// Tokenizer handles text tokenization and stemming for BM25
type Tokenizer struct {
	stopwords      map[string]struct{}
	tokenMaxLength int
}

// NewTokenizer creates a new tokenizer with the given stopwords and max token length
func NewTokenizer(stopwords []string, tokenMaxLength int) *Tokenizer {
	sw := make(map[string]struct{}, len(stopwords))
	for _, word := range stopwords {
		sw[strings.ToLower(word)] = struct{}{}
	}
	return &Tokenizer{
		stopwords:      sw,
		tokenMaxLength: tokenMaxLength,
	}
}

// Tokenize processes text and returns stemmed tokens.
// It performs: lowercase -> split -> filter stopwords -> filter by length -> stem
func (t *Tokenizer) Tokenize(text string) []string {
	text = strings.ToLower(text)
	text = nonAlphanumericRegex.ReplaceAllString(text, " ")
	words := strings.Fields(text)

	tokens := make([]string, 0, len(words))
	for _, word := range words {
		if word == "" {
			continue
		}
		if _, isStopword := t.stopwords[word]; isStopword {
			continue
		}
		if utf8.RuneCountInString(word) > t.tokenMaxLength {
			continue
		}
		stemmed, err := snowball.Stem(word, "english", true)
		if err != nil {
			stemmed = word
		}
		tokens = append(tokens, stemmed)
	}
	return tokens
}
