package bm25

import (
	"github.com/pkg/errors"
)

const (
	defaultK              = 1.2
	defaultB              = 0.75
	defaultAvgDocLength   = 256.0
	defaultTokenMaxLength = 40
)

// Option is a function that configures a Client
type Option func(c *Client) error

// WithK sets the BM25 saturation parameter (default: 1.2)
// K=0 disables term frequency saturation (raw TF)
func WithK(k float64) Option {
	return func(c *Client) error {
		if k < 0 {
			return errors.New("k must be non-negative")
		}
		c.K = k
		c.kSet = true
		return nil
	}
}

// WithB sets the BM25 document length normalization parameter (default: 0.75)
// B=0 disables document length normalization
func WithB(b float64) Option {
	return func(c *Client) error {
		if b < 0 || b > 1 {
			return errors.New("b must be between 0 and 1")
		}
		c.B = b
		c.bSet = true
		return nil
	}
}

// WithAvgDocLength sets the expected average document length (default: 256.0)
func WithAvgDocLength(avgDocLength float64) Option {
	return func(c *Client) error {
		if avgDocLength <= 0 {
			return errors.New("avgDocLength must be positive")
		}
		c.AvgDocLength = avgDocLength
		return nil
	}
}

// WithTokenMaxLength sets the maximum token character length (default: 40)
func WithTokenMaxLength(maxLength int) Option {
	return func(c *Client) error {
		if maxLength <= 0 {
			return errors.New("tokenMaxLength must be positive")
		}
		c.TokenMaxLength = maxLength
		return nil
	}
}

// WithStopwords sets custom stopwords (default: DefaultStopwords)
func WithStopwords(stopwords []string) Option {
	return func(c *Client) error {
		c.Stopwords = stopwords
		return nil
	}
}

// WithIncludeTokens enables including token labels in the output (default: false)
func WithIncludeTokens(include bool) Option {
	return func(c *Client) error {
		c.IncludeTokens = include
		return nil
	}
}

func applyDefaults(c *Client) {
	if !c.kSet {
		c.K = defaultK
	}
	if !c.bSet {
		c.B = defaultB
	}
	if c.AvgDocLength == 0 {
		c.AvgDocLength = defaultAvgDocLength
	}
	if c.TokenMaxLength == 0 {
		c.TokenMaxLength = defaultTokenMaxLength
	}
	if c.Stopwords == nil {
		c.Stopwords = DefaultStopwords
	}
}
