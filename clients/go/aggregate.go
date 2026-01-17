package chroma

import (
	"encoding/json"

	"github.com/pkg/errors"
)

// Aggregate represents an aggregation operation for GroupBy.
type Aggregate interface {
	MarshalJSON() ([]byte, error)
	Validate() error
}

// MinK selects k records with the smallest values (ascending order).
// Use when lower values are better (e.g., distance scores, prices).
type MinK struct {
	Keys []Key
	K    int
}

// NewMinK creates a MinK aggregation that selects k records with smallest values.
//
// Example:
//
//	NewMinK(3, KScore)                 // Top 3 lowest scores
//	NewMinK(2, K("priority"), KScore)  // With tiebreaker
func NewMinK(k int, keys ...Key) *MinK {
	return &MinK{Keys: keys, K: k}
}

func (m *MinK) Validate() error {
	if m.K < 1 {
		return errors.New("k must be >= 1")
	}
	if len(m.Keys) == 0 {
		return errors.New("at least one key is required")
	}
	return nil
}

func (m *MinK) MarshalJSON() ([]byte, error) {
	keys := make([]string, len(m.Keys))
	for i, k := range m.Keys {
		keys[i] = string(k)
	}
	return json.Marshal(map[string]any{
		"$min_k": map[string]any{
			"keys": keys,
			"k":    m.K,
		},
	})
}

// MaxK selects k records with the largest values (descending order).
// Use when higher values are better (e.g., ratings, relevance scores).
type MaxK struct {
	Keys []Key
	K    int
}

// NewMaxK creates a MaxK aggregation that selects k records with largest values.
//
// Example:
//
//	NewMaxK(3, K("rating"))            // Top 3 highest ratings
//	NewMaxK(2, K("year"), K("rating")) // With tiebreaker
func NewMaxK(k int, keys ...Key) *MaxK {
	return &MaxK{Keys: keys, K: k}
}

func (m *MaxK) Validate() error {
	if m.K < 1 {
		return errors.New("k must be >= 1")
	}
	if len(m.Keys) == 0 {
		return errors.New("at least one key is required")
	}
	return nil
}

func (m *MaxK) MarshalJSON() ([]byte, error) {
	keys := make([]string, len(m.Keys))
	for i, k := range m.Keys {
		keys[i] = string(k)
	}
	return json.Marshal(map[string]any{
		"$max_k": map[string]any{
			"keys": keys,
			"k":    m.K,
		},
	})
}
