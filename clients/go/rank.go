package chroma

import (
	"encoding/json"
	"math"

	"github.com/pkg/errors"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

// Operand is an interface for values that can participate in rank expressions.
// Concrete types include [IntOperand], [FloatOperand], and all [Rank] implementations.
type Operand interface {
	IsOperand()
}

// IntOperand wraps an integer for use in rank arithmetic expressions.
//
// Example:
//
//	rank := Val(1.0).Add(IntOperand(5))  // Produces: {"$sum": [{"$val": 1}, {"$val": 5}]}
type IntOperand int64

func (i IntOperand) IsOperand() {}

// FloatOperand wraps a float for use in rank arithmetic expressions.
//
// Example:
//
//	rank := NewKnnRank(KnnQueryText("query")).Multiply(FloatOperand(0.7))
type FloatOperand float64

func (f FloatOperand) IsOperand() {}

// Rank is the core interface for building ranking expressions.
//
// Rank expressions are composable through arithmetic operations (Add, Sub, Multiply, Div)
// and mathematical functions (Abs, Exp, Log, Max, Min). Each operation returns a new Rank,
// enabling fluent method chaining.
//
// Example - weighted combination of two KNN searches:
//
//	rank := NewKnnRank(KnnQueryText("machine learning")).
//	    Multiply(FloatOperand(0.7)).
//	    Add(NewKnnRank(KnnQueryText("deep learning")).Multiply(FloatOperand(0.3)))
//
// Example - log compression with offset:
//
//	rank := NewKnnRank(KnnQueryText("query")).Add(FloatOperand(1)).Log()
type Rank interface {
	Operand
	Multiply(operand Operand) Rank
	Sub(operand Operand) Rank
	Add(operand Operand) Rank
	Div(operand Operand) Rank
	Negate() Rank
	Abs() Rank
	Exp() Rank
	Log() Rank
	Max(operand Operand) Rank
	Min(operand Operand) Rank
	MarshalJSON() ([]byte, error)
	UnmarshalJSON(b []byte) error
}

// RankWithWeight pairs a Rank with a weight for use in Reciprocal Rank Fusion (RRF).
//
// Create using the WithWeight method on KnnRank:
//
//	knn, _ := NewKnnRank(KnnQueryText("query"), WithKnnReturnRank())
//	weighted := knn.WithWeight(0.5)
type RankWithWeight struct {
	Rank   Rank
	Weight float64
}

// UnknownRank is a sentinel type returned by operandToRank when an unknown
// operand type is encountered. It errors on MarshalJSON to surface programming
// errors instead of silently producing incorrect results.
type UnknownRank struct {
	ValRank
}

func (u *UnknownRank) MarshalJSON() ([]byte, error) {
	return nil, errors.New("UnknownRank: cannot marshal unknown operand type - this indicates a programming error")
}

// ValRank represents a constant numeric value in rank expressions.
// Serializes to JSON as {"$val": <value>}.
type ValRank struct {
	value float64
}

// Val creates a constant value rank expression.
//
// Example:
//
//	// Add a constant offset to KNN scores
//	rank := NewKnnRank(KnnQueryText("query")).Add(Val(1.0))
//
//	// Create a weighted scalar
//	rank := Val(0.5).Multiply(NewKnnRank(KnnQueryText("query")))
func Val(value float64) *ValRank {
	return &ValRank{value: value}
}

func (v *ValRank) IsOperand() {}

func (v *ValRank) Multiply(operand Operand) Rank {
	return &MulRank{ranks: []Rank{v, operandToRank(operand)}}
}

func (v *ValRank) Sub(operand Operand) Rank {
	return &SubRank{left: v, right: operandToRank(operand)}
}

func (v *ValRank) Add(operand Operand) Rank {
	return &SumRank{ranks: []Rank{v, operandToRank(operand)}}
}

func (v *ValRank) Div(operand Operand) Rank {
	return &DivRank{left: v, right: operandToRank(operand)}
}

func (v *ValRank) Negate() Rank {
	return &MulRank{ranks: []Rank{Val(-1), v}}
}

func (v *ValRank) Abs() Rank {
	return &AbsRank{rank: v}
}

func (v *ValRank) Exp() Rank {
	return &ExpRank{rank: v}
}

func (v *ValRank) Log() Rank {
	return &LogRank{rank: v}
}

func (v *ValRank) Max(operand Operand) Rank {
	return &MaxRank{ranks: []Rank{v, operandToRank(operand)}}
}

func (v *ValRank) Min(operand Operand) Rank {
	return &MinRank{ranks: []Rank{v, operandToRank(operand)}}
}

func (v *ValRank) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]float64{"$val": v.value})
}

func (v *ValRank) UnmarshalJSON(b []byte) error {
	return errors.New("ValRank: unmarshaling is not supported")
}

// SumRank represents the addition of multiple rank expressions.
// Serializes to JSON as {"$sum": [...]}.
type SumRank struct {
	ranks []Rank
}

func (s *SumRank) IsOperand() {}

func (s *SumRank) Multiply(operand Operand) Rank {
	return &MulRank{ranks: []Rank{s, operandToRank(operand)}}
}

func (s *SumRank) Sub(operand Operand) Rank {
	return &SubRank{left: s, right: operandToRank(operand)}
}

func (s *SumRank) Add(operand Operand) Rank {
	r := operandToRank(operand)
	newRanks := make([]Rank, len(s.ranks))
	copy(newRanks, s.ranks)
	if sum, ok := r.(*SumRank); ok {
		return &SumRank{ranks: append(newRanks, sum.ranks...)}
	}
	return &SumRank{ranks: append(newRanks, r)}
}

func (s *SumRank) Div(operand Operand) Rank {
	return &DivRank{left: s, right: operandToRank(operand)}
}

func (s *SumRank) Negate() Rank {
	return &MulRank{ranks: []Rank{Val(-1), s}}
}

func (s *SumRank) Abs() Rank {
	return &AbsRank{rank: s}
}

func (s *SumRank) Exp() Rank {
	return &ExpRank{rank: s}
}

func (s *SumRank) Log() Rank {
	return &LogRank{rank: s}
}

func (s *SumRank) Max(operand Operand) Rank {
	return &MaxRank{ranks: []Rank{s, operandToRank(operand)}}
}

func (s *SumRank) Min(operand Operand) Rank {
	return &MinRank{ranks: []Rank{s, operandToRank(operand)}}
}

func (s *SumRank) MarshalJSON() ([]byte, error) {
	if len(s.ranks) > MaxExpressionTerms {
		return nil, errors.Errorf("sum expression exceeds maximum of %d terms", MaxExpressionTerms)
	}
	rankMaps := make([]json.RawMessage, len(s.ranks))
	for i, r := range s.ranks {
		data, err := r.MarshalJSON()
		if err != nil {
			return nil, err
		}
		rankMaps[i] = data
	}
	return json.Marshal(map[string][]json.RawMessage{"$sum": rankMaps})
}

func (s *SumRank) UnmarshalJSON(_ []byte) error {
	return errors.New("SumRank: unmarshaling is not supported")
}

// SubRank represents subtraction of two rank expressions.
// Serializes to JSON as {"$sub": {"left": ..., "right": ...}}.
type SubRank struct {
	left  Rank
	right Rank
}

func (s *SubRank) IsOperand() {}

func (s *SubRank) Multiply(operand Operand) Rank {
	return &MulRank{ranks: []Rank{s, operandToRank(operand)}}
}

func (s *SubRank) Sub(operand Operand) Rank {
	return &SubRank{left: s, right: operandToRank(operand)}
}

func (s *SubRank) Add(operand Operand) Rank {
	return &SumRank{ranks: []Rank{s, operandToRank(operand)}}
}

func (s *SubRank) Div(operand Operand) Rank {
	return &DivRank{left: s, right: operandToRank(operand)}
}

func (s *SubRank) Negate() Rank {
	return &MulRank{ranks: []Rank{Val(-1), s}}
}

func (s *SubRank) Abs() Rank {
	return &AbsRank{rank: s}
}

func (s *SubRank) Exp() Rank {
	return &ExpRank{rank: s}
}

func (s *SubRank) Log() Rank {
	return &LogRank{rank: s}
}

func (s *SubRank) Max(operand Operand) Rank {
	return &MaxRank{ranks: []Rank{s, operandToRank(operand)}}
}

func (s *SubRank) Min(operand Operand) Rank {
	return &MinRank{ranks: []Rank{s, operandToRank(operand)}}
}

func (s *SubRank) MarshalJSON() ([]byte, error) {
	leftData, err := s.left.MarshalJSON()
	if err != nil {
		return nil, err
	}
	rightData, err := s.right.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]map[string]json.RawMessage{
		"$sub": {"left": leftData, "right": rightData},
	})
}

func (s *SubRank) UnmarshalJSON(_ []byte) error {
	return errors.New("SubRank: unmarshaling is not supported")
}

// MulRank represents multiplication of multiple rank expressions.
// Serializes to JSON as {"$mul": [...]}.
type MulRank struct {
	ranks []Rank
}

func (m *MulRank) IsOperand() {}

func (m *MulRank) Multiply(operand Operand) Rank {
	r := operandToRank(operand)
	newRanks := make([]Rank, len(m.ranks))
	copy(newRanks, m.ranks)
	if mul, ok := r.(*MulRank); ok {
		return &MulRank{ranks: append(newRanks, mul.ranks...)}
	}
	return &MulRank{ranks: append(newRanks, r)}
}

func (m *MulRank) Sub(operand Operand) Rank {
	return &SubRank{left: m, right: operandToRank(operand)}
}

func (m *MulRank) Add(operand Operand) Rank {
	return &SumRank{ranks: []Rank{m, operandToRank(operand)}}
}

func (m *MulRank) Div(operand Operand) Rank {
	return &DivRank{left: m, right: operandToRank(operand)}
}

func (m *MulRank) Negate() Rank {
	return &MulRank{ranks: []Rank{Val(-1), m}}
}

func (m *MulRank) Abs() Rank {
	return &AbsRank{rank: m}
}

func (m *MulRank) Exp() Rank {
	return &ExpRank{rank: m}
}

func (m *MulRank) Log() Rank {
	return &LogRank{rank: m}
}

func (m *MulRank) Max(operand Operand) Rank {
	return &MaxRank{ranks: []Rank{m, operandToRank(operand)}}
}

func (m *MulRank) Min(operand Operand) Rank {
	return &MinRank{ranks: []Rank{m, operandToRank(operand)}}
}

func (m *MulRank) MarshalJSON() ([]byte, error) {
	if len(m.ranks) > MaxExpressionTerms {
		return nil, errors.Errorf("mul expression exceeds maximum of %d terms", MaxExpressionTerms)
	}
	rankMaps := make([]json.RawMessage, len(m.ranks))
	for i, r := range m.ranks {
		data, err := r.MarshalJSON()
		if err != nil {
			return nil, err
		}
		rankMaps[i] = data
	}
	return json.Marshal(map[string][]json.RawMessage{"$mul": rankMaps})
}

func (m *MulRank) UnmarshalJSON(_ []byte) error {
	return errors.New("MulRank: unmarshaling is not supported")
}

// DivRank represents division of two rank expressions.
// Serializes to JSON as {"$div": {"left": ..., "right": ...}}.
//
// NOTE: Division by zero validation only catches literal zero denominators (Val(0)).
// Complex expressions that evaluate to zero at runtime (e.g., Val(1).Sub(Val(1)))
// will produce Inf/NaN on the server following NumPy semantics.
// Use epsilon values when dividing by potentially zero expressions.
type DivRank struct {
	left  Rank
	right Rank
}

func (d *DivRank) IsOperand() {}

func (d *DivRank) Multiply(operand Operand) Rank {
	return &MulRank{ranks: []Rank{d, operandToRank(operand)}}
}

func (d *DivRank) Sub(operand Operand) Rank {
	return &SubRank{left: d, right: operandToRank(operand)}
}

func (d *DivRank) Add(operand Operand) Rank {
	return &SumRank{ranks: []Rank{d, operandToRank(operand)}}
}

func (d *DivRank) Div(operand Operand) Rank {
	return &DivRank{left: d, right: operandToRank(operand)}
}

func (d *DivRank) Negate() Rank {
	return &MulRank{ranks: []Rank{Val(-1), d}}
}

func (d *DivRank) Abs() Rank {
	return &AbsRank{rank: d}
}

func (d *DivRank) Exp() Rank {
	return &ExpRank{rank: d}
}

func (d *DivRank) Log() Rank {
	return &LogRank{rank: d}
}

func (d *DivRank) Max(operand Operand) Rank {
	return &MaxRank{ranks: []Rank{d, operandToRank(operand)}}
}

func (d *DivRank) Min(operand Operand) Rank {
	return &MinRank{ranks: []Rank{d, operandToRank(operand)}}
}

func (d *DivRank) MarshalJSON() ([]byte, error) {
	// Check for division by zero literal.
	// NOTE: Only catches literal Val(0). Complex expressions like Val(1).Sub(Val(1))
	// are not detected; the server will return Inf/NaN following NumPy semantics.
	if v, ok := d.right.(*ValRank); ok && v.value == 0 {
		return nil, errors.New("division by zero: denominator is a zero literal")
	}

	leftData, err := d.left.MarshalJSON()
	if err != nil {
		return nil, err
	}
	rightData, err := d.right.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]map[string]json.RawMessage{
		"$div": {"left": leftData, "right": rightData},
	})
}

func (d *DivRank) UnmarshalJSON(_ []byte) error {
	return errors.New("DivRank: unmarshaling is not supported")
}

// AbsRank represents the absolute value of a rank expression.
// Serializes to JSON as {"$abs": <rank>}.
type AbsRank struct {
	rank Rank
}

func (a *AbsRank) IsOperand() {}

func (a *AbsRank) Multiply(operand Operand) Rank {
	return &MulRank{ranks: []Rank{a, operandToRank(operand)}}
}

func (a *AbsRank) Sub(operand Operand) Rank {
	return &SubRank{left: a, right: operandToRank(operand)}
}

func (a *AbsRank) Add(operand Operand) Rank {
	return &SumRank{ranks: []Rank{a, operandToRank(operand)}}
}

func (a *AbsRank) Div(operand Operand) Rank {
	return &DivRank{left: a, right: operandToRank(operand)}
}

func (a *AbsRank) Negate() Rank {
	return &MulRank{ranks: []Rank{Val(-1), a}}
}

func (a *AbsRank) Abs() Rank {
	return a // abs(abs(x)) = abs(x)
}

func (a *AbsRank) Exp() Rank {
	return &ExpRank{rank: a}
}

func (a *AbsRank) Log() Rank {
	return &LogRank{rank: a}
}

func (a *AbsRank) Max(operand Operand) Rank {
	return &MaxRank{ranks: []Rank{a, operandToRank(operand)}}
}

func (a *AbsRank) Min(operand Operand) Rank {
	return &MinRank{ranks: []Rank{a, operandToRank(operand)}}
}

func (a *AbsRank) MarshalJSON() ([]byte, error) {
	data, err := a.rank.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]json.RawMessage{"$abs": data})
}

func (a *AbsRank) UnmarshalJSON(_ []byte) error {
	return errors.New("AbsRank: unmarshaling is not supported")
}

// ExpRank represents the exponential (e^x) of a rank expression.
// Serializes to JSON as {"$exp": <rank>}.
type ExpRank struct {
	rank Rank
}

func (e *ExpRank) IsOperand() {}

func (e *ExpRank) Multiply(operand Operand) Rank {
	return &MulRank{ranks: []Rank{e, operandToRank(operand)}}
}

func (e *ExpRank) Sub(operand Operand) Rank {
	return &SubRank{left: e, right: operandToRank(operand)}
}

func (e *ExpRank) Add(operand Operand) Rank {
	return &SumRank{ranks: []Rank{e, operandToRank(operand)}}
}

func (e *ExpRank) Div(operand Operand) Rank {
	return &DivRank{left: e, right: operandToRank(operand)}
}

func (e *ExpRank) Negate() Rank {
	return &MulRank{ranks: []Rank{Val(-1), e}}
}

func (e *ExpRank) Abs() Rank {
	return &AbsRank{rank: e}
}

func (e *ExpRank) Exp() Rank {
	return &ExpRank{rank: e}
}

func (e *ExpRank) Log() Rank {
	return &LogRank{rank: e}
}

func (e *ExpRank) Max(operand Operand) Rank {
	return &MaxRank{ranks: []Rank{e, operandToRank(operand)}}
}

func (e *ExpRank) Min(operand Operand) Rank {
	return &MinRank{ranks: []Rank{e, operandToRank(operand)}}
}

func (e *ExpRank) MarshalJSON() ([]byte, error) {
	data, err := e.rank.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]json.RawMessage{"$exp": data})
}

func (e *ExpRank) UnmarshalJSON(_ []byte) error {
	return errors.New("ExpRank: unmarshaling is not supported")
}

// LogRank represents the natural logarithm of a rank expression.
// Serializes to JSON as {"$log": <rank>}.
type LogRank struct {
	rank Rank
}

func (l *LogRank) IsOperand() {}

func (l *LogRank) Multiply(operand Operand) Rank {
	return &MulRank{ranks: []Rank{l, operandToRank(operand)}}
}

func (l *LogRank) Sub(operand Operand) Rank {
	return &SubRank{left: l, right: operandToRank(operand)}
}

func (l *LogRank) Add(operand Operand) Rank {
	return &SumRank{ranks: []Rank{l, operandToRank(operand)}}
}

func (l *LogRank) Div(operand Operand) Rank {
	return &DivRank{left: l, right: operandToRank(operand)}
}

func (l *LogRank) Negate() Rank {
	return &MulRank{ranks: []Rank{Val(-1), l}}
}

func (l *LogRank) Abs() Rank {
	return &AbsRank{rank: l}
}

func (l *LogRank) Exp() Rank {
	return &ExpRank{rank: l}
}

func (l *LogRank) Log() Rank {
	return l
}

func (l *LogRank) Max(operand Operand) Rank {
	return &MaxRank{ranks: []Rank{l, operandToRank(operand)}}
}

func (l *LogRank) Min(operand Operand) Rank {
	return &MinRank{ranks: []Rank{l, operandToRank(operand)}}
}

func (l *LogRank) MarshalJSON() ([]byte, error) {
	data, err := l.rank.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]json.RawMessage{"$log": data})
}

func (l *LogRank) UnmarshalJSON(_ []byte) error {
	return errors.New("LogRank: unmarshaling is not supported")
}

// MaxRank represents the maximum of multiple rank expressions.
// Serializes to JSON as {"$max": [...]}.
type MaxRank struct {
	ranks []Rank
}

func (m *MaxRank) IsOperand() {}

func (m *MaxRank) Multiply(operand Operand) Rank {
	return &MulRank{ranks: []Rank{m, operandToRank(operand)}}
}

func (m *MaxRank) Sub(operand Operand) Rank {
	return &SubRank{left: m, right: operandToRank(operand)}
}

func (m *MaxRank) Add(operand Operand) Rank {
	return &SumRank{ranks: []Rank{m, operandToRank(operand)}}
}

func (m *MaxRank) Div(operand Operand) Rank {
	return &DivRank{left: m, right: operandToRank(operand)}
}

func (m *MaxRank) Negate() Rank {
	return &MulRank{ranks: []Rank{Val(-1), m}}
}

func (m *MaxRank) Abs() Rank {
	return &AbsRank{rank: m}
}

func (m *MaxRank) Exp() Rank {
	return &ExpRank{rank: m}
}

func (m *MaxRank) Log() Rank {
	return &LogRank{rank: m}
}

func (m *MaxRank) Max(operand Operand) Rank {
	r := operandToRank(operand)
	newRanks := make([]Rank, len(m.ranks))
	copy(newRanks, m.ranks)
	if max, ok := r.(*MaxRank); ok {
		return &MaxRank{ranks: append(newRanks, max.ranks...)}
	}
	return &MaxRank{ranks: append(newRanks, r)}
}

func (m *MaxRank) Min(operand Operand) Rank {
	return &MinRank{ranks: []Rank{m, operandToRank(operand)}}
}

func (m *MaxRank) MarshalJSON() ([]byte, error) {
	if len(m.ranks) > MaxExpressionTerms {
		return nil, errors.Errorf("max expression exceeds maximum of %d terms", MaxExpressionTerms)
	}
	rankMaps := make([]json.RawMessage, len(m.ranks))
	for i, r := range m.ranks {
		data, err := r.MarshalJSON()
		if err != nil {
			return nil, err
		}
		rankMaps[i] = data
	}
	return json.Marshal(map[string][]json.RawMessage{"$max": rankMaps})
}

func (m *MaxRank) UnmarshalJSON(_ []byte) error {
	return errors.New("MaxRank: unmarshaling is not supported")
}

// MinRank represents the minimum of multiple rank expressions.
// Serializes to JSON as {"$min": [...]}.
type MinRank struct {
	ranks []Rank
}

func (m *MinRank) IsOperand() {}

func (m *MinRank) Multiply(operand Operand) Rank {
	return &MulRank{ranks: []Rank{m, operandToRank(operand)}}
}

func (m *MinRank) Sub(operand Operand) Rank {
	return &SubRank{left: m, right: operandToRank(operand)}
}

func (m *MinRank) Add(operand Operand) Rank {
	return &SumRank{ranks: []Rank{m, operandToRank(operand)}}
}

func (m *MinRank) Div(operand Operand) Rank {
	return &DivRank{left: m, right: operandToRank(operand)}
}

func (m *MinRank) Negate() Rank {
	return &MulRank{ranks: []Rank{Val(-1), m}}
}

func (m *MinRank) Abs() Rank {
	return &AbsRank{rank: m}
}

func (m *MinRank) Exp() Rank {
	return &ExpRank{rank: m}
}

func (m *MinRank) Log() Rank {
	return &LogRank{rank: m}
}

func (m *MinRank) Max(operand Operand) Rank {
	return &MaxRank{ranks: []Rank{m, operandToRank(operand)}}
}

func (m *MinRank) Min(operand Operand) Rank {
	r := operandToRank(operand)
	newRanks := make([]Rank, len(m.ranks))
	copy(newRanks, m.ranks)
	if min, ok := r.(*MinRank); ok {
		return &MinRank{ranks: append(newRanks, min.ranks...)}
	}
	return &MinRank{ranks: append(newRanks, r)}
}

func (m *MinRank) MarshalJSON() ([]byte, error) {
	if len(m.ranks) > MaxExpressionTerms {
		return nil, errors.Errorf("min expression exceeds maximum of %d terms", MaxExpressionTerms)
	}
	rankMaps := make([]json.RawMessage, len(m.ranks))
	for i, r := range m.ranks {
		data, err := r.MarshalJSON()
		if err != nil {
			return nil, err
		}
		rankMaps[i] = data
	}
	return json.Marshal(map[string][]json.RawMessage{"$min": rankMaps})
}

func (m *MinRank) UnmarshalJSON(_ []byte) error {
	return errors.New("MinRank: unmarshaling is not supported")
}

// KnnOption configures optional parameters for [KnnRank].
type KnnOption func(req *KnnRank) error

// KnnQueryOption sets the query for [KnnRank].
type KnnQueryOption func(req *KnnRank) error

// KnnQueryVector creates a KNN query from a dense vector embedding.
// The vector will be used directly for similarity search without auto-embedding.
//
// Example:
//
//	vector := embeddings.NewEmbeddingFromFloat32([]float32{0.1, 0.2, 0.3, ...})
//	rank := NewKnnRank(KnnQueryVector(vector))
func KnnQueryVector(queryVector embeddings.KnnVector) KnnQueryOption {
	return func(req *KnnRank) error {
		req.Query = queryVector.ValuesAsFloat32()
		return nil
	}
}

// KnnQuerySparseVector creates a KNN query from a sparse vector.
// Use with [WithKnnKey] to target a sparse embedding field.
//
// Example:
//
//	sparse, err := embeddings.NewSparseVector([]int{1, 5, 10}, []float32{0.5, 0.3, 0.8})
//	if err != nil { return err }
//	rank, err := NewKnnRank(KnnQuerySparseVector(sparse), WithKnnKey(K("sparse_embedding")))
func KnnQuerySparseVector(sparseVector *embeddings.SparseVector) KnnQueryOption {
	return func(req *KnnRank) error {
		req.Query = sparseVector
		return nil
	}
}

// KnnQueryText creates a KNN query from text that will be auto-embedded.
// The collection's embedding function will convert the text to a vector at search time.
//
// Example:
//
//	rank := NewKnnRank(KnnQueryText("machine learning research"))
func KnnQueryText(text string) KnnQueryOption {
	return func(req *KnnRank) error {
		req.Query = text
		return nil
	}
}

// WithKnnLimit sets the maximum number of nearest neighbors to retrieve.
// Only the top K documents are scored; others receive the default score or are excluded.
// Default is 16.
func WithKnnLimit(limit int) KnnOption {
	return func(req *KnnRank) error {
		if limit < 1 {
			return errors.New("knn limit must be >= 1")
		}
		req.Limit = limit
		return nil
	}
}

// WithKnnKey specifies which embedding field to search.
// Default is "#embedding" (the main embedding). Use for multi-vector or sparse searches.
//
// Example:
//
//	rank := NewKnnRank(query, WithKnnKey(K("sparse_embedding")))
func WithKnnKey(key Key) KnnOption {
	return func(req *KnnRank) error {
		req.Key = key
		return nil
	}
}

// WithKnnDefault sets the score for documents not in the top-K nearest neighbors.
// When set, documents outside the KNN results receive this score instead of being excluded.
// Use this for inclusive multi-query searches where a document should match ANY query.
//
// Example:
//
//	rank := NewKnnRank(query, WithKnnDefault(10.0))  // Non-matches get score 10.0
func WithKnnDefault(defaultScore float64) KnnOption {
	return func(req *KnnRank) error {
		req.DefaultScore = &defaultScore
		return nil
	}
}

// WithKnnReturnRank makes the KNN return rank position (1, 2, 3...) instead of distance.
// Required when using [KnnRank] with Reciprocal Rank Fusion ([RrfRank]).
func WithKnnReturnRank() KnnOption {
	return func(req *KnnRank) error {
		req.ReturnRank = true
		return nil
	}
}

// KnnRank performs K-Nearest Neighbors search and scoring.
// Serializes to JSON as {"$knn": {...}}.
//
// Create using [NewKnnRank] with a query option and optional configuration:
//
//	// Text query (auto-embedded)
//	rank := NewKnnRank(KnnQueryText("search query"))
//
//	// With options
//	rank := NewKnnRank(
//	    KnnQueryText("query"),
//	    WithKnnLimit(100),
//	    WithKnnDefault(10.0),
//	)
//
//	// Weighted combination
//	combined := rank1.Multiply(FloatOperand(0.7)).Add(rank2.Multiply(FloatOperand(0.3)))
type KnnRank struct {
	Query        interface{}
	Key          Key
	Limit        int
	DefaultScore *float64
	ReturnRank   bool
}

// NewKnnRank creates a K-Nearest Neighbors ranking expression.
//
// Parameters:
//   - query: The search query (use [KnnQueryText], [KnnQueryVector], or [KnnQuerySparseVector])
//   - knnOptions: Optional configuration ([WithKnnLimit], [WithKnnKey], [WithKnnDefault], [WithKnnReturnRank])
//
// Example:
//
//	rank, err := NewKnnRank(
//	    KnnQueryText("machine learning"),
//	    WithKnnLimit(50),
//	    WithKnnDefault(10.0),
//	)
func NewKnnRank(query KnnQueryOption, knnOptions ...KnnOption) (*KnnRank, error) {
	knn := &KnnRank{
		Key:   KEmbedding,
		Limit: 16,
	}
	if query != nil {
		if err := query(knn); err != nil {
			return nil, err
		}
	}
	for _, opt := range knnOptions {
		if err := opt(knn); err != nil {
			return nil, err
		}
	}
	return knn, nil
}

func (k *KnnRank) IsOperand() {}

func (k *KnnRank) Multiply(operand Operand) Rank {
	return &MulRank{ranks: []Rank{k, operandToRank(operand)}}
}

func (k *KnnRank) Sub(operand Operand) Rank {
	return &SubRank{left: k, right: operandToRank(operand)}
}

func (k *KnnRank) Add(operand Operand) Rank {
	return &SumRank{ranks: []Rank{k, operandToRank(operand)}}
}

func (k *KnnRank) Div(operand Operand) Rank {
	return &DivRank{left: k, right: operandToRank(operand)}
}

func (k *KnnRank) Negate() Rank {
	return &MulRank{ranks: []Rank{Val(-1), k}}
}

func (k *KnnRank) Abs() Rank {
	return &AbsRank{rank: k}
}

func (k *KnnRank) Exp() Rank {
	return &ExpRank{rank: k}
}

func (k *KnnRank) Log() Rank {
	return &LogRank{rank: k}
}

func (k *KnnRank) Max(operand Operand) Rank {
	return &MaxRank{ranks: []Rank{k, operandToRank(operand)}}
}

func (k *KnnRank) Min(operand Operand) Rank {
	return &MinRank{ranks: []Rank{k, operandToRank(operand)}}
}

func (k *KnnRank) WithWeight(weight float64) RankWithWeight {
	return RankWithWeight{Rank: k, Weight: weight}
}

func (k *KnnRank) MarshalJSON() ([]byte, error) {
	// Validate query type
	switch k.Query.(type) {
	case string, []float32, *embeddings.SparseVector, nil:
		// Valid types
	default:
		return nil, errors.Errorf("invalid KnnRank query type: %T (expected string, []float32, or *SparseVector)", k.Query)
	}

	inner := map[string]interface{}{
		"query": k.Query,
		"key":   string(k.Key),
		"limit": k.Limit,
	}
	if k.DefaultScore != nil {
		inner["default"] = *k.DefaultScore
	}
	if k.ReturnRank {
		inner["return_rank"] = true
	}
	return json.Marshal(map[string]interface{}{"$knn": inner})
}

func (k *KnnRank) UnmarshalJSON(b []byte) error {
	return errors.New("json: cannot unmarshal KnnRank JSON object")
}

// RffOption configures [RrfRank] parameters.
type RffOption func(req *RrfRank) error

// WithRffRanks adds weighted ranking expressions to RRF.
// Each rank should use [WithKnnReturnRank] to return rank positions instead of distances.
//
// Example:
//
//	rrf, _ := NewRrfRank(
//	    WithRffRanks(
//	        NewKnnRank(KnnQueryText("query1"), WithKnnReturnRank()).WithWeight(0.5),
//	        NewKnnRank(KnnQueryText("query2"), WithKnnReturnRank()).WithWeight(0.5),
//	    ),
//	)
func WithRffRanks(ranks ...RankWithWeight) RffOption {
	return func(req *RrfRank) error {
		req.Ranks = append(req.Ranks, ranks...)
		return nil
	}
}

// WithRffK sets the smoothing constant for RRF. Default is 60.
// Higher values reduce the impact of rank differences.
func WithRffK(k int) RffOption {
	return func(req *RrfRank) error {
		if k < 1 {
			return errors.New("rrf k must be >= 1")
		}
		req.K = k
		return nil
	}
}

// WithRffNormalize enables weight normalization so weights sum to 1.0.
func WithRffNormalize() RffOption {
	return func(req *RrfRank) error {
		req.Normalize = true
		return nil
	}
}

// RrfRank implements Reciprocal Rank Fusion for combining multiple ranking strategies.
//
// RRF uses the formula: -sum(weight_i / (k + rank_i))
//
// This is useful for combining semantic search with keyword search, or multiple
// embedding types. Each input rank should use [WithKnnReturnRank] to return
// positions rather than distances.
//
// Example:
//
//	rrf, err := NewRrfRank(
//	    WithRffRanks(
//	        NewKnnRank(KnnQueryText("AI"), WithKnnReturnRank()).WithWeight(1.0),
//	        NewKnnRank(KnnQueryText("ML"), WithKnnReturnRank()).WithWeight(1.0),
//	    ),
//	    WithRffK(60),
//	)
type RrfRank struct {
	Ranks     []RankWithWeight
	K         int
	Normalize bool
}

// NewRrfRank creates a Reciprocal Rank Fusion ranking expression.
//
// Example:
//
//	rrf, err := NewRrfRank(
//	    WithRffRanks(
//	        NewKnnRank(KnnQueryText("query"), WithKnnReturnRank()).WithWeight(1.0),
//	    ),
//	    WithRffK(60),
//	    WithRffNormalize(),
//	)
//
// MaxRrfRanks is the maximum number of ranks allowed in RRF to prevent excessive memory allocation.
const MaxRrfRanks = 100

// MaxExpressionTerms is the maximum number of terms allowed in variadic rank expressions (Sum, Mul, Max, Min).
const MaxExpressionTerms = 1000

// MaxExpressionDepth is the maximum nesting depth for rank expressions to prevent stack overflow.
const MaxExpressionDepth = 100

func NewRrfRank(opts ...RffOption) (*RrfRank, error) {
	rrf := &RrfRank{
		K: 60,
	}
	for _, opt := range opts {
		if err := opt(rrf); err != nil {
			return nil, errors.Wrap(err, "invalid rank options")
		}
	}
	err := rrf.Validate()
	if err != nil {
		return nil, errors.Wrap(err, "cannot construct RrfRank")
	}
	return rrf, nil
}

func (r *RrfRank) Validate() error {
	if r.K < 1 {
		return errors.New("rrf k must be >= 1")
	}
	if len(r.Ranks) == 0 {
		return errors.New("rrf requires at least one rank")
	}
	if len(r.Ranks) > MaxRrfRanks {
		return errors.Errorf("rrf cannot have more than %d ranks", MaxRrfRanks)
	}
	for i, rw := range r.Ranks {
		if rw.Weight < 0 {
			return errors.Errorf("rank %d has negative weight %v: weights must be non-negative", i, rw.Weight)
		}
		if math.IsNaN(rw.Weight) || math.IsInf(rw.Weight, 0) {
			return errors.Errorf("rank %d has invalid weight: NaN and Inf are not allowed", i)
		}
	}
	return nil
}

func (r *RrfRank) IsOperand() {}

// no-op
func (r *RrfRank) Multiply(operand Operand) Rank {
	return r
}

func (r *RrfRank) Sub(operand Operand) Rank {
	return r
}

func (r *RrfRank) Add(operand Operand) Rank {
	return r
}

func (r *RrfRank) Div(operand Operand) Rank {
	return r
}

func (r *RrfRank) Negate() Rank {
	return r
}

func (r *RrfRank) Abs() Rank {
	return r
}

func (r *RrfRank) Exp() Rank {
	return r
}

func (r *RrfRank) Log() Rank {
	return r
}

func (r *RrfRank) Max(operand Operand) Rank {
	return r
}

func (r *RrfRank) Min(operand Operand) Rank {
	return r
}

func (r *RrfRank) MarshalJSON() ([]byte, error) {
	err := r.Validate()
	if err != nil {
		return nil, errors.Wrap(err, "cannot marshal RrfRank")
	}
	// Compute weights
	weights := make([]float64, len(r.Ranks))
	for i, rw := range r.Ranks {
		if rw.Weight == 0 {
			weights[i] = 1.0
		} else {
			weights[i] = rw.Weight
		}
	}

	// Normalize if requested
	if r.Normalize {
		sum := 0.0
		for _, w := range weights {
			sum += w
		}
		if math.IsInf(sum, 0) {
			return nil, errors.New("sum of weights overflowed: use smaller weight values")
		}
		if sum < 1e-6 {
			return nil, errors.New("sum of weights must be positive when normalize=true")
		}
		for i := range weights {
			weights[i] /= sum
		}
	}

	// Build terms: weight / (k + rank)
	terms := make([]Rank, len(r.Ranks))
	for i, rw := range r.Ranks {
		// term = weight / (k + rank)
		kVal := Val(float64(r.K))
		denominator := kVal.Add(rw.Rank)
		terms[i] = Val(weights[i]).Div(denominator)
	}

	// Sum all terms
	rrfSum := terms[0]
	for _, term := range terms[1:] {
		rrfSum = rrfSum.Add(term)
	}

	// Negate (RRF gives higher scores for better, Chroma needs lower for better)
	result := rrfSum.Negate()
	return result.MarshalJSON()
}

func (r *RrfRank) UnmarshalJSON(_ []byte) error {
	return errors.New("RrfRank: unmarshaling is not supported")
}

// operandToRank converts an Operand to a Rank.
// Supported operand types: Rank, IntOperand, FloatOperand.
// For nil or unknown types, returns Val(0) to maintain fluid API chaining.
// Note: Only the public operand types (IntOperand, FloatOperand) and Rank implementations
// are expected; unknown types indicate a programming error.
func operandToRank(operand Operand) Rank {
	if operand == nil {
		return Val(0)
	}
	switch v := operand.(type) {
	case Rank:
		return v
	case IntOperand:
		return Val(float64(v))
	case FloatOperand:
		return Val(float64(v))
	default:
		// Unknown operand type - return zero to maintain chaining.
		// This should not happen with proper API usage.
		return &UnknownRank{}
	}
}

// WithKnnRank adds a KNN ranking expression to a search request.
//
// Example:
//
//	search := NewSearchRequest(
//	    WithKnnRank(KnnQueryText("machine learning"), WithKnnLimit(50)),
//	    NewPage(Limit(10)),
//	)
func WithKnnRank(query KnnQueryOption, knnOptions ...KnnOption) SearchOption {
	return SearchRequestOptionFunc(func(req *SearchRequest) error {
		knn, err := NewKnnRank(query, knnOptions...)
		if err != nil {
			return err
		}
		req.Rank = knn
		return nil
	})
}

// WithRffRank adds an RRF ranking expression to a search request.
//
// Example:
//
//	search := NewSearchRequest(
//	    WithRffRank(
//	        WithRffRanks(
//	            NewKnnRank(KnnQueryText("q1"), WithKnnReturnRank()).WithWeight(0.5),
//	            NewKnnRank(KnnQueryText("q2"), WithKnnReturnRank()).WithWeight(0.5),
//	        ),
//	    ),
//	)
func WithRffRank(opts ...RffOption) SearchOption {
	return SearchRequestOptionFunc(func(req *SearchRequest) error {
		rrf, err := NewRrfRank(opts...)
		if err != nil {
			return err
		}
		req.Rank = rrf
		return nil
	})
}
