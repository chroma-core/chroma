package tokenizers

import (
	"fmt"

	puretokenizers "github.com/amikos-tech/pure-tokenizers"
	"github.com/pkg/errors"
)

// Tokenizer wraps pure-tokenizers with backward-compatible API
type Tokenizer struct {
	tokenizer               *puretokenizers.Tokenizer
	defaultAddSpecialTokens bool
}

// Offset represents a character offset range [start, end]
type Offset [2]uint

// Encoding represents the result of tokenizing text
type Encoding struct {
	IDs               []uint32
	TypeIDs           []uint32
	SpecialTokensMask []uint32
	AttentionMask     []uint32
	Tokens            []string
	Offsets           []Offset
}

type TokenizerOption func(to *tokenizerOpts)

type tokenizerOpts struct {
	encodeSpecialTokens bool
}

func WithEncodeSpecialTokens() TokenizerOption {
	return func(to *tokenizerOpts) {
		to.encodeSpecialTokens = true
	}
}

type TruncationDirection int

const (
	TruncationDirectionLeft TruncationDirection = iota
	TruncationDirectionRight
)

type EncodeOption func(eo *encodeOpts)

type encodeOpts struct {
	AddSpecialTokens        bool
	ReturnTypeIDs           bool
	ReturnTokens            bool
	ReturnSpecialTokensMask bool
	ReturnAttentionMask     bool
	ReturnOffsets           bool
}

func WithReturnAllAttributes() EncodeOption {
	return func(eo *encodeOpts) {
		eo.ReturnTypeIDs = true
		eo.ReturnSpecialTokensMask = true
		eo.ReturnAttentionMask = true
		eo.ReturnTokens = true
		eo.ReturnOffsets = true
	}
}

func WithReturnTypeIDs() EncodeOption {
	return func(eo *encodeOpts) {
		eo.ReturnTypeIDs = true
	}
}

func WithReturnSpecialTokensMask() EncodeOption {
	return func(eo *encodeOpts) {
		eo.ReturnSpecialTokensMask = true
	}
}

func WithReturnTokens() EncodeOption {
	return func(eo *encodeOpts) {
		eo.ReturnTokens = true
	}
}

func WithReturnAttentionMask() EncodeOption {
	return func(eo *encodeOpts) {
		eo.ReturnAttentionMask = true
	}
}

func WithReturnOffsets() EncodeOption {
	return func(eo *encodeOpts) {
		eo.ReturnOffsets = true
	}
}

// LoadLibrary is a no-op for backward compatibility
// pure-tokenizers handles library loading automatically
func LoadLibrary(path string) error {
	return nil
}

// FromBytes creates a tokenizer from byte configuration
func FromBytes(data []byte, opts ...TokenizerOption) (*Tokenizer, error) {
	allOpts := &tokenizerOpts{
		encodeSpecialTokens: false,
	}
	for _, opt := range opts {
		opt(allOpts)
	}

	var pureOpts []puretokenizers.TokenizerOption

	tk, err := puretokenizers.FromBytes(data, pureOpts...)
	if err != nil {
		return nil, err
	}

	return &Tokenizer{
		tokenizer:               tk,
		defaultAddSpecialTokens: allOpts.encodeSpecialTokens,
	}, nil
}

// FromBytesWithTruncation creates a tokenizer with truncation settings
func FromBytesWithTruncation(data []byte, maxLen uint32, dir TruncationDirection) (*Tokenizer, error) {
	// Validate maxLen bounds
	if maxLen == 0 {
		return nil, errors.New("maxLen must be greater than 0")
	}
	// Reasonable upper bound for tokenization (1M tokens should be more than enough)
	// This prevents issues with underlying library and nonsensical values
	if maxLen > 1_000_000 {
		return nil, errors.New("maxLen exceeds maximum allowed value of 1,000,000")
	}

	var truncDir puretokenizers.TruncationDirection
	if dir == TruncationDirectionLeft {
		truncDir = puretokenizers.TruncationDirectionLeft
	} else {
		truncDir = puretokenizers.TruncationDirectionRight
	}

	tk, err := puretokenizers.FromBytes(data,
		puretokenizers.WithTruncation(
			uintptr(maxLen),
			truncDir,
			puretokenizers.TruncationStrategyLongestFirst,
		),
	)
	if err != nil {
		return nil, err
	}

	return &Tokenizer{
		tokenizer:               tk,
		defaultAddSpecialTokens: false,
	}, nil
}

// FromFile creates a tokenizer from a file path
func FromFile(path string) (*Tokenizer, error) {
	tk, err := puretokenizers.FromFile(path)
	if err != nil {
		return nil, err
	}
	return &Tokenizer{
		tokenizer:               tk,
		defaultAddSpecialTokens: false,
	}, nil
}

// Close closes the tokenizer and frees resources
func (t *Tokenizer) Close() error {
	if t.tokenizer != nil {
		return t.tokenizer.Close()
	}
	return nil
}

// Encode tokenizes text with simple options
func (t *Tokenizer) Encode(str string, addSpecialTokens bool) ([]uint32, []string, error) {
	if t.tokenizer == nil {
		return nil, nil, errors.New("tokenizer is not initialized")
	}

	// Use OR logic: if either tokenizer default OR parameter is true, add special tokens
	shouldAddSpecial := addSpecialTokens || t.defaultAddSpecialTokens

	var opts []puretokenizers.EncodeOption
	if shouldAddSpecial {
		opts = append(opts, puretokenizers.WithAddSpecialTokens())
	}
	opts = append(opts, puretokenizers.WithReturnTokens())

	result, err := t.tokenizer.Encode(str, opts...)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to encode text")
	}

	return result.IDs, result.Tokens, nil
}

// EncodeWithOptions tokenizes text with full control over encoding options
func (t *Tokenizer) EncodeWithOptions(str string, addSpecialTokens bool, opts ...EncodeOption) (Encoding, error) {
	if t.tokenizer == nil {
		return Encoding{}, errors.New("tokenizer is not initialized")
	}

	// Use OR logic: if either tokenizer default OR parameter is true, add special tokens
	shouldAddSpecial := addSpecialTokens || t.defaultAddSpecialTokens

	encOptions := &encodeOpts{
		AddSpecialTokens: shouldAddSpecial,
	}
	for _, opt := range opts {
		opt(encOptions)
	}

	var pureOpts []puretokenizers.EncodeOption
	if encOptions.AddSpecialTokens {
		pureOpts = append(pureOpts, puretokenizers.WithAddSpecialTokens())
	}
	if encOptions.ReturnTypeIDs {
		pureOpts = append(pureOpts, puretokenizers.WithReturnTypeIDs())
	}
	if encOptions.ReturnTokens {
		pureOpts = append(pureOpts, puretokenizers.WithReturnTokens())
	}
	if encOptions.ReturnSpecialTokensMask {
		pureOpts = append(pureOpts, puretokenizers.WithReturnSpecialTokensMask())
	}
	if encOptions.ReturnAttentionMask {
		pureOpts = append(pureOpts, puretokenizers.WithReturnAttentionMask())
	}
	if encOptions.ReturnOffsets {
		pureOpts = append(pureOpts, puretokenizers.WithReturnOffsets())
	}

	result, err := t.tokenizer.Encode(str, pureOpts...)
	if err != nil {
		return Encoding{}, errors.Wrap(err, "failed to encode text")
	}

	// Convert pure-tokenizers result to our Encoding type
	encoding := Encoding{
		IDs:               result.IDs,
		TypeIDs:           result.TypeIDs,
		SpecialTokensMask: result.SpecialTokensMask,
		AttentionMask:     result.AttentionMask,
		Tokens:            result.Tokens,
	}

	// Convert flattened offsets to array of [2]uint
	if len(result.Offsets) > 0 {
		if len(result.Offsets)%2 != 0 {
			return Encoding{}, fmt.Errorf("malformed offset data: expected even length, got %d", len(result.Offsets))
		}
		encoding.Offsets = make([]Offset, len(result.Offsets)/2)
		for i := 0; i < len(result.Offsets)/2; i++ {
			encoding.Offsets[i] = Offset{
				uint(result.Offsets[i*2]),
				uint(result.Offsets[i*2+1]),
			}
		}
	}

	return encoding, nil
}

// Decode converts token IDs back to text
func (t *Tokenizer) Decode(tokenIDs []uint32, skipSpecialTokens bool) (string, error) {
	if t.tokenizer == nil {
		return "", errors.New("tokenizer is not initialized")
	}
	return t.tokenizer.Decode(tokenIDs, skipSpecialTokens)
}

// VocabSize returns the vocabulary size
func (t *Tokenizer) VocabSize() (uint32, error) {
	if t.tokenizer == nil {
		return 0, errors.New("tokenizer is not initialized")
	}
	return t.tokenizer.VocabSize()
}
