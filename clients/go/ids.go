package chroma

import (
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/google/uuid"
	"github.com/oklog/ulid"
)

type GenerateOptions struct {
	Document string
}

type IDGeneratorOption func(opts *GenerateOptions)

func WithDocument(document string) IDGeneratorOption {
	return func(opts *GenerateOptions) {
		opts.Document = document
	}
}

type IDGenerator interface {
	Generate(opts ...IDGeneratorOption) string
}

type UUIDGenerator struct{}

func (u *UUIDGenerator) Generate(opts ...IDGeneratorOption) string {
	// Use NewRandom instead of New to avoid panic from Must wrapper
	uuidV4, err := uuid.NewRandom()
	if err != nil {
		// fallback to empty string and let Chroma reject it
		return ""
	}
	return uuidV4.String()
}

func NewUUIDGenerator() *UUIDGenerator {
	return &UUIDGenerator{}
}

type SHA256Generator struct{}

func (s *SHA256Generator) Generate(opts ...IDGeneratorOption) string {
	op := GenerateOptions{}
	for _, opt := range opts {
		opt(&op)
	}
	if op.Document == "" {
		// Use NewRandom instead of New to avoid panic from Must wrapper
		uuidV4, err := uuid.NewRandom()
		if err != nil {
			// fallback to empty string and let Chroma reject it
			return ""
		}
		op.Document = uuidV4.String()
	}
	hasher := sha256.New()
	hasher.Write([]byte(op.Document))
	sha256Hash := hex.EncodeToString(hasher.Sum(nil))
	return sha256Hash
}

func NewSHA256Generator() *SHA256Generator {
	return &SHA256Generator{}
}

type ULIDGenerator struct{}

func (u *ULIDGenerator) Generate(opts ...IDGeneratorOption) string {
	// Wrap in a function to handle panics properly
	generateID := func() (id string) {
		defer func() {
			if r := recover(); r != nil {
				// fallback to empty string and let Chroma reject it
				id = ""
			}
		}()

		t := time.Now()
		// Use crypto/rand for secure entropy
		entropy := ulid.Monotonic(crand.Reader, 0)

		docULID, err := ulid.New(ulid.Timestamp(t), entropy)
		if err != nil {
			// fallback to empty string and let Chroma reject it
			return ""
		}
		id = docULID.String()
		return id
	}

	return generateID()
}

func NewULIDGenerator() *ULIDGenerator {
	return &ULIDGenerator{}
}
