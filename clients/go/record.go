package chroma

import (
	"github.com/pkg/errors"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

type Record interface {
	ID() DocumentID
	Document() Document // should work for both text and URI based documents
	Embedding() embeddings.Embedding
	Metadata() DocumentMetadata
	Validate() error
	Unwrap() (DocumentID, Document, embeddings.Embedding, DocumentMetadata)
}

type Records []Record

type SimpleRecord struct {
	id        string
	embedding embeddings.Embedding
	metadata  DocumentMetadata
	document  string
	uri       string
	err       error // indicating whether the record is valid or nto
}
type RecordOption func(record *SimpleRecord) error

func WithRecordID(id string) RecordOption {
	return func(r *SimpleRecord) error {
		r.id = id
		return nil
	}
}

func WithRecordEmbedding(embedding embeddings.Embedding) RecordOption {
	return func(r *SimpleRecord) error {
		r.embedding = embedding
		return nil
	}
}

func WithRecordMetadatas(metadata DocumentMetadata) RecordOption {
	return func(r *SimpleRecord) error {
		r.metadata = metadata
		return nil
	}
}
func (r *SimpleRecord) constructValidate() error {
	if r.id == "" {
		return errors.New("record id is empty")
	}
	return nil
}
func NewSimpleRecord(opts ...RecordOption) (*SimpleRecord, error) {
	r := &SimpleRecord{}
	for _, opt := range opts {
		err := opt(r)
		if err != nil {
			return nil, errors.Wrap(err, "error applying record option")
		}
	}

	err := r.constructValidate()
	if err != nil {
		return nil, errors.Wrap(err, "error validating record")
	}
	return r, nil
}

func (r *SimpleRecord) ID() DocumentID {
	return DocumentID(r.id)
}

func (r *SimpleRecord) Document() Document {
	return NewTextDocument(r.document)
}

func (r *SimpleRecord) URI() string {
	return r.uri
}

func (r *SimpleRecord) Embedding() embeddings.Embedding {
	return r.embedding
}

func (r *SimpleRecord) Metadata() DocumentMetadata {
	return r.metadata
}

func (r *SimpleRecord) Validate() error {
	return r.err
}

func (r *SimpleRecord) Unwrap() (DocumentID, Document, embeddings.Embedding, DocumentMetadata) {
	return r.ID(), r.Document(), r.Embedding(), r.Metadata()
}
