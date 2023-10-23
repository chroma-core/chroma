package model

type CollectionMetadataValueType interface {
	IsCollectionMetadataValueType()
}

type CollectionMetadataValueStringType struct {
	Value string
}

func (s *CollectionMetadataValueStringType) IsCollectionMetadataValueType() {}

type CollectionMetadataValueInt64Type struct {
	Value int64
}

func (s *CollectionMetadataValueInt64Type) IsCollectionMetadataValueType() {}

type CollectionMetadataValueFloat64Type struct {
	Value float64
}

func (s *CollectionMetadataValueFloat64Type) IsCollectionMetadataValueType() {}

type CollectionMetadata[T CollectionMetadataValueType] struct {
	Metadata map[string]T
}

func NewCollectionMetadata[T CollectionMetadataValueType]() *CollectionMetadata[T] {
	return &CollectionMetadata[T]{
		Metadata: make(map[string]T),
	}
}

func (m *CollectionMetadata[T]) Add(key string, value T) {
	m.Metadata[key] = value
}

func (m *CollectionMetadata[T]) Get(key string) T {
	return m.Metadata[key]
}

func (m *CollectionMetadata[T]) Remove(key string) {
	delete(m.Metadata, key)
}
