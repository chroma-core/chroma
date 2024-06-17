package model

type CollectionMetadataValueType interface {
	IsCollectionMetadataValueType()
	Equals(other CollectionMetadataValueType) bool
}

type CollectionMetadataValueStringType struct {
	Value string
}

func (s *CollectionMetadataValueStringType) IsCollectionMetadataValueType() {}

func (s *CollectionMetadataValueStringType) Equals(other CollectionMetadataValueType) bool {
	if o, ok := other.(*CollectionMetadataValueStringType); ok {
		return s.Value == o.Value
	}
	return false
}

type CollectionMetadataValueInt64Type struct {
	Value int64
}

func (s *CollectionMetadataValueInt64Type) IsCollectionMetadataValueType() {}

func (s *CollectionMetadataValueInt64Type) Equals(other CollectionMetadataValueType) bool {
	if o, ok := other.(*CollectionMetadataValueInt64Type); ok {
		return s.Value == o.Value
	}
	return false
}

type CollectionMetadataValueFloat64Type struct {
	Value float64
}

func (s *CollectionMetadataValueFloat64Type) IsCollectionMetadataValueType() {}

func (s *CollectionMetadataValueFloat64Type) Equals(other CollectionMetadataValueType) bool {
	if o, ok := other.(*CollectionMetadataValueFloat64Type); ok {
		return s.Value == o.Value
	}
	return false
}

type CollectionMetadataValueBoolType struct {
	Value bool
}

func (s *CollectionMetadataValueBoolType) IsCollectionMetadataValueType() {}

func (s *CollectionMetadataValueBoolType) Equals(other CollectionMetadataValueType) bool {
	if o, ok := other.(*CollectionMetadataValueBoolType); ok {
		return s.Value == o.Value
	}
	return false
}

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

func (m *CollectionMetadata[T]) Empty() bool {
	return len(m.Metadata) == 0
}

func (m *CollectionMetadata[T]) Equals(other *CollectionMetadata[T]) bool {
	if m == nil && other == nil {
		return true
	}
	if m == nil && other != nil {
		return false
	}
	if m != nil && other == nil {
		return false
	}
	if len(m.Metadata) != len(other.Metadata) {
		return false
	}
	for key, value := range m.Metadata {
		if otherValue, ok := other.Metadata[key]; !ok || !value.Equals(otherValue) {
			return false
		}
	}
	return true
}
