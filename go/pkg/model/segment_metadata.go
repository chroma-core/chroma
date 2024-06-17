package model

type SegmentMetadataValueType interface {
	IsSegmentMetadataValueType()
}

type SegmentMetadataValueStringType struct {
	Value string
}

func (s *SegmentMetadataValueStringType) IsSegmentMetadataValueType() {}

type SegmentMetadataValueInt64Type struct {
	Value int64
}

func (s *SegmentMetadataValueInt64Type) IsSegmentMetadataValueType() {}

type SegmentMetadataValueBoolType struct {
	Value bool
}

func (s *SegmentMetadataValueBoolType) IsSegmentMetadataValueType() {}

type SegmentMetadataValueFloat64Type struct {
	Value float64
}

func (s *SegmentMetadataValueFloat64Type) IsSegmentMetadataValueType() {}

type SegmentMetadata[T SegmentMetadataValueType] struct {
	Metadata map[string]T
}

func NewSegmentMetadata[T SegmentMetadataValueType]() *SegmentMetadata[T] {
	return &SegmentMetadata[T]{
		Metadata: make(map[string]T),
	}
}

func (m *SegmentMetadata[T]) Set(key string, value T) {
	m.Metadata[key] = value
}

func (m *SegmentMetadata[T]) Get(key string) T {
	return m.Metadata[key]
}

func (m *SegmentMetadata[T]) Remove(key string) {
	delete(m.Metadata, key)
}

func (m *SegmentMetadata[T]) Keys() []string {
	keys := make([]string, 0, len(m.Metadata))
	for k := range m.Metadata {
		keys = append(keys, k)
	}
	return keys
}

func (m *SegmentMetadata[T]) Empty() bool {
	return len(m.Metadata) == 0
}
