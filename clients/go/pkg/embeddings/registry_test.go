package embeddings

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockEmbeddingFunction struct {
	name string
}

func (m *mockEmbeddingFunction) EmbedDocuments(_ context.Context, _ []string) ([]Embedding, error) {
	return nil, nil
}

func (m *mockEmbeddingFunction) EmbedQuery(_ context.Context, _ string) (Embedding, error) {
	return nil, nil
}

func (m *mockEmbeddingFunction) Name() string {
	return m.name
}

func (m *mockEmbeddingFunction) GetConfig() EmbeddingFunctionConfig {
	return EmbeddingFunctionConfig{"name": m.name}
}

func (m *mockEmbeddingFunction) DefaultSpace() DistanceMetric {
	return COSINE
}

func (m *mockEmbeddingFunction) SupportedSpaces() []DistanceMetric {
	return []DistanceMetric{COSINE, L2, IP}
}

type mockCloseableEmbeddingFunction struct {
	name   string
	closed bool
}

func (m *mockCloseableEmbeddingFunction) EmbedDocuments(_ context.Context, _ []string) ([]Embedding, error) {
	return nil, nil
}

func (m *mockCloseableEmbeddingFunction) EmbedQuery(_ context.Context, _ string) (Embedding, error) {
	return nil, nil
}

func (m *mockCloseableEmbeddingFunction) Name() string {
	return m.name
}

func (m *mockCloseableEmbeddingFunction) GetConfig() EmbeddingFunctionConfig {
	return EmbeddingFunctionConfig{"name": m.name}
}

func (m *mockCloseableEmbeddingFunction) DefaultSpace() DistanceMetric {
	return COSINE
}

func (m *mockCloseableEmbeddingFunction) SupportedSpaces() []DistanceMetric {
	return []DistanceMetric{COSINE, L2, IP}
}

func (m *mockCloseableEmbeddingFunction) Close() error {
	m.closed = true
	return nil
}

type mockSparseEmbeddingFunction struct {
	name string
}

func (m *mockSparseEmbeddingFunction) EmbedDocumentsSparse(_ context.Context, _ []string) ([]*SparseVector, error) {
	return nil, nil
}

func (m *mockSparseEmbeddingFunction) EmbedQuerySparse(_ context.Context, _ string) (*SparseVector, error) {
	return nil, nil
}

func (m *mockSparseEmbeddingFunction) Name() string {
	return m.name
}

func (m *mockSparseEmbeddingFunction) GetConfig() EmbeddingFunctionConfig {
	return EmbeddingFunctionConfig{"name": m.name}
}

func TestRegisterAndBuildDense(t *testing.T) {
	name := "test_dense_ef"
	err := RegisterDense(name, func(_ EmbeddingFunctionConfig) (EmbeddingFunction, error) {
		return &mockEmbeddingFunction{name: name}, nil
	})
	require.NoError(t, err)

	assert.True(t, HasDense(name))

	ef, err := BuildDense(name, nil)
	require.NoError(t, err)
	assert.Equal(t, name, ef.Name())
}

func TestRegisterAndBuildSparse(t *testing.T) {
	name := "test_sparse_ef"
	err := RegisterSparse(name, func(_ EmbeddingFunctionConfig) (SparseEmbeddingFunction, error) {
		return &mockSparseEmbeddingFunction{name: name}, nil
	})
	require.NoError(t, err)

	assert.True(t, HasSparse(name))

	ef, err := BuildSparse(name, nil)
	require.NoError(t, err)
	assert.Equal(t, name, ef.Name())
}

func TestBuildDenseUnknown(t *testing.T) {
	_, err := BuildDense("nonexistent_dense", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown embedding function")
}

func TestBuildSparseUnknown(t *testing.T) {
	_, err := BuildSparse("nonexistent_sparse", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown sparse embedding function")
}

func TestListDense(t *testing.T) {
	name := "test_list_dense"
	err := RegisterDense(name, func(_ EmbeddingFunctionConfig) (EmbeddingFunction, error) {
		return &mockEmbeddingFunction{name: name}, nil
	})
	require.NoError(t, err)

	names := ListDense()
	assert.Contains(t, names, name)
}

func TestListSparse(t *testing.T) {
	name := "test_list_sparse"
	err := RegisterSparse(name, func(_ EmbeddingFunctionConfig) (SparseEmbeddingFunction, error) {
		return &mockSparseEmbeddingFunction{name: name}, nil
	})
	require.NoError(t, err)

	names := ListSparse()
	assert.Contains(t, names, name)
}

func TestHasDense(t *testing.T) {
	name := "test_has_dense"
	assert.False(t, HasDense(name))

	err := RegisterDense(name, func(_ EmbeddingFunctionConfig) (EmbeddingFunction, error) {
		return &mockEmbeddingFunction{name: name}, nil
	})
	require.NoError(t, err)

	assert.True(t, HasDense(name))
}

func TestHasSparse(t *testing.T) {
	name := "test_has_sparse"
	assert.False(t, HasSparse(name))

	err := RegisterSparse(name, func(_ EmbeddingFunctionConfig) (SparseEmbeddingFunction, error) {
		return &mockSparseEmbeddingFunction{name: name}, nil
	})
	require.NoError(t, err)

	assert.True(t, HasSparse(name))
}

func TestRegisterDenseDuplicate(t *testing.T) {
	name := "test_dense_duplicate"
	err := RegisterDense(name, func(_ EmbeddingFunctionConfig) (EmbeddingFunction, error) {
		return &mockEmbeddingFunction{name: name}, nil
	})
	require.NoError(t, err)

	err = RegisterDense(name, func(_ EmbeddingFunctionConfig) (EmbeddingFunction, error) {
		return &mockEmbeddingFunction{name: name}, nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already registered")
}

func TestRegisterSparseDuplicate(t *testing.T) {
	name := "test_sparse_duplicate"
	err := RegisterSparse(name, func(_ EmbeddingFunctionConfig) (SparseEmbeddingFunction, error) {
		return &mockSparseEmbeddingFunction{name: name}, nil
	})
	require.NoError(t, err)

	err = RegisterSparse(name, func(_ EmbeddingFunctionConfig) (SparseEmbeddingFunction, error) {
		return &mockSparseEmbeddingFunction{name: name}, nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already registered")
}

func TestBuildDenseCloseableWithCloseable(t *testing.T) {
	name := "test_dense_closeable"
	mockEf := &mockCloseableEmbeddingFunction{name: name}
	err := RegisterDense(name, func(_ EmbeddingFunctionConfig) (EmbeddingFunction, error) {
		return mockEf, nil
	})
	require.NoError(t, err)

	ef, closer, err := BuildDenseCloseable(name, nil)
	require.NoError(t, err)
	require.NotNil(t, ef)
	require.NotNil(t, closer)
	assert.Equal(t, name, ef.Name())
	assert.False(t, mockEf.closed)

	err = closer()
	require.NoError(t, err)
	assert.True(t, mockEf.closed)
}

func TestBuildDenseCloseableWithoutCloseable(t *testing.T) {
	name := "test_dense_no_closeable"
	err := RegisterDense(name, func(_ EmbeddingFunctionConfig) (EmbeddingFunction, error) {
		return &mockEmbeddingFunction{name: name}, nil
	})
	require.NoError(t, err)

	ef, closer, err := BuildDenseCloseable(name, nil)
	require.NoError(t, err)
	require.NotNil(t, ef)
	require.NotNil(t, closer)
	assert.Equal(t, name, ef.Name())

	// closer should be a no-op for non-closeable EFs
	err = closer()
	require.NoError(t, err)
}

func TestBuildDenseCloseableUnknown(t *testing.T) {
	_, _, err := BuildDenseCloseable("nonexistent_closeable", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown embedding function")
}

func TestBuildSparseCloseableWithoutCloseable(t *testing.T) {
	name := "test_sparse_closeable"
	err := RegisterSparse(name, func(_ EmbeddingFunctionConfig) (SparseEmbeddingFunction, error) {
		return &mockSparseEmbeddingFunction{name: name}, nil
	})
	require.NoError(t, err)

	ef, closer, err := BuildSparseCloseable(name, nil)
	require.NoError(t, err)
	require.NotNil(t, ef)
	require.NotNil(t, closer)
	assert.Equal(t, name, ef.Name())

	// closer should be a no-op for non-closeable EFs
	err = closer()
	require.NoError(t, err)
}

func TestBuildSparseCloseableUnknown(t *testing.T) {
	_, _, err := BuildSparseCloseable("nonexistent_sparse_closeable", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown sparse embedding function")
}
