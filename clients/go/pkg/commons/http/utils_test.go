package http

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadLimitedBody(t *testing.T) {
	t.Run("under limit", func(t *testing.T) {
		input := []byte("hello world")
		data, err := ReadLimitedBody(bytes.NewReader(input))
		require.NoError(t, err)
		assert.Equal(t, input, data)
	})

	t.Run("empty reader", func(t *testing.T) {
		data, err := ReadLimitedBody(bytes.NewReader(nil))
		require.NoError(t, err)
		assert.Empty(t, data)
	})

	t.Run("exactly at limit", func(t *testing.T) {
		input := strings.Repeat("a", MaxResponseBodySize)
		data, err := ReadLimitedBody(strings.NewReader(input))
		require.NoError(t, err)
		assert.Len(t, data, MaxResponseBodySize)
	})

	t.Run("over limit", func(t *testing.T) {
		input := strings.Repeat("a", MaxResponseBodySize+1)
		_, err := ReadLimitedBody(strings.NewReader(input))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "response body exceeds maximum size")
	})
}
