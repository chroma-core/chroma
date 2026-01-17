//go:build !cloud

package chroma

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTextDocument(t *testing.T) {

	doc := "Hello, world!\n"

	tdoc := NewTextDocument(doc)

	marshal, err := json.Marshal(tdoc)
	require.NoError(t, err)
	require.Equal(t, `"Hello, world!\n"`, string(marshal))
}
