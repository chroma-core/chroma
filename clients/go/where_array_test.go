//go:build !cloud

package chroma

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetadataContainsString(t *testing.T) {
	clause := MetadataContainsString(K("tags"), "science")
	require.Equal(t, ContainsWhereOperator, clause.Operator())
	require.Equal(t, "tags", clause.Key())
	require.Equal(t, "science", clause.Operand())
	require.NoError(t, clause.Validate())

	b, err := json.Marshal(clause)
	require.NoError(t, err)
	require.JSONEq(t, `{"tags":{"$contains":"science"}}`, string(b))
}

func TestMetadataNotContainsString(t *testing.T) {
	clause := MetadataNotContainsString(K("tags"), "deprecated")
	require.Equal(t, NotContainsWhereOperator, clause.Operator())
	require.Equal(t, "tags", clause.Key())
	require.Equal(t, "deprecated", clause.Operand())
	require.NoError(t, clause.Validate())

	b, err := json.Marshal(clause)
	require.NoError(t, err)
	require.JSONEq(t, `{"tags":{"$not_contains":"deprecated"}}`, string(b))
}

func TestMetadataContainsInt(t *testing.T) {
	clause := MetadataContainsInt(K("scores"), 100)
	require.Equal(t, ContainsWhereOperator, clause.Operator())
	require.NoError(t, clause.Validate())

	b, err := json.Marshal(clause)
	require.NoError(t, err)
	require.JSONEq(t, `{"scores":{"$contains":100}}`, string(b))
}

func TestMetadataNotContainsInt(t *testing.T) {
	clause := MetadataNotContainsInt(K("scores"), 0)
	require.Equal(t, NotContainsWhereOperator, clause.Operator())
	require.NoError(t, clause.Validate())

	b, err := json.Marshal(clause)
	require.NoError(t, err)
	require.JSONEq(t, `{"scores":{"$not_contains":0}}`, string(b))
}

func TestMetadataContainsFloat(t *testing.T) {
	clause := MetadataContainsFloat(K("ratios"), 1.5)
	require.Equal(t, ContainsWhereOperator, clause.Operator())
	require.NoError(t, clause.Validate())

	b, err := json.Marshal(clause)
	require.NoError(t, err)
	require.JSONEq(t, `{"ratios":{"$contains":1.5}}`, string(b))
}

func TestMetadataNotContainsFloat(t *testing.T) {
	clause := MetadataNotContainsFloat(K("ratios"), 0.0)
	require.Equal(t, NotContainsWhereOperator, clause.Operator())
	require.NoError(t, clause.Validate())
}

func TestMetadataContainsBool(t *testing.T) {
	clause := MetadataContainsBool(K("flags"), true)
	require.Equal(t, ContainsWhereOperator, clause.Operator())
	require.NoError(t, clause.Validate())

	b, err := json.Marshal(clause)
	require.NoError(t, err)
	require.JSONEq(t, `{"flags":{"$contains":true}}`, string(b))
}

func TestMetadataNotContainsBool(t *testing.T) {
	clause := MetadataNotContainsBool(K("flags"), false)
	require.Equal(t, NotContainsWhereOperator, clause.Operator())
	require.NoError(t, clause.Validate())

	b, err := json.Marshal(clause)
	require.NoError(t, err)
	require.JSONEq(t, `{"flags":{"$not_contains":false}}`, string(b))
}

func TestMetadataContainsStringEmptyKey(t *testing.T) {
	clause := MetadataContainsString("", "val")
	err := clause.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-empty")
}

func TestMetadataContainsStringEmptyOperand(t *testing.T) {
	clause := MetadataContainsString(K("tags"), "")
	err := clause.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-empty string")
}

func TestMetadataContainsWithAnd(t *testing.T) {
	clause := And(
		MetadataContainsString(K("tags"), "science"),
		EqInt(K("year"), 2024),
	)
	require.NoError(t, clause.Validate())

	b, err := json.Marshal(clause)
	require.NoError(t, err)

	var result map[string]interface{}
	err = json.Unmarshal(b, &result)
	require.NoError(t, err)
	require.Contains(t, result, "$and")
}

func TestMetadataContainsWithOr(t *testing.T) {
	clause := Or(
		MetadataContainsString(K("tags"), "science"),
		MetadataContainsString(K("tags"), "math"),
	)
	require.NoError(t, clause.Validate())

	b, err := json.Marshal(clause)
	require.NoError(t, err)

	var result map[string]interface{}
	err = json.Unmarshal(b, &result)
	require.NoError(t, err)
	require.Contains(t, result, "$or")
}
