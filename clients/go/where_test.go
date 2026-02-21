//go:build !cloud

package chroma

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWhere(t *testing.T) {
	var tests = []struct {
		name       string
		clause     WhereClause
		expected   string
		shouldFail bool
	}{
		{
			name: "eq string",
			clause: func() WhereClause {
				return EqString(K("name"), "value")
			}(),
			expected: `{"name":{"$eq":"value"}}`,
		},
		{
			name: "eq int",
			clause: func() WhereClause {
				return EqInt(K("name"), 42)
			}(),
			expected: `{"name":{"$eq":42}}`,
		},
		{
			name: "eq float",
			clause: func() WhereClause {
				return EqFloat(K("name"), 42.42)
			}(),
			expected: `{"name":{"$eq":42.42}}`,
		},
		{
			name: "eq bool",
			clause: func() WhereClause {
				return EqBool(K("name"), true)
			}(),
			expected: `{"name":{"$eq":true}}`,
		},

		{
			name: "Ne string",
			clause: func() WhereClause {
				return NotEqString(K("name"), "value")
			}(),
			expected: `{"name":{"$ne":"value"}}`,
		},
		{
			name: "Ne int",
			clause: func() WhereClause {
				return NotEqInt(K("name"), 42)
			}(),
			expected: `{"name":{"$ne":42}}`,
		},
		{
			name: "Ne float",
			clause: func() WhereClause {
				return NotEqFloat(K("name"), 42.42)
			}(),
			expected: `{"name":{"$ne":42.42}}`,
		},
		{
			name: "Ne bool",
			clause: func() WhereClause {
				return NotEqBool(K("name"), false)
			}(),
			expected: `{"name":{"$ne":false}}`,
		},
		{
			name: "Gt int",
			clause: func() WhereClause {
				return GtInt(K("name"), 42)
			}(),
			expected: `{"name":{"$gt":42}}`,
		},
		{
			name: "Gte int",
			clause: func() WhereClause {
				return GteInt(K("name"), 42)
			}(),
			expected: `{"name":{"$gte":42}}`,
		},
		{
			name: "Gt float",
			clause: func() WhereClause {
				return GtFloat(K("name"), 42.42)
			}(),
			expected: `{"name":{"$gt":42.42}}`,
		},
		{
			name: "Gte float",
			clause: func() WhereClause {
				return GteFloat(K("name"), 42.42)
			}(),
			expected: `{"name":{"$gte":42.42}}`,
		},

		//-----
		{
			name: "Lt int",
			clause: func() WhereClause {
				return LtInt(K("name"), 42)
			}(),
			expected: `{"name":{"$lt":42}}`,
		},
		{
			name: "Lte int",
			clause: func() WhereClause {
				return LteInt(K("name"), 42)
			}(),
			expected: `{"name":{"$lte":42}}`,
		},
		{
			name: "Lt float",
			clause: func() WhereClause {
				return LtFloat(K("name"), 42.42)
			}(),
			expected: `{"name":{"$lt":42.42}}`,
		},
		{
			name: "Lte float",
			clause: func() WhereClause {
				return LteFloat(K("name"), 42.42)
			}(),
			expected: `{"name":{"$lte":42.42}}`,
		},
		//-----
		{
			name: "In int",
			clause: func() WhereClause {
				return InInt(K("name"), 42, 43)
			}(),
			expected: `{"name":{"$in":[42,43]}}`,
		},
		{
			name: "In float",
			clause: func() WhereClause {
				return InFloat(K("name"), 42.42, 43.43)
			}(),
			expected: `{"name":{"$in":[42.42, 43.43]}}`,
		},
		{
			name: "In string",
			clause: func() WhereClause {
				return InString(K("name"), "ok", "ko")
			}(),
			expected: `{"name":{"$in":["ok","ko"]}}`,
		},
		{
			name: "In bool",
			clause: func() WhereClause {
				return InBool(K("name"), true, false)
			}(),
			expected: `{"name":{"$in":[true,false]}}`,
		},
		//----
		{
			name: "Nin int",
			clause: func() WhereClause {
				return NinInt(K("name"), 42, 43)
			}(),
			expected: `{"name":{"$nin":[42,43]}}`,
		},
		{
			name: "Nin float",
			clause: func() WhereClause {
				return NinFloat(K("name"), 42.42, 43.43)
			}(),
			expected: `{"name":{"$nin":[42.42, 43.43]}}`,
		},
		{
			name: "Nin string",
			clause: func() WhereClause {
				return NinString(K("name"), "ok", "ko")
			}(),
			expected: `{"name":{"$nin":["ok","ko"]}}`,
		},
		{
			name: "Nin bool",
			clause: func() WhereClause {
				return NinBool(K("name"), true, false)
			}(),
			expected: `{"name":{"$nin":[true,false]}}`,
		},
		//--- ID filters
		{
			name: "IDIn",
			clause: func() WhereClause {
				return IDIn("doc1", "doc2", "doc3")
			}(),
			expected: `{"#id":{"$in":["doc1","doc2","doc3"]}}`,
		},
		{
			name: "IDNotIn",
			clause: func() WhereClause {
				return IDNotIn("seen1", "seen2")
			}(),
			expected: `{"#id":{"$nin":["seen1","seen2"]}}`,
		},
		{
			name: "IDNotIn combined with And",
			clause: func() WhereClause {
				return And(EqString(K("category"), "tech"), IDNotIn("seen1", "seen2"))
			}(),
			expected: `{"$and":[{"category":{"$eq":"tech"}},{"#id":{"$nin":["seen1","seen2"]}}]}`,
		},
		//--- Document content filters
		{
			name: "DocumentContains",
			clause: func() WhereClause {
				return DocumentContains("search text")
			}(),
			expected: `{"#document":{"$contains":"search text"}}`,
		},
		{
			name: "DocumentNotContains",
			clause: func() WhereClause {
				return DocumentNotContains("excluded text")
			}(),
			expected: `{"#document":{"$not_contains":"excluded text"}}`,
		},
		{
			name: "DocumentContains combined with metadata filter",
			clause: func() WhereClause {
				return And(EqString(K("category"), "tech"), DocumentContains("AI"))
			}(),
			expected: `{"$and":[{"category":{"$eq":"tech"}},{"#document":{"$contains":"AI"}}]}`,
		},
		//---
		{
			name: "And",
			clause: func() WhereClause {
				return And(EqString(K("name"), "value"), EqInt(K("age"), 42))
			}(),
			expected: `{"$and":[{"name":{"$eq":"value"}},{"age":{"$eq":42}}]}`,
		},
		{
			name: "Or",
			clause: func() WhereClause {
				return Or(EqString(K("name"), "value"), EqInt(K("age"), 42))
			}(),
			expected: `{"$or":[{"name":{"$eq":"value"}},{"age":{"$eq":42}}]}`,
		},

		{
			name: "And Or",
			clause: func() WhereClause {
				return Or(EqString(K("name"), "value"), EqInt(K("age"), 42), Or(EqString(K("name"), "value"), EqInt(K("age"), 42)))
			}(),
			expected: `{"$or":[{"name":{"$eq":"value"}},{"age":{"$eq":42}},{"$or":[{"name":{"$eq":"value"}},{"age":{"$eq":42}}]}]}`,
		},
		{
			name: "And Or And",
			clause: func() WhereClause {
				return Or(EqString(K("name"), "value"), EqInt(K("age"), 42), And(EqString(K("name"), "value"), EqInt(K("age"), 42)))
			}(),
			expected: `{"$or":[{"name":{"$eq":"value"}},{"age":{"$eq":42}},{"$and":[{"name":{"$eq":"value"}},{"age":{"$eq":42}}]}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			json, err := tt.clause.MarshalJSON()
			if tt.shouldFail {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.JSONEq(t, tt.expected, string(json))
		})
	}
}

func TestWhereClauseEmptyOperandValidation(t *testing.T) {
	tests := []struct {
		name        string
		clause      WhereClause
		expectedErr string
	}{
		{
			name:        "IDIn with no arguments",
			clause:      IDIn(),
			expectedErr: "invalid operand for $in on key \"#id\", expected at least one value",
		},
		{
			name:        "IDNotIn with no arguments",
			clause:      IDNotIn(),
			expectedErr: "invalid operand for $nin on key \"#id\", expected at least one value",
		},
		{
			name:        "InString with no values",
			clause:      InString(K("field")),
			expectedErr: "invalid operand for $in on key \"field\", expected at least one value",
		},
		{
			name:        "NinString with no values",
			clause:      NinString(K("field")),
			expectedErr: "invalid operand for $nin on key \"field\", expected at least one value",
		},
		{
			name:        "Empty IDIn nested in And",
			clause:      And(EqString(K("status"), "active"), IDIn()),
			expectedErr: "invalid operand for $in on key \"#id\", expected at least one value",
		},
		{
			name:        "DocumentContains with empty string",
			clause:      DocumentContains(""),
			expectedErr: "invalid operand for $contains on key \"#document\", expected non-empty string",
		},
		{
			name:        "DocumentNotContains with empty string",
			clause:      DocumentNotContains(""),
			expectedErr: "invalid operand for $not_contains on key \"#document\", expected non-empty string",
		},
		{
			name:        "Empty DocumentContains nested in And",
			clause:      And(EqString(K("category"), "tech"), DocumentContains("")),
			expectedErr: "invalid operand for $contains on key \"#document\", expected non-empty string",
		},
		{
			name:        "Nil clause in And",
			clause:      And(EqString(K("status"), "active"), nil),
			expectedErr: "nil clause in $and expression",
		},
		{
			name:        "Nil clause in Or",
			clause:      Or(nil, EqString(K("status"), "active")),
			expectedErr: "nil clause in $or expression",
		},
		{
			name:        "Nil clause in nested compound",
			clause:      And(EqString(K("a"), "b"), Or(EqString(K("c"), "d"), nil)),
			expectedErr: "nil clause in $or expression",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Construction should succeed (lazy validation)
			require.NotNil(t, tt.clause)

			// Validation should fail
			err := tt.clause.Validate()
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.expectedErr)
		})
	}
}
