//go:build !cloud

package chroma

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetadataValueGetStringArray(t *testing.T) {
	mv := MetadataValue{StringArray: []string{"a", "b", "c"}}
	arr, ok := mv.GetStringArray()
	require.True(t, ok)
	require.Equal(t, []string{"a", "b", "c"}, arr)
}

func TestMetadataValueGetIntArray(t *testing.T) {
	mv := MetadataValue{IntArray: []int64{1, 2, 3}}
	arr, ok := mv.GetIntArray()
	require.True(t, ok)
	require.Equal(t, []int64{1, 2, 3}, arr)
}

func TestMetadataValueGetFloatArray(t *testing.T) {
	mv := MetadataValue{FloatArray: []float64{1.1, 2.2, 3.3}}
	arr, ok := mv.GetFloatArray()
	require.True(t, ok)
	require.Equal(t, []float64{1.1, 2.2, 3.3}, arr)
}

func TestMetadataValueGetBoolArray(t *testing.T) {
	mv := MetadataValue{BoolArray: []bool{true, false, true}}
	arr, ok := mv.GetBoolArray()
	require.True(t, ok)
	require.Equal(t, []bool{true, false, true}, arr)
}

func TestMetadataValueGetStringArrayMissing(t *testing.T) {
	mv := MetadataValue{}
	_, ok := mv.GetStringArray()
	require.False(t, ok)
}

func TestMetadataValueGetIntArrayMissing(t *testing.T) {
	mv := MetadataValue{}
	_, ok := mv.GetIntArray()
	require.False(t, ok)
}

func TestMetadataValueGetFloatArrayMissing(t *testing.T) {
	mv := MetadataValue{}
	_, ok := mv.GetFloatArray()
	require.False(t, ok)
}

func TestMetadataValueGetBoolArrayMissing(t *testing.T) {
	mv := MetadataValue{}
	_, ok := mv.GetBoolArray()
	require.False(t, ok)
}

func TestMetadataValueGetRawArray(t *testing.T) {
	tests := []struct {
		name     string
		mv       MetadataValue
		expected interface{}
	}{
		{"string array", MetadataValue{StringArray: []string{"a"}}, []string{"a"}},
		{"int array", MetadataValue{IntArray: []int64{1}}, []int64{1}},
		{"float array", MetadataValue{FloatArray: []float64{1.5}}, []float64{1.5}},
		{"bool array", MetadataValue{BoolArray: []bool{true}}, []bool{true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, ok := tt.mv.GetRaw()
			require.True(t, ok)
			require.Equal(t, tt.expected, raw)
		})
	}
}

func TestMetadataValueEqualArrays(t *testing.T) {
	tests := []struct {
		name  string
		a, b  MetadataValue
		equal bool
	}{
		{"same string arrays", MetadataValue{StringArray: []string{"a", "b"}}, MetadataValue{StringArray: []string{"a", "b"}}, true},
		{"different string arrays", MetadataValue{StringArray: []string{"a"}}, MetadataValue{StringArray: []string{"b"}}, false},
		{"same int arrays", MetadataValue{IntArray: []int64{1, 2}}, MetadataValue{IntArray: []int64{1, 2}}, true},
		{"different int arrays", MetadataValue{IntArray: []int64{1}}, MetadataValue{IntArray: []int64{2}}, false},
		{"same float arrays", MetadataValue{FloatArray: []float64{1.1}}, MetadataValue{FloatArray: []float64{1.1}}, true},
		{"different float arrays", MetadataValue{FloatArray: []float64{1.1}}, MetadataValue{FloatArray: []float64{2.2}}, false},
		{"same bool arrays", MetadataValue{BoolArray: []bool{true, false}}, MetadataValue{BoolArray: []bool{true, false}}, true},
		{"different bool arrays", MetadataValue{BoolArray: []bool{true}}, MetadataValue{BoolArray: []bool{false}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.equal, tt.a.Equal(&tt.b))
		})
	}
}

func TestMetadataValueStringRepresentationArrays(t *testing.T) {
	require.Equal(t, "[a b c]", (&MetadataValue{StringArray: []string{"a", "b", "c"}}).String())
	require.Equal(t, "[1 2 3]", (&MetadataValue{IntArray: []int64{1, 2, 3}}).String())
	require.Equal(t, "[1.1 2.2]", (&MetadataValue{FloatArray: []float64{1.1, 2.2}}).String())
	require.Equal(t, "[true false]", (&MetadataValue{BoolArray: []bool{true, false}}).String())
}

func TestMetadataValueJSONMarshalArrays(t *testing.T) {
	tests := []struct {
		name     string
		mv       MetadataValue
		expected string
	}{
		{"string array", MetadataValue{StringArray: []string{"a", "b"}}, `["a","b"]`},
		{"int array", MetadataValue{IntArray: []int64{1, 2}}, `[1,2]`},
		{"float array", MetadataValue{FloatArray: []float64{1.5, 2.5}}, `[1.5,2.5]`},
		{"bool array", MetadataValue{BoolArray: []bool{true, false}}, `[true,false]`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := json.Marshal(&tt.mv)
			require.NoError(t, err)
			require.JSONEq(t, tt.expected, string(b))
		})
	}
}

func TestMetadataValueJSONUnmarshalArrays(t *testing.T) {
	tests := []struct {
		name  string
		input string
		check func(t *testing.T, mv MetadataValue)
	}{
		{"string array", `["hello","world"]`, func(t *testing.T, mv MetadataValue) {
			arr, ok := mv.GetStringArray()
			require.True(t, ok)
			require.Equal(t, []string{"hello", "world"}, arr)
		}},
		{"int array", `[1,2,3]`, func(t *testing.T, mv MetadataValue) {
			arr, ok := mv.GetIntArray()
			require.True(t, ok)
			require.Equal(t, []int64{1, 2, 3}, arr)
		}},
		{"float array", `[1.5,2.5]`, func(t *testing.T, mv MetadataValue) {
			arr, ok := mv.GetFloatArray()
			require.True(t, ok)
			require.Equal(t, []float64{1.5, 2.5}, arr)
		}},
		{"bool array", `[true,false,true]`, func(t *testing.T, mv MetadataValue) {
			arr, ok := mv.GetBoolArray()
			require.True(t, ok)
			require.Equal(t, []bool{true, false, true}, arr)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var mv MetadataValue
			err := json.Unmarshal([]byte(tt.input), &mv)
			require.NoError(t, err)
			tt.check(t, mv)
		})
	}
}

func TestMetadataValueJSONRoundtripArrays(t *testing.T) {
	original := MetadataValue{StringArray: []string{"x", "y", "z"}}
	b, err := json.Marshal(&original)
	require.NoError(t, err)
	var decoded MetadataValue
	err = json.Unmarshal(b, &decoded)
	require.NoError(t, err)
	require.True(t, original.Equal(&decoded))
}

func TestMetadataValueUnmarshalNestedArrayReject(t *testing.T) {
	var mv MetadataValue
	err := json.Unmarshal([]byte(`[["nested"]]`), &mv)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nested arrays are not supported")
}

func TestMetadataValueUnmarshalObjectArrayReject(t *testing.T) {
	var mv MetadataValue
	err := json.Unmarshal([]byte(`[{"key":"val"}]`), &mv)
	require.Error(t, err)
	require.Contains(t, err.Error(), "arrays of objects are not supported")
}

func TestMetadataValueUnmarshalEmptyArrayReject(t *testing.T) {
	var mv MetadataValue
	err := json.Unmarshal([]byte(`[]`), &mv)
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-empty")
}

func TestValidateArrayMetadataEmpty(t *testing.T) {
	tests := []struct {
		name string
		mv   MetadataValue
	}{
		{"empty string array", MetadataValue{StringArray: []string{}}},
		{"empty int array", MetadataValue{IntArray: []int64{}}},
		{"empty float array", MetadataValue{FloatArray: []float64{}}},
		{"empty bool array", MetadataValue{BoolArray: []bool{}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateArrayMetadata(&tt.mv)
			require.Error(t, err)
			require.Contains(t, err.Error(), "non-empty")
		})
	}
}

func TestValidateArrayMetadataValid(t *testing.T) {
	mv := MetadataValue{StringArray: []string{"a"}}
	require.NoError(t, ValidateArrayMetadata(&mv))
}

func TestValidateArrayMetadataScalar(t *testing.T) {
	s := "hello"
	mv := MetadataValue{StringValue: &s}
	require.NoError(t, ValidateArrayMetadata(&mv))
}

func TestNewMetadataFromMapWithArrays(t *testing.T) {
	m := NewMetadataFromMap(map[string]interface{}{
		"tags":   []string{"a", "b"},
		"scores": []int64{1, 2},
		"ratios": []float64{0.5, 1.5},
		"flags":  []bool{true, false},
	})
	arr, ok := m.GetStringArray("tags")
	require.True(t, ok)
	require.Equal(t, []string{"a", "b"}, arr)

	intArr, ok := m.GetIntArray("scores")
	require.True(t, ok)
	require.Equal(t, []int64{1, 2}, intArr)

	floatArr, ok := m.GetFloatArray("ratios")
	require.True(t, ok)
	require.Equal(t, []float64{0.5, 1.5}, floatArr)

	boolArr, ok := m.GetBoolArray("flags")
	require.True(t, ok)
	require.Equal(t, []bool{true, false}, boolArr)
}

func TestNewMetadataFromMapWithInterfaceSlice(t *testing.T) {
	m := NewMetadataFromMap(map[string]interface{}{
		"tags": []interface{}{"a", "b"},
	})
	arr, ok := m.GetStringArray("tags")
	require.True(t, ok)
	require.Equal(t, []string{"a", "b"}, arr)
}

func TestNewDocumentMetadataFromMapWithArrays(t *testing.T) {
	md, err := NewDocumentMetadataFromMap(map[string]interface{}{
		"tags":   []string{"x", "y"},
		"scores": []int64{10, 20},
		"ratios": []float64{0.1, 0.2},
		"flags":  []bool{true},
	})
	require.NoError(t, err)

	arr, ok := md.GetStringArray("tags")
	require.True(t, ok)
	require.Equal(t, []string{"x", "y"}, arr)

	intArr, ok := md.GetIntArray("scores")
	require.True(t, ok)
	require.Equal(t, []int64{10, 20}, intArr)

	floatArr, ok := md.GetFloatArray("ratios")
	require.True(t, ok)
	require.Equal(t, []float64{0.1, 0.2}, floatArr)

	boolArr, ok := md.GetBoolArray("flags")
	require.True(t, ok)
	require.Equal(t, []bool{true}, boolArr)
}

func TestNewDocumentMetadataFromMapWithInterfaceSlice(t *testing.T) {
	md, err := NewDocumentMetadataFromMap(map[string]interface{}{
		"tags": []interface{}{"a", "b"},
	})
	require.NoError(t, err)
	arr, ok := md.GetStringArray("tags")
	require.True(t, ok)
	require.Equal(t, []string{"a", "b"}, arr)
}

func TestConvertInterfaceSliceMixedTypes(t *testing.T) {
	_, err := convertInterfaceSliceToMetadataValue([]interface{}{"a", 1})
	require.Error(t, err)
	require.Contains(t, err.Error(), "mixed types")
}

func TestConvertInterfaceSliceEmpty(t *testing.T) {
	_, err := convertInterfaceSliceToMetadataValue([]interface{}{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-empty")
}

func TestConvertInterfaceSliceNestedArray(t *testing.T) {
	_, err := convertInterfaceSliceToMetadataValue([]interface{}{[]interface{}{"nested"}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "nested arrays")
}

func TestCollectionMetadataImplArraySettersGetters(t *testing.T) {
	cm := NewEmptyMetadata()
	cm.SetStringArray("tags", []string{"a", "b"})
	cm.SetIntArray("scores", []int64{1, 2})
	cm.SetFloatArray("ratios", []float64{0.5})
	cm.SetBoolArray("flags", []bool{true})

	arr, ok := cm.GetStringArray("tags")
	require.True(t, ok)
	require.Equal(t, []string{"a", "b"}, arr)

	intArr, ok := cm.GetIntArray("scores")
	require.True(t, ok)
	require.Equal(t, []int64{1, 2}, intArr)

	floatArr, ok := cm.GetFloatArray("ratios")
	require.True(t, ok)
	require.Equal(t, []float64{0.5}, floatArr)

	boolArr, ok := cm.GetBoolArray("flags")
	require.True(t, ok)
	require.Equal(t, []bool{true}, boolArr)
}

func TestDocumentMetadataImplArraySettersGetters(t *testing.T) {
	dm := NewDocumentMetadata()
	dm.SetStringArray("tags", []string{"x"})
	dm.SetIntArray("nums", []int64{42})
	dm.SetFloatArray("vals", []float64{3.14})
	dm.SetBoolArray("bools", []bool{false, true})

	arr, ok := dm.GetStringArray("tags")
	require.True(t, ok)
	require.Equal(t, []string{"x"}, arr)

	intArr, ok := dm.GetIntArray("nums")
	require.True(t, ok)
	require.Equal(t, []int64{42}, intArr)

	floatArr, ok := dm.GetFloatArray("vals")
	require.True(t, ok)
	require.Equal(t, []float64{3.14}, floatArr)

	boolArr, ok := dm.GetBoolArray("bools")
	require.True(t, ok)
	require.Equal(t, []bool{false, true}, boolArr)
}

func TestDocumentMetadataImplSetRawArrays(t *testing.T) {
	dm := NewDocumentMetadata()
	dm.SetRaw("tags", []string{"a", "b"})
	dm.SetRaw("nums", []int64{1, 2})
	dm.SetRaw("vals", []float64{1.1})
	dm.SetRaw("bools", []bool{true})

	arr, ok := dm.GetStringArray("tags")
	require.True(t, ok)
	require.Equal(t, []string{"a", "b"}, arr)

	intArr, ok := dm.GetIntArray("nums")
	require.True(t, ok)
	require.Equal(t, []int64{1, 2}, intArr)

	floatArr, ok := dm.GetFloatArray("vals")
	require.True(t, ok)
	require.Equal(t, []float64{1.1}, floatArr)

	boolArr, ok := dm.GetBoolArray("bools")
	require.True(t, ok)
	require.Equal(t, []bool{true}, boolArr)
}

func TestCollectionMetadataImplSetRawArrays(t *testing.T) {
	cm := NewEmptyMetadata()
	cm.SetRaw("tags", []string{"a"})
	cm.SetRaw("nums", []int64{1})
	cm.SetRaw("vals", []float64{1.1})
	cm.SetRaw("bools", []bool{true})

	arr, ok := cm.GetStringArray("tags")
	require.True(t, ok)
	require.Equal(t, []string{"a"}, arr)

	intArr, ok := cm.GetIntArray("nums")
	require.True(t, ok)
	require.Equal(t, []int64{1}, intArr)

	floatArr, ok := cm.GetFloatArray("vals")
	require.True(t, ok)
	require.Equal(t, []float64{1.1}, floatArr)

	boolArr, ok := cm.GetBoolArray("bools")
	require.True(t, ok)
	require.Equal(t, []bool{true}, boolArr)
}

func TestCollectionMetadataMarshalJSONWithArrays(t *testing.T) {
	cm := NewEmptyMetadata()
	cm.SetStringArray("tags", []string{"a", "b"})
	cm.SetIntArray("nums", []int64{1, 2})

	b, err := cm.MarshalJSON()
	require.NoError(t, err)

	var result map[string]interface{}
	err = json.Unmarshal(b, &result)
	require.NoError(t, err)
	require.Contains(t, result, "tags")
	require.Contains(t, result, "nums")
}

func TestDocumentMetadataMarshalJSONWithArrays(t *testing.T) {
	dm := NewDocumentMetadata()
	dm.SetStringArray("tags", []string{"x", "y"})
	dm.SetBoolArray("flags", []bool{true})

	impl := dm.(*DocumentMetadataImpl)
	b, err := impl.MarshalJSON()
	require.NoError(t, err)

	var result map[string]interface{}
	err = json.Unmarshal(b, &result)
	require.NoError(t, err)
	require.Contains(t, result, "tags")
	require.Contains(t, result, "flags")
}

func TestDocumentMetadataUnmarshalJSONWithArrays(t *testing.T) {
	input := `{"tags":["a","b"],"scores":[1,2],"ratios":[1.5,2.5],"flags":[true,false]}`
	impl := &DocumentMetadataImpl{}
	err := impl.UnmarshalJSON([]byte(input))
	require.NoError(t, err)

	arr, ok := impl.GetStringArray("tags")
	require.True(t, ok)
	require.Equal(t, []string{"a", "b"}, arr)

	intArr, ok := impl.GetIntArray("scores")
	require.True(t, ok)
	require.Equal(t, []int64{1, 2}, intArr)

	floatArr, ok := impl.GetFloatArray("ratios")
	require.True(t, ok)
	require.Equal(t, []float64{1.5, 2.5}, floatArr)

	boolArr, ok := impl.GetBoolArray("flags")
	require.True(t, ok)
	require.Equal(t, []bool{true, false}, boolArr)
}

func TestCollectionMetadataUnmarshalJSONWithArrays(t *testing.T) {
	input := `{"tags":["hello","world"],"nums":[10,20]}`
	impl := &CollectionMetadataImpl{}
	err := impl.UnmarshalJSON([]byte(input))
	require.NoError(t, err)

	arr, ok := impl.GetStringArray("tags")
	require.True(t, ok)
	require.Equal(t, []string{"hello", "world"}, arr)

	intArr, ok := impl.GetIntArray("nums")
	require.True(t, ok)
	require.Equal(t, []int64{10, 20}, intArr)
}

func TestNewStringArrayAttribute(t *testing.T) {
	attr := NewStringArrayAttribute("tags", []string{"a", "b"})
	require.Equal(t, "tags", attr.key)
	arr, ok := attr.value.GetStringArray()
	require.True(t, ok)
	require.Equal(t, []string{"a", "b"}, arr)
}

func TestNewIntArrayAttribute(t *testing.T) {
	attr := NewIntArrayAttribute("nums", []int64{1, 2})
	require.Equal(t, "nums", attr.key)
	arr, ok := attr.value.GetIntArray()
	require.True(t, ok)
	require.Equal(t, []int64{1, 2}, arr)
}

func TestNewFloatArrayAttribute(t *testing.T) {
	attr := NewFloatArrayAttribute("vals", []float64{1.5})
	require.Equal(t, "vals", attr.key)
	arr, ok := attr.value.GetFloatArray()
	require.True(t, ok)
	require.Equal(t, []float64{1.5}, arr)
}

func TestNewBoolArrayAttribute(t *testing.T) {
	attr := NewBoolArrayAttribute("flags", []bool{true, false})
	require.Equal(t, "flags", attr.key)
	arr, ok := attr.value.GetBoolArray()
	require.True(t, ok)
	require.Equal(t, []bool{true, false}, arr)
}

func TestNewArrayAttributeEmptyReturnsNil(t *testing.T) {
	require.Nil(t, NewStringArrayAttribute("k", []string{}))
	require.Nil(t, NewIntArrayAttribute("k", []int64{}))
	require.Nil(t, NewFloatArrayAttribute("k", []float64{}))
	require.Nil(t, NewBoolArrayAttribute("k", []bool{}))
}

func TestNewMetadataSkipsNilAttributes(t *testing.T) {
	md := NewMetadata(
		NewStringAttribute("name", "test"),
		NewStringArrayAttribute("empty", []string{}), // nil, should be skipped
		NewStringArrayAttribute("tags", []string{"a"}),
	)
	_, ok := md.GetStringArray("empty")
	require.False(t, ok)
	arr, ok := md.GetStringArray("tags")
	require.True(t, ok)
	require.Equal(t, []string{"a"}, arr)
}

func TestSetArrayEmptySliceIsNoop(t *testing.T) {
	cm := NewMetadata(NewStringAttribute("name", "test"))
	cm.SetStringArray("tags", []string{})
	cm.SetIntArray("nums", []int64{})
	cm.SetFloatArray("vals", []float64{})
	cm.SetBoolArray("flags", []bool{})
	_, ok := cm.GetStringArray("tags")
	require.False(t, ok)
	_, ok = cm.GetIntArray("nums")
	require.False(t, ok)
	_, ok = cm.GetFloatArray("vals")
	require.False(t, ok)
	_, ok = cm.GetBoolArray("flags")
	require.False(t, ok)

	dm := NewDocumentMetadata(NewStringAttribute("name", "test"))
	dm.SetStringArray("tags", []string{})
	dm.SetIntArray("nums", []int64{})
	dm.SetFloatArray("vals", []float64{})
	dm.SetBoolArray("flags", []bool{})
	_, ok = dm.GetStringArray("tags")
	require.False(t, ok)
	_, ok = dm.GetIntArray("nums")
	require.False(t, ok)
	_, ok = dm.GetFloatArray("vals")
	require.False(t, ok)
	_, ok = dm.GetBoolArray("flags")
	require.False(t, ok)
}

func TestUnmarshalArrayMixedTypesError(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"bool then string", `[true, "hello"]`},
		{"string then number", `["hello", 42]`},
		{"number then bool", `[42, true]`},
		{"string then bool", `["hello", false]`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var mv MetadataValue
			err := json.Unmarshal([]byte(tt.input), &mv)
			require.Error(t, err)
			require.Contains(t, err.Error(), "mixed types")
		})
	}
}

func TestUnmarshalArrayNullRejected(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"null first element", `[null, "hello"]`},
		{"null in string array", `["hello", null]`},
		{"null in bool array", `[true, null]`},
		{"null in number array", `[42, null]`},
		{"only null", `[null]`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var mv MetadataValue
			err := json.Unmarshal([]byte(tt.input), &mv)
			require.Error(t, err)
			require.Contains(t, err.Error(), "null")
		})
	}
}

func TestConvertInterfaceSliceGoIntTypes(t *testing.T) {
	tests := []struct {
		name  string
		input []interface{}
	}{
		{"int", []interface{}{int(1), int(2), int(3)}},
		{"int8", []interface{}{int8(1), int8(2), int8(3)}},
		{"int16", []interface{}{int16(1), int16(2), int16(3)}},
		{"int32", []interface{}{int32(1), int32(2), int32(3)}},
		{"int64", []interface{}{int64(1), int64(2), int64(3)}},
		{"uint", []interface{}{uint(1), uint(2), uint(3)}},
		{"uint8", []interface{}{uint8(1), uint8(2), uint8(3)}},
		{"uint16", []interface{}{uint16(1), uint16(2), uint16(3)}},
		{"uint32", []interface{}{uint32(1), uint32(2), uint32(3)}},
		{"uint64", []interface{}{uint64(1), uint64(2), uint64(3)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mv, err := convertInterfaceSliceToMetadataValue(tt.input)
			require.NoError(t, err)
			require.Equal(t, []int64{1, 2, 3}, mv.IntArray)
		})
	}
}

func TestConvertInterfaceSliceGoIntMixedWithOtherType(t *testing.T) {
	_, err := convertInterfaceSliceToMetadataValue([]interface{}{1, "two", 3})
	require.Error(t, err)
	require.Contains(t, err.Error(), "mixed types")
}

func TestNewMetadataFromMapWithGoIntInterfaceSlice(t *testing.T) {
	m := NewMetadataFromMap(map[string]interface{}{
		"ids": []interface{}{1, 2, 3},
	})
	arr, ok := m.GetIntArray("ids")
	require.True(t, ok)
	require.Equal(t, []int64{1, 2, 3}, arr)
}

func TestConvertInterfaceSliceUintOverflow(t *testing.T) {
	tests := []struct {
		name  string
		input []interface{}
	}{
		{"uint", []interface{}{uint(math.MaxInt64 + 1)}},
		{"uint64", []interface{}{uint64(math.MaxInt64 + 1)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := convertInterfaceSliceToMetadataValue(tt.input)
			require.Error(t, err)
			require.Contains(t, err.Error(), "overflow")
		})
	}
}

func TestConvertInterfaceSliceUintMaxInt64IsValid(t *testing.T) {
	mv, err := convertInterfaceSliceToMetadataValue([]interface{}{uint64(math.MaxInt64)})
	require.NoError(t, err)
	require.Equal(t, []int64{math.MaxInt64}, mv.IntArray)
}

func TestConvertInterfaceSliceFloat64(t *testing.T) {
	mv, err := convertInterfaceSliceToMetadataValue([]interface{}{1.0, 2.5, 3.0})
	require.NoError(t, err)
	require.Equal(t, []float64{1.0, 2.5, 3.0}, mv.FloatArray)
}

func TestUnmarshalArrayMixedIntFloat(t *testing.T) {
	var mv MetadataValue
	err := json.Unmarshal([]byte(`[1, 2.5, 3]`), &mv)
	require.NoError(t, err)
	require.Equal(t, []float64{1.0, 2.5, 3.0}, mv.FloatArray)
}

func TestSetRawEmptyArrayIsNoop(t *testing.T) {
	dm := NewDocumentMetadata()
	dm.SetRaw("tags", []string{})
	dm.SetRaw("nums", []int64{})
	dm.SetRaw("vals", []float64{})
	dm.SetRaw("flags", []bool{})
	_, ok := dm.GetStringArray("tags")
	require.False(t, ok)
	_, ok = dm.GetIntArray("nums")
	require.False(t, ok)
	_, ok = dm.GetFloatArray("vals")
	require.False(t, ok)
	_, ok = dm.GetBoolArray("flags")
	require.False(t, ok)

	cm := NewEmptyMetadata()
	cm.SetRaw("tags", []string{})
	cm.SetRaw("nums", []int64{})
	cm.SetRaw("vals", []float64{})
	cm.SetRaw("flags", []bool{})
	_, ok = cm.GetStringArray("tags")
	require.False(t, ok)
	_, ok = cm.GetIntArray("nums")
	require.False(t, ok)
	_, ok = cm.GetFloatArray("vals")
	require.False(t, ok)
	_, ok = cm.GetBoolArray("flags")
	require.False(t, ok)
}

func TestGetArrayReturnsCopy(t *testing.T) {
	mv := MetadataValue{StringArray: []string{"a", "b"}}
	arr, ok := mv.GetStringArray()
	require.True(t, ok)
	arr[0] = "modified"
	original, _ := mv.GetStringArray()
	require.Equal(t, "a", original[0])

	mv2 := MetadataValue{IntArray: []int64{1, 2}}
	intArr, ok := mv2.GetIntArray()
	require.True(t, ok)
	intArr[0] = 99
	originalInt, _ := mv2.GetIntArray()
	require.Equal(t, int64(1), originalInt[0])

	mv3 := MetadataValue{FloatArray: []float64{1.1, 2.2}}
	floatArr, ok := mv3.GetFloatArray()
	require.True(t, ok)
	floatArr[0] = 99.9
	originalFloat, _ := mv3.GetFloatArray()
	require.Equal(t, 1.1, originalFloat[0])

	mv4 := MetadataValue{BoolArray: []bool{true, false}}
	boolArr, ok := mv4.GetBoolArray()
	require.True(t, ok)
	boolArr[0] = false
	originalBool, _ := mv4.GetBoolArray()
	require.Equal(t, true, originalBool[0])
}

func TestGetRawArrayReturnsCopy(t *testing.T) {
	mv := MetadataValue{StringArray: []string{"a", "b"}}
	raw, ok := mv.GetRaw()
	require.True(t, ok)
	raw.([]string)[0] = "modified"
	original, _ := mv.GetRaw()
	require.Equal(t, "a", original.([]string)[0])

	mv2 := MetadataValue{IntArray: []int64{1, 2}}
	raw2, ok := mv2.GetRaw()
	require.True(t, ok)
	raw2.([]int64)[0] = 99
	original2, _ := mv2.GetRaw()
	require.Equal(t, int64(1), original2.([]int64)[0])

	mv3 := MetadataValue{FloatArray: []float64{1.1, 2.2}}
	raw3, ok := mv3.GetRaw()
	require.True(t, ok)
	raw3.([]float64)[0] = 99.9
	original3, _ := mv3.GetRaw()
	require.Equal(t, 1.1, original3.([]float64)[0])

	mv4 := MetadataValue{BoolArray: []bool{true, false}}
	raw4, ok := mv4.GetRaw()
	require.True(t, ok)
	raw4.([]bool)[0] = false
	original4, _ := mv4.GetRaw()
	require.Equal(t, true, original4.([]bool)[0])
}
