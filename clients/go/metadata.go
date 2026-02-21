package chroma

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type CollectionMetadata interface {
	Keys() []string
	GetRaw(key string) (interface{}, bool)
	GetString(key string) (string, bool)
	GetInt(key string) (int64, bool)
	GetFloat(key string) (float64, bool)
	GetBool(key string) (bool, bool)
	GetStringArray(key string) ([]string, bool)
	GetIntArray(key string) ([]int64, bool)
	GetFloatArray(key string) ([]float64, bool)
	GetBoolArray(key string) ([]bool, bool)
	SetRaw(key string, value interface{})
	SetString(key, value string)
	SetInt(key string, value int64)
	SetFloat(key string, value float64)
	SetBool(key string, value bool)
	SetStringArray(key string, value []string)
	SetIntArray(key string, value []int64)
	SetFloatArray(key string, value []float64)
	SetBoolArray(key string, value []bool)
	MarshalJSON() ([]byte, error)
	UnmarshalJSON(b []byte) error
}

type MetadataValue struct {
	Bool        *bool     `json:"-"`
	Float64     *float64  `json:"-"`
	Int         *int64    `json:"-"`
	StringValue *string   `json:"-"`
	NilValue    bool      `json:"-"`
	StringArray []string  `json:"-"`
	IntArray    []int64   `json:"-"`
	FloatArray  []float64 `json:"-"`
	BoolArray   []bool    `json:"-"`
}

type MetaAttribute struct {
	key       string
	value     MetadataValue
	valueType reflect.Type
}

func NewStringAttribute(key string, value string) *MetaAttribute {
	return &MetaAttribute{key: key, value: MetadataValue{StringValue: &value}, valueType: reflect.TypeOf(value)}
}

func NewIntAttribute(key string, value int64) *MetaAttribute {
	return &MetaAttribute{key: key, value: MetadataValue{Int: &value}, valueType: reflect.TypeOf(value)}
}

func NewFloatAttribute(key string, value float64) *MetaAttribute {
	return &MetaAttribute{key: key, value: MetadataValue{Float64: &value}, valueType: reflect.TypeOf(value)}
}

func RemoveAttribute(key string) *MetaAttribute {
	return &MetaAttribute{key: key, value: MetadataValue{NilValue: true}, valueType: nil}
}

func NewBoolAttribute(key string, value bool) *MetaAttribute {
	return &MetaAttribute{key: key, value: MetadataValue{Bool: &value}, valueType: reflect.TypeOf(value)}
}

func NewStringArrayAttribute(key string, values []string) *MetaAttribute {
	if len(values) == 0 {
		return nil
	}
	cp := make([]string, len(values))
	copy(cp, values)
	return &MetaAttribute{key: key, value: MetadataValue{StringArray: cp}, valueType: reflect.TypeOf(values)}
}

func NewIntArrayAttribute(key string, values []int64) *MetaAttribute {
	if len(values) == 0 {
		return nil
	}
	cp := make([]int64, len(values))
	copy(cp, values)
	return &MetaAttribute{key: key, value: MetadataValue{IntArray: cp}, valueType: reflect.TypeOf(values)}
}

func NewFloatArrayAttribute(key string, values []float64) *MetaAttribute {
	if len(values) == 0 {
		return nil
	}
	cp := make([]float64, len(values))
	copy(cp, values)
	return &MetaAttribute{key: key, value: MetadataValue{FloatArray: cp}, valueType: reflect.TypeOf(values)}
}

func NewBoolArrayAttribute(key string, values []bool) *MetaAttribute {
	if len(values) == 0 {
		return nil
	}
	cp := make([]bool, len(values))
	copy(cp, values)
	return &MetaAttribute{key: key, value: MetadataValue{BoolArray: cp}, valueType: reflect.TypeOf(values)}
}

func (mv *MetadataValue) GetInt() (int64, bool) {
	if mv.Int == nil {
		return 0, false
	}
	return *mv.Int, true
}

func (mv *MetadataValue) String() string {
	if mv.Bool != nil {
		return fmt.Sprintf("%v", *mv.Bool)
	}
	if mv.Float64 != nil {
		return fmt.Sprintf("%v", *mv.Float64)
	}
	if mv.Int != nil {
		return fmt.Sprintf("%v", *mv.Int)
	}
	if mv.StringValue != nil {
		return *mv.StringValue
	}
	if mv.StringArray != nil {
		return fmt.Sprintf("%v", mv.StringArray)
	}
	if mv.IntArray != nil {
		return fmt.Sprintf("%v", mv.IntArray)
	}
	if mv.FloatArray != nil {
		return fmt.Sprintf("%v", mv.FloatArray)
	}
	if mv.BoolArray != nil {
		return fmt.Sprintf("%v", mv.BoolArray)
	}
	return ""
}

func (mv *MetadataValue) GetFloat() (float64, bool) {
	if mv.Float64 == nil {
		return 0, false
	}
	return *mv.Float64, true
}

func (mv *MetadataValue) GetBool() (bool, bool) {
	if mv.Bool == nil {
		return false, false
	}
	return *mv.Bool, true
}

func (mv *MetadataValue) GetString() (string, bool) {
	if mv.StringValue == nil {
		return "", false
	}
	return *mv.StringValue, true
}

func (mv *MetadataValue) GetRaw() (interface{}, bool) {
	if mv.Bool != nil {
		return *mv.Bool, true
	}
	if mv.Float64 != nil {
		return *mv.Float64, true
	}
	if mv.Int != nil {
		return *mv.Int, true
	}
	if mv.StringValue != nil {
		return *mv.StringValue, true
	}
	if mv.StringArray != nil {
		cp := make([]string, len(mv.StringArray))
		copy(cp, mv.StringArray)
		return cp, true
	}
	if mv.IntArray != nil {
		cp := make([]int64, len(mv.IntArray))
		copy(cp, mv.IntArray)
		return cp, true
	}
	if mv.FloatArray != nil {
		cp := make([]float64, len(mv.FloatArray))
		copy(cp, mv.FloatArray)
		return cp, true
	}
	if mv.BoolArray != nil {
		cp := make([]bool, len(mv.BoolArray))
		copy(cp, mv.BoolArray)
		return cp, true
	}
	return nil, false
}

func (mv *MetadataValue) GetStringArray() ([]string, bool) {
	if mv.StringArray == nil {
		return nil, false
	}
	cp := make([]string, len(mv.StringArray))
	copy(cp, mv.StringArray)
	return cp, true
}

func (mv *MetadataValue) GetIntArray() ([]int64, bool) {
	if mv.IntArray == nil {
		return nil, false
	}
	cp := make([]int64, len(mv.IntArray))
	copy(cp, mv.IntArray)
	return cp, true
}

func (mv *MetadataValue) GetFloatArray() ([]float64, bool) {
	if mv.FloatArray == nil {
		return nil, false
	}
	cp := make([]float64, len(mv.FloatArray))
	copy(cp, mv.FloatArray)
	return cp, true
}

func (mv *MetadataValue) GetBoolArray() ([]bool, bool) {
	if mv.BoolArray == nil {
		return nil, false
	}
	cp := make([]bool, len(mv.BoolArray))
	copy(cp, mv.BoolArray)
	return cp, true
}

func (mv *MetadataValue) Equal(other *MetadataValue) bool {
	if mv.Bool != nil && other.Bool != nil {
		return *mv.Bool == *other.Bool
	}
	if mv.Float64 != nil && other.Float64 != nil {
		return *mv.Float64 == *other.Float64
	}
	if mv.Int != nil && other.Int != nil {
		return *mv.Int == *other.Int
	}
	if mv.StringValue != nil && other.StringValue != nil {
		return *mv.StringValue == *other.StringValue
	}
	if mv.StringArray != nil && other.StringArray != nil {
		return reflect.DeepEqual(mv.StringArray, other.StringArray)
	}
	if mv.IntArray != nil && other.IntArray != nil {
		return reflect.DeepEqual(mv.IntArray, other.IntArray)
	}
	if mv.FloatArray != nil && other.FloatArray != nil {
		return reflect.DeepEqual(mv.FloatArray, other.FloatArray)
	}
	if mv.BoolArray != nil && other.BoolArray != nil {
		return reflect.DeepEqual(mv.BoolArray, other.BoolArray)
	}
	return false
}

// MarshalJSON ensures only the correct type is serialized.
func (mv *MetadataValue) MarshalJSON() ([]byte, error) {
	if mv.NilValue {
		return json.Marshal(nil)
	}
	if mv.Bool != nil {
		return json.Marshal(*mv.Bool)
	}
	if mv.Float64 != nil {
		return []byte(strconv.FormatFloat(*mv.Float64, 'e', -1, 64)), nil
	}
	if mv.Int != nil {
		return json.Marshal(float64(*mv.Int)) // Ensure int64 is converted to float64
	}
	if mv.StringValue != nil {
		return json.Marshal(*mv.StringValue)
	}
	if mv.StringArray != nil {
		return json.Marshal(mv.StringArray)
	}
	if mv.IntArray != nil {
		return json.Marshal(mv.IntArray)
	}
	if mv.FloatArray != nil {
		return json.Marshal(mv.FloatArray)
	}
	if mv.BoolArray != nil {
		return json.Marshal(mv.BoolArray)
	}
	return json.Marshal(nil)
}

// UnmarshalJSON properly detects and assigns the correct type.
func (mv *MetadataValue) UnmarshalJSON(b []byte) error {
	var raw json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}

	trimmed := bytes.TrimSpace(raw)

	// Check if it's an array
	if len(trimmed) > 0 && trimmed[0] == '[' {
		return mv.unmarshalArray(trimmed)
	}

	// Try to parse as bool
	var boolVal bool
	if err := json.Unmarshal(raw, &boolVal); err == nil {
		mv.Bool = &boolVal
		return nil
	}

	// Try to parse as string
	var strVal string
	if err := json.Unmarshal(raw, &strVal); err == nil {
		mv.StringValue = &strVal
		return nil
	}

	// Try to parse as json.Number to differentiate int and float
	var num json.Number
	if err := json.Unmarshal(raw, &num); err == nil {
		if fv, rr := num.Int64(); rr == nil {
			mv.Int = &fv
			return nil
		}
		if fv, rr := num.Float64(); rr == nil {
			mv.Float64 = &fv
			return nil
		}
	}
	return errors.New("data failed to match schemas in anyOf(Metadata)")
}

func (mv *MetadataValue) unmarshalArray(b []byte) error {
	var rawElements []json.RawMessage
	if err := json.Unmarshal(b, &rawElements); err != nil {
		return errors.Wrap(err, "failed to unmarshal metadata array")
	}
	if len(rawElements) == 0 {
		return errors.New("metadata arrays must be non-empty")
	}

	first := bytes.TrimSpace(rawElements[0])
	if len(first) == 0 {
		return errors.New("invalid empty element in metadata array")
	}

	// Determine expected type from first element
	var expectedType string
	switch {
	case first[0] == '[':
		return errors.New("nested arrays are not supported in metadata values; only flat arrays of string, int, float, or bool are allowed")
	case first[0] == '{':
		return errors.New("arrays of objects are not supported in metadata values; only flat arrays of string, int, float, or bool are allowed")
	case first[0] == '"':
		expectedType = "string"
	case bytes.Equal(first, []byte("true")) || bytes.Equal(first, []byte("false")):
		expectedType = "bool"
	case bytes.Equal(first, []byte("null")):
		return errors.New("null values are not allowed in metadata arrays")
	default:
		expectedType = "number"
	}

	// Validate all elements have the same type
	for i := 1; i < len(rawElements); i++ {
		elem := bytes.TrimSpace(rawElements[i])
		if len(elem) == 0 {
			return errors.Errorf("invalid empty element at index %d in metadata array", i)
		}
		if bytes.Equal(elem, []byte("null")) {
			return errors.Errorf("null values are not allowed in metadata arrays (index %d)", i)
		}
		var elemType string
		switch {
		case elem[0] == '"':
			elemType = "string"
		case bytes.Equal(elem, []byte("true")) || bytes.Equal(elem, []byte("false")):
			elemType = "bool"
		case elem[0] == '[':
			return errors.New("nested arrays are not supported in metadata values; only flat arrays of string, int, float, or bool are allowed")
		case elem[0] == '{':
			return errors.New("arrays of objects are not supported in metadata values; only flat arrays of string, int, float, or bool are allowed")
		default:
			elemType = "number"
		}
		if elemType != expectedType {
			return errors.Errorf("metadata array has mixed types: expected %s at index %d, got %s", expectedType, i, elemType)
		}
	}

	// All elements are the same type, unmarshal
	switch expectedType {
	case "string":
		var arr []string
		if err := json.Unmarshal(b, &arr); err != nil {
			return errors.Wrap(err, "failed to unmarshal string array metadata")
		}
		mv.StringArray = arr
		return nil
	case "bool":
		var arr []bool
		if err := json.Unmarshal(b, &arr); err != nil {
			return errors.Wrap(err, "failed to unmarshal bool array metadata")
		}
		mv.BoolArray = arr
		return nil
	default:
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.UseNumber()
		var nums []json.Number
		if err := dec.Decode(&nums); err != nil {
			return errors.Wrap(err, "failed to unmarshal numeric array metadata")
		}
		allInts := true
		for _, n := range nums {
			s := n.String()
			if strings.Contains(s, ".") || strings.Contains(s, "e") || strings.Contains(s, "E") {
				allInts = false
				break
			}
		}
		if allInts {
			intArr := make([]int64, len(nums))
			for i, n := range nums {
				v, err := n.Int64()
				if err != nil {
					return errors.Wrapf(err, "failed to parse int at index %d in metadata array", i)
				}
				intArr[i] = v
			}
			mv.IntArray = intArr
		} else {
			floatArr := make([]float64, len(nums))
			for i, n := range nums {
				v, err := n.Float64()
				if err != nil {
					return errors.Wrapf(err, "failed to parse float at index %d in metadata array", i)
				}
				floatArr[i] = v
			}
			mv.FloatArray = floatArr
		}
		return nil
	}
}

// ValidateArrayMetadata checks that array metadata values are non-empty.
func ValidateArrayMetadata(mv *MetadataValue) error {
	if mv.StringArray != nil && len(mv.StringArray) == 0 {
		return errors.New("metadata arrays must be non-empty")
	}
	if mv.IntArray != nil && len(mv.IntArray) == 0 {
		return errors.New("metadata arrays must be non-empty")
	}
	if mv.FloatArray != nil && len(mv.FloatArray) == 0 {
		return errors.New("metadata arrays must be non-empty")
	}
	if mv.BoolArray != nil && len(mv.BoolArray) == 0 {
		return errors.New("metadata arrays must be non-empty")
	}
	return nil
}

func validateDocumentMetadatas(metadatas []DocumentMetadata) error {
	for i, md := range metadatas {
		if md == nil {
			continue
		}
		impl, ok := md.(*DocumentMetadataImpl)
		if !ok {
			continue
		}
		for key, mv := range impl.metadata {
			if err := ValidateArrayMetadata(&mv); err != nil {
				return errors.Wrapf(err, "invalid metadata for document at index %d, key %q", i, key)
			}
		}
	}
	return nil
}

type goInt interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

func convertIntSlice[T goInt](slice []interface{}) (MetadataValue, error) {
	arr := make([]int64, len(slice))
	for i, v := range slice {
		n, ok := v.(T)
		if !ok {
			return MetadataValue{}, errors.Errorf("metadata array has mixed types: expected integer at index %d, got %T", i, v)
		}
		if uint64(n) > math.MaxInt64 && n > 0 {
			return MetadataValue{}, errors.Errorf("metadata integer overflow at index %d: value %v exceeds int64 range", i, v)
		}
		arr[i] = int64(n)
	}
	return MetadataValue{IntArray: arr}, nil
}

func convertInterfaceSliceToMetadataValue(slice []interface{}) (MetadataValue, error) {
	if len(slice) == 0 {
		return MetadataValue{}, errors.New("metadata arrays must be non-empty")
	}
	switch slice[0].(type) {
	case string:
		arr := make([]string, len(slice))
		for i, v := range slice {
			s, ok := v.(string)
			if !ok {
				return MetadataValue{}, errors.Errorf("metadata array has mixed types: expected string at index %d, got %T", i, v)
			}
			arr[i] = s
		}
		return MetadataValue{StringArray: arr}, nil
	case bool:
		arr := make([]bool, len(slice))
		for i, v := range slice {
			b, ok := v.(bool)
			if !ok {
				return MetadataValue{}, errors.Errorf("metadata array has mixed types: expected bool at index %d, got %T", i, v)
			}
			arr[i] = b
		}
		return MetadataValue{BoolArray: arr}, nil
	case float64:
		arr := make([]float64, len(slice))
		for i, v := range slice {
			f, ok := v.(float64)
			if !ok {
				return MetadataValue{}, errors.Errorf("metadata array has mixed types: expected number at index %d, got %T", i, v)
			}
			arr[i] = f
		}
		return MetadataValue{FloatArray: arr}, nil
	case json.Number:
		allInts := true
		for _, v := range slice {
			n, ok := v.(json.Number)
			if !ok {
				return MetadataValue{}, errors.Errorf("metadata array has mixed types: expected number, got %T", v)
			}
			s := n.String()
			if strings.Contains(s, ".") || strings.Contains(s, "e") || strings.Contains(s, "E") {
				allInts = false
				break
			}
		}
		if allInts {
			arr := make([]int64, len(slice))
			for i, v := range slice {
				n, ok := v.(json.Number)
				if !ok {
					return MetadataValue{}, errors.Errorf("unexpected type %T at index %d, expected json.Number", v, i)
				}
				iv, err := n.Int64()
				if err != nil {
					return MetadataValue{}, errors.Wrapf(err, "failed to convert to int64 at index %d", i)
				}
				arr[i] = iv
			}
			return MetadataValue{IntArray: arr}, nil
		}
		arr := make([]float64, len(slice))
		for i, v := range slice {
			n, ok := v.(json.Number)
			if !ok {
				return MetadataValue{}, errors.Errorf("unexpected type %T at index %d, expected json.Number", v, i)
			}
			fv, err := n.Float64()
			if err != nil {
				return MetadataValue{}, errors.Wrapf(err, "failed to convert to float64 at index %d", i)
			}
			arr[i] = fv
		}
		return MetadataValue{FloatArray: arr}, nil
	case int:
		return convertIntSlice[int](slice)
	case int8:
		return convertIntSlice[int8](slice)
	case int16:
		return convertIntSlice[int16](slice)
	case int32:
		return convertIntSlice[int32](slice)
	case int64:
		return convertIntSlice[int64](slice)
	case uint:
		return convertIntSlice[uint](slice)
	case uint8:
		return convertIntSlice[uint8](slice)
	case uint16:
		return convertIntSlice[uint16](slice)
	case uint32:
		return convertIntSlice[uint32](slice)
	case uint64:
		return convertIntSlice[uint64](slice)
	case []interface{}:
		return MetadataValue{}, errors.New("nested arrays are not supported in metadata values; only flat arrays of string, int, float, or bool are allowed")
	default:
		return MetadataValue{}, errors.Errorf("unsupported metadata array element type: %T; only string, int, float, and bool arrays are supported", slice[0])
	}
}

// Collection metadata
type CollectionMetadataImpl struct {
	metadata map[string]MetadataValue
}

func NewMetadata(attributes ...*MetaAttribute) CollectionMetadata {
	metadata := make(map[string]MetadataValue)
	for _, attribute := range attributes {
		if attribute == nil {
			continue
		}
		metadata[attribute.key] = attribute.value
	}
	return &DocumentMetadataImpl{metadata: metadata}
}

func NewEmptyMetadata() CollectionMetadata {
	return &CollectionMetadataImpl{metadata: map[string]MetadataValue{}}
}

func NewMetadataFromMap(metadata map[string]interface{}) CollectionMetadata {
	if metadata == nil {
		return NewMetadata()
	}

	mv := &CollectionMetadataImpl{metadata: make(map[string]MetadataValue)}

	for k, v := range metadata {
		switch val := v.(type) {
		case json.Number:
			numStr := string(val)
			if strings.Contains(numStr, ".") || strings.Contains(numStr, "e") || strings.Contains(numStr, "E") {
				if floatVal, err := val.Float64(); err == nil {
					mv.SetFloat(k, floatVal)
				}
			} else {
				if intVal, err := val.Int64(); err == nil {
					mv.SetInt(k, intVal)
				}
			}
		case bool:
			mv.SetBool(k, val)
		case float32:
			mv.SetFloat(k, float64(val))
		case float64:
			mv.SetFloat(k, val)
		case int:
			mv.SetInt(k, int64(val))
		case int32:
			mv.SetInt(k, int64(val))
		case int64:
			mv.SetInt(k, val)
		case string:
			mv.SetString(k, val)
		case []string:
			mv.SetStringArray(k, val)
		case []int64:
			mv.SetIntArray(k, val)
		case []float64:
			mv.SetFloatArray(k, val)
		case []bool:
			mv.SetBoolArray(k, val)
		case []interface{}:
			if arr, err := convertInterfaceSliceToMetadataValue(val); err == nil {
				mv.metadata[k] = arr
			}
		}
	}
	return mv
}

func (cm *CollectionMetadataImpl) Keys() []string {
	keys := make([]string, 0, len(cm.metadata))
	for k := range cm.metadata {
		keys = append(keys, k)
	}
	return keys
}

func (cm *CollectionMetadataImpl) GetRaw(key string) (value interface{}, ok bool) {
	v, ok := cm.metadata[key]
	return v, ok
}

func (cm *CollectionMetadataImpl) GetString(key string) (value string, ok bool) {
	v, ok := cm.metadata[key]
	if !ok {
		return "", false
	}
	str, ok := v.GetString()
	return str, ok
}

func (cm *CollectionMetadataImpl) GetInt(key string) (value int64, ok bool) {
	v, ok := cm.metadata[key]
	if !ok {
		return 0, false
	}
	i, ok := v.GetInt()
	return i, ok
}

func (cm *CollectionMetadataImpl) GetFloat(key string) (value float64, ok bool) {
	v, ok := cm.metadata[key]
	if !ok {
		return 0, false
	}
	f, ok := v.GetFloat()
	return f, ok
}

func (cm *CollectionMetadataImpl) GetBool(key string) (value bool, ok bool) {
	v, ok := cm.metadata[key]
	if !ok {
		return false, false
	}
	b, ok := v.GetBool()
	return b, ok
}

func (cm *CollectionMetadataImpl) GetStringArray(key string) ([]string, bool) {
	v, ok := cm.metadata[key]
	if !ok {
		return nil, false
	}
	return v.GetStringArray()
}

func (cm *CollectionMetadataImpl) GetIntArray(key string) ([]int64, bool) {
	v, ok := cm.metadata[key]
	if !ok {
		return nil, false
	}
	return v.GetIntArray()
}

func (cm *CollectionMetadataImpl) GetFloatArray(key string) ([]float64, bool) {
	v, ok := cm.metadata[key]
	if !ok {
		return nil, false
	}
	return v.GetFloatArray()
}

func (cm *CollectionMetadataImpl) GetBoolArray(key string) ([]bool, bool) {
	v, ok := cm.metadata[key]
	if !ok {
		return nil, false
	}
	return v.GetBoolArray()
}

func (cm *CollectionMetadataImpl) SetRaw(key string, value interface{}) {
	switch val := value.(type) {
	case bool:
		cm.metadata[key] = MetadataValue{Bool: &val}
	case float32:
		var f64 = float64(val)
		cm.metadata[key] = MetadataValue{Float64: &f64}
	case float64:
		cm.metadata[key] = MetadataValue{Float64: &val}
	case int:
		tv := int64(val)
		cm.metadata[key] = MetadataValue{Int: &tv}
	case int32:
		tv := int64(val)
		cm.metadata[key] = MetadataValue{Int: &tv}
	case int64:
		cm.metadata[key] = MetadataValue{Int: &val}
	case string:
		cm.metadata[key] = MetadataValue{StringValue: &val}
	case []string:
		cm.SetStringArray(key, val)
	case []int64:
		cm.SetIntArray(key, val)
	case []float64:
		cm.SetFloatArray(key, val)
	case []bool:
		cm.SetBoolArray(key, val)
	}
}

func (cm *CollectionMetadataImpl) SetString(key, value string) {
	cm.metadata[key] = MetadataValue{StringValue: &value}
}

func (cm *CollectionMetadataImpl) SetInt(key string, value int64) {
	cm.metadata[key] = MetadataValue{Int: &value}
}

func (cm *CollectionMetadataImpl) SetFloat(key string, value float64) {
	cm.metadata[key] = MetadataValue{Float64: &value}
}

func (cm *CollectionMetadataImpl) SetBool(key string, value bool) {
	cm.metadata[key] = MetadataValue{Bool: &value}
}

func (cm *CollectionMetadataImpl) SetStringArray(key string, value []string) {
	if len(value) == 0 {
		return
	}
	cp := make([]string, len(value))
	copy(cp, value)
	cm.metadata[key] = MetadataValue{StringArray: cp}
}

func (cm *CollectionMetadataImpl) SetIntArray(key string, value []int64) {
	if len(value) == 0 {
		return
	}
	cp := make([]int64, len(value))
	copy(cp, value)
	cm.metadata[key] = MetadataValue{IntArray: cp}
}

func (cm *CollectionMetadataImpl) SetFloatArray(key string, value []float64) {
	if len(value) == 0 {
		return
	}
	cp := make([]float64, len(value))
	copy(cp, value)
	cm.metadata[key] = MetadataValue{FloatArray: cp}
}

func (cm *CollectionMetadataImpl) SetBoolArray(key string, value []bool) {
	if len(value) == 0 {
		return
	}
	cp := make([]bool, len(value))
	copy(cp, value)
	cm.metadata[key] = MetadataValue{BoolArray: cp}
}

func (cm *CollectionMetadataImpl) MarshalJSON() ([]byte, error) {
	processed := make(map[string]interface{})
	for k, v := range cm.metadata {
		switch val, _ := v.GetRaw(); val.(type) {
		case bool:
			processed[k], _ = v.GetBool()
		case int, int32, int64:
			processed[k], _ = v.GetInt()
		case float64, float32:
			// Chroma stores f32 on the server; Go parses JSON floats as f64.
			// Both scientific notation (here) and decimal (DocumentMetadataImpl)
			// round-trip losslessly through Chroma's f32 precision (~7 digits).
			processed[k] = &MetadataValue{
				Float64: v.Float64,
			}
		case string:
			processed[k], _ = v.GetString()
		case []string:
			cp := make([]string, len(v.StringArray))
			copy(cp, v.StringArray)
			processed[k] = cp
		case []int64:
			cp := make([]int64, len(v.IntArray))
			copy(cp, v.IntArray)
			processed[k] = cp
		case []float64:
			cp := make([]float64, len(v.FloatArray))
			copy(cp, v.FloatArray)
			processed[k] = cp
		case []bool:
			cp := make([]bool, len(v.BoolArray))
			copy(cp, v.BoolArray)
			processed[k] = cp
		}
	}
	b := bytes.NewBuffer(nil)
	encoder := json.NewEncoder(b)
	err := encoder.Encode(processed)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal metadata")
	}
	return b.Bytes(), nil
}

func (cm *CollectionMetadataImpl) UnmarshalJSON(b []byte) error {
	err := json.Unmarshal(b, &cm.metadata)
	if err != nil {
		return errors.Wrap(err, "failed to unmarshal metadata")
	}

	if cm.metadata == nil {
		cm.metadata = make(map[string]MetadataValue)
	}
	return nil
}
