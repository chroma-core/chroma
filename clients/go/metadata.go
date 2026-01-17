package chroma

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	"github.com/pkg/errors"
)

type CollectionMetadata interface {
	Keys() []string
	GetRaw(key string) (interface{}, bool)
	GetString(key string) (string, bool)
	GetInt(key string) (int64, bool)
	GetFloat(key string) (float64, bool)
	GetBool(key string) (bool, bool)
	SetRaw(key string, value interface{})
	SetString(key, value string)
	SetInt(key string, value int64)
	SetFloat(key string, value float64)
	SetBool(key string, value bool)
	MarshalJSON() ([]byte, error)
	UnmarshalJSON(b []byte) error
}

type MetadataValue struct {
	Bool        *bool    `json:"-"`
	Float64     *float64 `json:"-"`
	Int         *int64   `json:"-"`
	StringValue *string  `json:"-"`
	NilValue    bool     `json:"-"`
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
	return nil, false
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
	return json.Marshal(nil)
}

// UnmarshalJSON properly detects and assigns the correct type.
func (mv *MetadataValue) UnmarshalJSON(b []byte) error {
	var raw json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
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

// Collection metadata
type CollectionMetadataImpl struct {
	metadata map[string]MetadataValue
}

func NewMetadata(attributes ...*MetaAttribute) CollectionMetadata {
	metadata := make(map[string]MetadataValue)
	for _, attribute := range attributes {
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

func (cm *CollectionMetadataImpl) MarshalJSON() ([]byte, error) {
	processed := make(map[string]interface{})
	for k, v := range cm.metadata {
		switch val, _ := v.GetRaw(); val.(type) {
		case bool:
			processed[k], _ = v.GetBool()
		case int, int32, int64:
			processed[k], _ = v.GetInt()
		case float64, float32:
			processed[k] = &MetadataValue{
				Float64: v.Float64,
			}
		case string:
			processed[k], _ = v.GetString()
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
