package chroma

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

type DocumentMetadata interface {
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
}

type DocumentMetadatas []DocumentMetadata

type DocumentID string

type DocumentIDs []DocumentID

type Document interface {
	ContentRaw() []byte
	ContentString() string
}

type Documents []Document

type TextDocument struct {
	Content string
}

type TextDocuments []TextDocument

func NewTextDocument(content string) *TextDocument {
	return &TextDocument{Content: content}
}

func (d *TextDocument) ContentRaw() []byte {
	return []byte(d.Content)
}

func (d *TextDocument) ContentString() string {
	return d.Content
}

func (d *TextDocument) UnmarshalJSON(data []byte) error {
	d.Content = string(data)
	return nil
}

func (d *TextDocument) String() string {
	return d.Content
}

func (d *TextDocument) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Content)
}

func NewTextDocumentsFromInterface(docs []interface{}) (TextDocuments, error) {
	var textDocs = make([]TextDocument, 0)
	for _, doc := range docs {
		switch v := doc.(type) {
		case string:
			textDocs = append(textDocs, *NewTextDocument(v))
		case []byte:
			textDocs = append(textDocs, *NewTextDocument(string(v)))
		default:
			return nil, errors.Errorf("invalid document type (string and bytes are supported): %T for %v", v, doc)
		}
	}
	return textDocs, nil
}

type DocumentMetadataImpl struct {
	metadata map[string]MetadataValue
}

func NewDocumentMetadata(attributes ...*MetaAttribute) DocumentMetadata {
	metadata := make(map[string]MetadataValue)
	for _, attribute := range attributes {
		if attribute == nil {
			continue
		}
		metadata[attribute.key] = attribute.value
	}
	return &DocumentMetadataImpl{metadata: metadata}
}

func NewDocumentMetadataFromMap(metadata map[string]interface{}) (DocumentMetadata, error) {
	if metadata == nil {
		return NewMetadata(), nil
	}

	mv := &DocumentMetadataImpl{metadata: make(map[string]MetadataValue)}
	for k, v := range metadata {
		switch val := v.(type) {
		case json.Number:
			numStr := string(val)
			if strings.Contains(numStr, ".") || strings.Contains(numStr, "e") || strings.Contains(numStr, "E") {
				// Has decimal point or scientific notation - treat as float
				if floatVal, err := val.Float64(); err == nil {
					mv.SetFloat(k, floatVal)
				} else {
					return nil, errors.Errorf("invalid float value for key %s: %v", k, val)
				}
			} else {
				// No decimal indicators - treat as integer
				if intVal, err := val.Int64(); err == nil {
					mv.SetInt(k, intVal)
				} else {
					return nil, errors.Errorf("invalid int value for key %s: %v", k, val)
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
			arrVal, err := convertInterfaceSliceToMetadataValue(val)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid array metadata for key %q", k)
			}
			mv.metadata[k] = arrVal
		default:
			return nil, errors.Errorf("invalid metadata value type: %T", v)
		}
	}
	return mv, nil
}

func (cm *DocumentMetadataImpl) Keys() []string {
	keys := make([]string, 0, len(cm.metadata))
	for k := range cm.metadata {
		keys = append(keys, k)
	}
	return keys
}

func (cm *DocumentMetadataImpl) GetRaw(key string) (value interface{}, ok bool) {
	v, ok := cm.metadata[key]
	return v, ok
}

func (cm *DocumentMetadataImpl) GetString(key string) (value string, ok bool) {
	v, ok := cm.metadata[key]
	if !ok {
		return "", false
	}
	str, ok := v.GetString()
	return str, ok
}

func (cm *DocumentMetadataImpl) GetInt(key string) (value int64, ok bool) {
	v, ok := cm.metadata[key]
	if !ok {
		return 0, false
	}
	i, ok := v.GetInt()
	return i, ok
}

func (cm *DocumentMetadataImpl) GetFloat(key string) (value float64, ok bool) {
	v, ok := cm.metadata[key]
	if !ok {
		return 0, false
	}
	f, ok := v.GetFloat()
	return f, ok
}

func (cm *DocumentMetadataImpl) GetBool(key string) (value bool, ok bool) {
	v, ok := cm.metadata[key]
	if !ok {
		return false, false
	}
	b, ok := v.GetBool()
	return b, ok
}

func (cm *DocumentMetadataImpl) GetStringArray(key string) ([]string, bool) {
	v, ok := cm.metadata[key]
	if !ok {
		return nil, false
	}
	return v.GetStringArray()
}

func (cm *DocumentMetadataImpl) GetIntArray(key string) ([]int64, bool) {
	v, ok := cm.metadata[key]
	if !ok {
		return nil, false
	}
	return v.GetIntArray()
}

func (cm *DocumentMetadataImpl) GetFloatArray(key string) ([]float64, bool) {
	v, ok := cm.metadata[key]
	if !ok {
		return nil, false
	}
	return v.GetFloatArray()
}

func (cm *DocumentMetadataImpl) GetBoolArray(key string) ([]bool, bool) {
	v, ok := cm.metadata[key]
	if !ok {
		return nil, false
	}
	return v.GetBoolArray()
}

func (cm *DocumentMetadataImpl) SetRaw(key string, value interface{}) {
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

func (cm *DocumentMetadataImpl) SetString(key, value string) {
	cm.metadata[key] = MetadataValue{StringValue: &value}
}

func (cm *DocumentMetadataImpl) SetInt(key string, value int64) {
	cm.metadata[key] = MetadataValue{Int: &value}
}

func (cm *DocumentMetadataImpl) SetFloat(key string, value float64) {
	cm.metadata[key] = MetadataValue{Float64: &value}
}

func (cm *DocumentMetadataImpl) SetBool(key string, value bool) {
	cm.metadata[key] = MetadataValue{Bool: &value}
}

func (cm *DocumentMetadataImpl) SetStringArray(key string, value []string) {
	if len(value) == 0 {
		return
	}
	cp := make([]string, len(value))
	copy(cp, value)
	cm.metadata[key] = MetadataValue{StringArray: cp}
}

func (cm *DocumentMetadataImpl) SetIntArray(key string, value []int64) {
	if len(value) == 0 {
		return
	}
	cp := make([]int64, len(value))
	copy(cp, value)
	cm.metadata[key] = MetadataValue{IntArray: cp}
}

func (cm *DocumentMetadataImpl) SetFloatArray(key string, value []float64) {
	if len(value) == 0 {
		return
	}
	cp := make([]float64, len(value))
	copy(cp, value)
	cm.metadata[key] = MetadataValue{FloatArray: cp}
}

func (cm *DocumentMetadataImpl) SetBoolArray(key string, value []bool) {
	if len(value) == 0 {
		return
	}
	cp := make([]bool, len(value))
	copy(cp, value)
	cm.metadata[key] = MetadataValue{BoolArray: cp}
}

func (cm *DocumentMetadataImpl) MarshalJSON() ([]byte, error) {
	processed := make(map[string]interface{})
	for k, v := range cm.metadata {
		switch val, _ := v.GetRaw(); val.(type) {
		case nil:
			processed[k] = nil
		case bool:
			processed[k], _ = v.GetBool()
		case float32, float64:
			floatVal, _ := v.GetFloat()
			processed[k] = json.Number(fmt.Sprintf("%.15f", floatVal))
		case int, int32, int64:
			processed[k], _ = v.GetInt()
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

func (cm *DocumentMetadataImpl) UnmarshalJSON(b []byte) error {
	err := json.Unmarshal(b, &cm.metadata)
	if err != nil {
		return err
	}
	if cm.metadata == nil {
		cm.metadata = make(map[string]MetadataValue)
	}
	return nil
}
