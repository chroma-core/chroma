package chroma

import (
	"encoding/json"

	"github.com/pkg/errors"
)

type WhereFilterOperator string

const (
	EqualOperator              WhereFilterOperator = "$eq"
	NotEqualOperator           WhereFilterOperator = "$ne"
	GreaterThanOperator        WhereFilterOperator = "$gt"
	GreaterThanOrEqualOperator WhereFilterOperator = "$gte"
	LessThanOperator           WhereFilterOperator = "$lt"
	LessThanOrEqualOperator    WhereFilterOperator = "$lte"
	InOperator                 WhereFilterOperator = "$in"
	NotInOperator              WhereFilterOperator = "$nin"
	AndOperator                WhereFilterOperator = "$and"
	OrOperator                 WhereFilterOperator = "$or"
	ContainsWhereOperator      WhereFilterOperator = "$contains"
	NotContainsWhereOperator   WhereFilterOperator = "$not_contains"
)

type WhereClause interface {
	Operator() WhereFilterOperator
	Key() string
	Operand() interface{}
	String() string
	Validate() error
	MarshalJSON() ([]byte, error)
	UnmarshalJSON(b []byte) error
}

type WhereClauseBase struct {
	operator WhereFilterOperator
	key      string
}

func (w *WhereClauseBase) Operator() WhereFilterOperator {
	return w.operator
}

func (w *WhereClauseBase) Key() string {
	return w.key
}

func (w *WhereClauseBase) String() string {
	return ""
}

// StringValue

type WhereClauseString struct {
	WhereClauseBase
	operand string
}

func (w *WhereClauseString) Operand() interface{} {
	return w.operand
}

func (w *WhereClauseString) Validate() error {
	if w.key == "" {
		return errors.Errorf("invalid key for %s, expected non-empty", w.operator)
	}
	switch w.operator {
	case EqualOperator, NotEqualOperator, ContainsWhereOperator, NotContainsWhereOperator:
		// Valid operators for string
	default:
		return errors.Errorf("invalid operator %s for string clause", w.operator)
	}
	// $contains and $not_contains require non-empty operand
	if (w.operator == ContainsWhereOperator || w.operator == NotContainsWhereOperator) && w.operand == "" {
		return errors.Errorf("invalid operand for %s on key %q, expected non-empty string", w.operator, w.key)
	}
	return nil
}

func (w *WhereClauseString) MarshalJSON() ([]byte, error) {
	var x = map[string]map[WhereFilterOperator]string{
		w.key: {
			w.operator: w.operand,
		},
	}
	return json.Marshal(x)
}

func (w *WhereClauseString) UnmarshalJSON(b []byte) error {
	var x = map[string]map[WhereFilterOperator]string{}
	err := json.Unmarshal(b, &x)
	if err != nil {
		return err
	}

	for key, value := range x {
		w.key = key
		for operator, operand := range value {
			w.operator = operator
			w.operand = operand
		}
	}
	return nil
}

type WhereClauseStrings struct {
	WhereClauseBase
	operand []string
}

func (w *WhereClauseStrings) Operand() interface{} {
	return w.operand
}

func (w *WhereClauseStrings) Validate() error {
	if w.key == "" {
		return errors.Errorf("invalid key for %s, expected non-empty", w.operator)
	}
	if w.operator != InOperator && w.operator != NotInOperator {
		return errors.New("invalid operator, expected in or nin")
	}
	if len(w.operand) == 0 {
		return errors.Errorf("invalid operand for %s on key %q, expected at least one value", w.operator, w.key)
	}
	return nil
}

func (w *WhereClauseStrings) MarshalJSON() ([]byte, error) {
	var x = map[string]map[WhereFilterOperator][]string{
		w.key: {
			w.operator: w.operand,
		},
	}
	return json.Marshal(x)
}

func (w *WhereClauseStrings) UnmarshalJSON(b []byte) error {
	var x = map[string]map[WhereFilterOperator][]string{}
	err := json.Unmarshal(b, &x)
	if err != nil {
		return err
	}

	for key, value := range x {
		w.key = key
		for operator, operand := range value {
			w.operator = operator
			w.operand = operand
		}
	}
	return nil
}

// Int

type WhereClauseInt struct {
	WhereClauseBase
	operand int
}

func (w *WhereClauseInt) Operand() interface{} {
	return w.operand
}

func (w *WhereClauseInt) Validate() error {
	if w.key == "" {
		return errors.Errorf("invalid key for %s, expected non-empty", w.operator)
	}
	switch w.operator {
	case EqualOperator, NotEqualOperator, GreaterThanOperator, GreaterThanOrEqualOperator, LessThanOperator, LessThanOrEqualOperator,
		ContainsWhereOperator, NotContainsWhereOperator:
		// Valid operators for int (includes $contains/$not_contains for array metadata)
	default:
		return errors.Errorf("invalid operator %s for int clause", w.operator)
	}
	return nil
}

func (w *WhereClauseInt) MarshalJSON() ([]byte, error) {
	var x = map[string]map[WhereFilterOperator]int{
		w.key: {
			w.operator: w.operand,
		},
	}
	return json.Marshal(x)
}

func (w *WhereClauseInt) UnmarshalJSON(b []byte) error {
	var x = map[string]map[WhereFilterOperator]int{}
	err := json.Unmarshal(b, &x)
	if err != nil {
		return err
	}

	for key, value := range x {
		w.key = key
		for operator, operand := range value {
			w.operator = operator
			w.operand = operand
		}
	}
	return nil
}

type WhereClauseInts struct {
	WhereClauseBase
	operand []int
}

func (w *WhereClauseInts) Operand() interface{} {
	return w.operand
}

func (w *WhereClauseInts) Validate() error {
	if w.key == "" {
		return errors.Errorf("invalid key for %s, expected non-empty", w.operator)
	}
	if w.operator != InOperator && w.operator != NotInOperator {
		return errors.New("invalid operator, expected in or nin")
	}
	if len(w.operand) == 0 {
		return errors.Errorf("invalid operand for %s on key %q, expected at least one value", w.operator, w.key)
	}
	return nil
}

func (w *WhereClauseInts) MarshalJSON() ([]byte, error) {
	var x = map[string]map[WhereFilterOperator][]int{
		w.key: {
			w.operator: w.operand,
		},
	}
	return json.Marshal(x)
}

func (w *WhereClauseInts) UnmarshalJSON(b []byte) error {
	var x = map[string]map[WhereFilterOperator][]int{}
	err := json.Unmarshal(b, &x)
	if err != nil {
		return err
	}

	for key, value := range x {
		w.key = key
		for operator, operand := range value {
			w.operator = operator
			w.operand = operand
		}
	}
	return nil
}

// Float

type WhereClauseFloat struct {
	WhereClauseBase
	operand float32
}

func (w *WhereClauseFloat) Operand() interface{} {
	return w.operand
}

func (w *WhereClauseFloat) Validate() error {
	if w.key == "" {
		return errors.Errorf("invalid key for %s, expected non-empty", w.operator)
	}
	switch w.operator {
	case EqualOperator, NotEqualOperator, GreaterThanOperator, GreaterThanOrEqualOperator, LessThanOperator, LessThanOrEqualOperator,
		ContainsWhereOperator, NotContainsWhereOperator:
		// Valid operators for float (includes $contains/$not_contains for array metadata)
	default:
		return errors.Errorf("invalid operator %s for float clause", w.operator)
	}
	return nil
}

func (w *WhereClauseFloat) MarshalJSON() ([]byte, error) {
	var x = map[string]map[WhereFilterOperator]float32{
		w.key: {
			w.operator: w.operand,
		},
	}
	return json.Marshal(x)
}

func (w *WhereClauseFloat) UnmarshalJSON(b []byte) error {
	var x = map[string]map[WhereFilterOperator]float32{}
	err := json.Unmarshal(b, &x)
	if err != nil {
		return err
	}

	for key, value := range x {
		w.key = key
		for operator, operand := range value {
			w.operator = operator
			w.operand = operand
		}
	}
	return nil
}

type WhereClauseFloats struct {
	WhereClauseBase
	operand []float32
}

func (w *WhereClauseFloats) Operand() interface{} {
	return w.operand
}

func (w *WhereClauseFloats) Validate() error {
	if w.key == "" {
		return errors.Errorf("invalid key for %s, expected non-empty", w.operator)
	}
	if w.operator != InOperator && w.operator != NotInOperator {
		return errors.New("invalid operator, expected in or nin")
	}
	if len(w.operand) == 0 {
		return errors.Errorf("invalid operand for %s on key %q, expected at least one value", w.operator, w.key)
	}
	return nil
}

func (w *WhereClauseFloats) MarshalJSON() ([]byte, error) {
	var x = map[string]map[WhereFilterOperator][]float32{
		w.key: {
			w.operator: w.operand,
		},
	}
	return json.Marshal(x)
}

func (w *WhereClauseFloats) UnmarshalJSON(b []byte) error {
	var x = map[string]map[WhereFilterOperator][]float32{}
	err := json.Unmarshal(b, &x)
	if err != nil {
		return err
	}

	for key, value := range x {
		w.key = key
		for operator, operand := range value {
			w.operator = operator
			w.operand = operand
		}
	}
	return nil
}

// Bool

type WhereClauseBool struct {
	WhereClauseBase
	operand bool
}

func (w *WhereClauseBool) Operand() interface{} {
	return w.operand
}

func (w *WhereClauseBool) Validate() error {
	if w.key == "" {
		return errors.Errorf("invalid key for %s, expected non-empty", w.operator)
	}
	switch w.operator {
	case EqualOperator, NotEqualOperator, ContainsWhereOperator, NotContainsWhereOperator:
		// Valid operators for bool (includes $contains/$not_contains for array metadata)
	default:
		return errors.Errorf("invalid operator %s for bool clause", w.operator)
	}
	return nil
}

func (w *WhereClauseBool) MarshalJSON() ([]byte, error) {
	var x = map[string]map[WhereFilterOperator]bool{
		w.key: {
			w.operator: w.operand,
		},
	}
	return json.Marshal(x)
}

func (w *WhereClauseBool) UnmarshalJSON(b []byte) error {
	var x = map[string]map[WhereFilterOperator]bool{}
	err := json.Unmarshal(b, &x)
	if err != nil {
		return err
	}

	for key, value := range x {
		w.key = key
		for operator, operand := range value {
			w.operator = operator
			w.operand = operand
		}
	}
	return nil
}

type WhereClauseBools struct {
	WhereClauseBase
	operand []bool
}

func (w *WhereClauseBools) Operand() interface{} {
	return w.operand
}

func (w *WhereClauseBools) Validate() error {
	if w.key == "" {
		return errors.Errorf("invalid key for %s, expected non-empty", w.operator)
	}
	if w.operator != InOperator && w.operator != NotInOperator {
		return errors.New("invalid operator, expected in or nin")
	}
	if len(w.operand) == 0 {
		return errors.Errorf("invalid operand for %s on key %q, expected at least one value", w.operator, w.key)
	}
	return nil
}

func (w *WhereClauseBools) MarshalJSON() ([]byte, error) {
	var x = map[string]map[WhereFilterOperator][]bool{
		w.key: {
			w.operator: w.operand,
		},
	}
	return json.Marshal(x)
}

func (w *WhereClauseBools) UnmarshalJSON(b []byte) error {
	var x = map[string]map[WhereFilterOperator][]bool{}
	err := json.Unmarshal(b, &x)
	if err != nil {
		return err
	}

	for key, value := range x {
		w.key = key
		for operator, operand := range value {
			w.operator = operator
			w.operand = operand
		}
	}
	return nil
}

type WhereClauseWhereClauses struct {
	WhereClauseBase
	operand []WhereClause
}

func (w *WhereClauseWhereClauses) Operand() interface{} {
	return w.operand
}

func (w *WhereClauseWhereClauses) Validate() error {
	if w.operator != OrOperator && w.operator != AndOperator {
		return errors.New("invalid operator, expected $and or $or")
	}
	if len(w.operand) == 0 {
		return errors.Errorf("invalid operand for %s, expected at least one clause", w.operator)
	}
	for _, clause := range w.operand {
		if clause == nil {
			return errors.Errorf("nil clause in %s expression", w.operator)
		}
		if err := clause.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (w *WhereClauseWhereClauses) MarshalJSON() ([]byte, error) {
	var x = map[WhereFilterOperator][]WhereClause{
		w.operator: w.operand,
	}
	return json.Marshal(x)
}

func (w *WhereClauseWhereClauses) UnmarshalJSON(b []byte) error {
	var x = map[WhereFilterOperator][]WhereClause{}
	err := json.Unmarshal(b, &x)
	if err != nil {
		return err
	}

	for operator, clauses := range x {
		w.operator = operator
		w.operand = clauses
	}
	return nil
}

type WhereFilter interface {
	String() string
	Validate() error
	MarshalJSON() ([]byte, error)
	UnmarshalJSON(b []byte) error
}

func EqString(field Key, value string) WhereClause {
	return &WhereClauseString{
		WhereClauseBase: WhereClauseBase{
			operator: EqualOperator,
			key:      field,
		},
		operand: value,
	}
}

func EqInt(field Key, value int) WhereClause {
	return &WhereClauseInt{
		WhereClauseBase: WhereClauseBase{
			operator: EqualOperator,
			key:      field,
		},
		operand: value,
	}
}
func EqFloat(field Key, value float32) WhereClause {
	return &WhereClauseFloat{
		WhereClauseBase: WhereClauseBase{
			operator: EqualOperator,
			key:      field,
		},
		operand: value,
	}
}
func EqBool(field Key, value bool) WhereClause {
	return &WhereClauseBool{
		WhereClauseBase: WhereClauseBase{
			operator: EqualOperator,
			key:      field,
		},
		operand: value,
	}
}

func NotEqString(field Key, value string) WhereClause {
	return &WhereClauseString{
		WhereClauseBase: WhereClauseBase{
			operator: NotEqualOperator,
			key:      field,
		},
		operand: value,
	}
}
func NotEqInt(field Key, value int) WhereClause {
	return &WhereClauseInt{
		WhereClauseBase: WhereClauseBase{
			operator: NotEqualOperator,
			key:      field,
		},
		operand: value,
	}
}
func NotEqFloat(field Key, value float32) WhereClause {
	return &WhereClauseFloat{
		WhereClauseBase: WhereClauseBase{
			operator: NotEqualOperator,
			key:      field,
		},
		operand: value,
	}
}
func NotEqBool(field Key, value bool) WhereClause {
	return &WhereClauseBool{
		WhereClauseBase: WhereClauseBase{
			operator: NotEqualOperator,
			key:      field,
		},
		operand: value,
	}
}
func GtInt(field Key, value int) WhereClause {
	return &WhereClauseInt{
		WhereClauseBase: WhereClauseBase{
			operator: GreaterThanOperator,
			key:      field,
		},
		operand: value,
	}
}
func GtFloat(field Key, value float32) WhereClause {
	return &WhereClauseFloat{
		WhereClauseBase: WhereClauseBase{
			operator: GreaterThanOperator,
			key:      field,
		},
		operand: value,
	}
}
func LtInt(field Key, value int) WhereClause {
	return &WhereClauseInt{
		WhereClauseBase: WhereClauseBase{
			operator: LessThanOperator,
			key:      field,
		},
		operand: value,
	}
}
func LtFloat(field Key, value float32) WhereClause {
	return &WhereClauseFloat{
		WhereClauseBase: WhereClauseBase{
			operator: LessThanOperator,
			key:      field,
		},
		operand: value,
	}
}
func GteInt(field Key, value int) WhereClause {
	return &WhereClauseInt{
		WhereClauseBase: WhereClauseBase{
			operator: GreaterThanOrEqualOperator,
			key:      field,
		},
		operand: value,
	}
}
func GteFloat(field Key, value float32) WhereClause {
	return &WhereClauseFloat{
		WhereClauseBase: WhereClauseBase{
			operator: GreaterThanOrEqualOperator,
			key:      field,
		},
		operand: value,
	}
}
func LteInt(field Key, value int) WhereClause {
	return &WhereClauseInt{
		WhereClauseBase: WhereClauseBase{
			operator: LessThanOrEqualOperator,
			key:      field,
		},
		operand: value,
	}
}
func LteFloat(field Key, value float32) WhereClause {
	return &WhereClauseFloat{
		WhereClauseBase: WhereClauseBase{
			operator: LessThanOrEqualOperator,
			key:      field,
		},
		operand: value,
	}
}
func InString(field Key, values ...string) WhereClause {
	return &WhereClauseStrings{
		WhereClauseBase: WhereClauseBase{
			operator: InOperator,
			key:      field,
		},
		operand: values,
	}
}
func InInt(field Key, values ...int) WhereClause {
	return &WhereClauseInts{
		WhereClauseBase: WhereClauseBase{
			operator: InOperator,
			key:      field,
		},
		operand: values,
	}
}
func InFloat(field Key, values ...float32) WhereClause {
	return &WhereClauseFloats{
		WhereClauseBase: WhereClauseBase{
			operator: InOperator,
			key:      field,
		},
		operand: values,
	}
}
func InBool(field Key, values ...bool) WhereClause {
	return &WhereClauseBools{
		WhereClauseBase: WhereClauseBase{
			operator: InOperator,
			key:      field,
		},
		operand: values,
	}
}
func NinString(field Key, values ...string) WhereClause {
	return &WhereClauseStrings{
		WhereClauseBase: WhereClauseBase{
			operator: NotInOperator,
			key:      field,
		},
		operand: values,
	}
}
func NinInt(field Key, values ...int) WhereClause {
	return &WhereClauseInts{
		WhereClauseBase: WhereClauseBase{
			operator: NotInOperator,
			key:      field,
		},
		operand: values,
	}
}
func NinFloat(field Key, values ...float32) WhereClause {
	return &WhereClauseFloats{
		WhereClauseBase: WhereClauseBase{
			operator: NotInOperator,
			key:      field,
		},
		operand: values,
	}
}
func NinBool(field Key, values ...bool) WhereClause {
	return &WhereClauseBools{
		WhereClauseBase: WhereClauseBase{
			operator: NotInOperator,
			key:      field,
		},
		operand: values,
	}
}
func Or(clauses ...WhereClause) WhereClause {
	return &WhereClauseWhereClauses{
		WhereClauseBase: WhereClauseBase{
			operator: OrOperator,
		},
		operand: clauses,
	}
}
func And(clauses ...WhereClause) WhereClause {
	return &WhereClauseWhereClauses{
		WhereClauseBase: WhereClauseBase{
			operator: AndOperator,
		},
		operand: clauses,
	}
}

// IDIn creates a where clause that matches documents with any of the specified IDs.
// Use this in combination with other where clauses via And() or Or().
//
// Example:
//
//	WithFilter(And(EqString(K("status"), "published"), IDIn("doc1", "doc2", "doc3")))
func IDIn(ids ...DocumentID) WhereClause {
	strIDs := make([]string, len(ids))
	for i, id := range ids {
		strIDs[i] = string(id)
	}
	return InString(KID, strIDs...)
}

// IDNotIn creates a where clause that excludes documents with any of the specified IDs.
// Use this to filter out already-seen or unwanted documents from search results.
//
// Example:
//
//	WithFilter(IDNotIn("seen1", "seen2", "seen3"))
func IDNotIn(ids ...DocumentID) WhereClause {
	strIDs := make([]string, len(ids))
	for i, id := range ids {
		strIDs[i] = string(id)
	}
	return NinString(KID, strIDs...)
}

// DocumentContains creates a where clause that filters documents containing the specified text.
// Use this with Search API to filter by document content.
//
// Example:
//
//	WithFilter(DocumentContains("machine learning"))
func DocumentContains(text string) WhereClause {
	return &WhereClauseString{
		WhereClauseBase: WhereClauseBase{
			operator: ContainsWhereOperator,
			key:      KDocument,
		},
		operand: text,
	}
}

// DocumentNotContains creates a where clause that filters out documents containing the specified text.
// Use this with Search API to exclude documents with certain content.
//
// Example:
//
//	WithFilter(DocumentNotContains("deprecated"))
func DocumentNotContains(text string) WhereClause {
	return &WhereClauseString{
		WhereClauseBase: WhereClauseBase{
			operator: NotContainsWhereOperator,
			key:      KDocument,
		},
		operand: text,
	}
}

func MetadataContainsString(field Key, value string) WhereClause {
	return &WhereClauseString{
		WhereClauseBase: WhereClauseBase{
			operator: ContainsWhereOperator,
			key:      field,
		},
		operand: value,
	}
}

func MetadataNotContainsString(field Key, value string) WhereClause {
	return &WhereClauseString{
		WhereClauseBase: WhereClauseBase{
			operator: NotContainsWhereOperator,
			key:      field,
		},
		operand: value,
	}
}

func MetadataContainsInt(field Key, value int) WhereClause {
	return &WhereClauseInt{
		WhereClauseBase: WhereClauseBase{
			operator: ContainsWhereOperator,
			key:      field,
		},
		operand: value,
	}
}

func MetadataNotContainsInt(field Key, value int) WhereClause {
	return &WhereClauseInt{
		WhereClauseBase: WhereClauseBase{
			operator: NotContainsWhereOperator,
			key:      field,
		},
		operand: value,
	}
}

func MetadataContainsFloat(field Key, value float32) WhereClause {
	return &WhereClauseFloat{
		WhereClauseBase: WhereClauseBase{
			operator: ContainsWhereOperator,
			key:      field,
		},
		operand: value,
	}
}

func MetadataNotContainsFloat(field Key, value float32) WhereClause {
	return &WhereClauseFloat{
		WhereClauseBase: WhereClauseBase{
			operator: NotContainsWhereOperator,
			key:      field,
		},
		operand: value,
	}
}

func MetadataContainsBool(field Key, value bool) WhereClause {
	return &WhereClauseBool{
		WhereClauseBase: WhereClauseBase{
			operator: ContainsWhereOperator,
			key:      field,
		},
		operand: value,
	}
}

func MetadataNotContainsBool(field Key, value bool) WhereClause {
	return &WhereClauseBool{
		WhereClauseBase: WhereClauseBase{
			operator: NotContainsWhereOperator,
			key:      field,
		},
		operand: value,
	}
}
