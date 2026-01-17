package chroma

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
)

type WhereDocumentFilterOperator string

const (
	ContainsOperator    WhereDocumentFilterOperator = "$contains"
	NotContainsOperator WhereDocumentFilterOperator = "$not_contains"
	RegexOperator       WhereDocumentFilterOperator = "$regex"
	NotRegexOperator    WhereDocumentFilterOperator = "$not_regex"
	OrDocumentOperator  WhereDocumentFilterOperator = "$or"
	AndDocumentOperator WhereDocumentFilterOperator = "$and"
)

type WhereDocumentFilter interface {
	Operator() WhereDocumentFilterOperator
	Operand() interface{}
	Validate() error
	String() string
	MarshalJSON() ([]byte, error)
	UnmarshalJSON(b []byte) error
}

type WhereDocumentFilterBase struct {
	operator WhereDocumentFilterOperator
	content  interface{}
}

func (w *WhereDocumentFilterBase) Operator() WhereDocumentFilterOperator {
	return w.operator
}

func (w *WhereDocumentFilterBase) Operand() interface{} {
	return w.content
}

func (w *WhereDocumentFilterBase) Validate() error {
	return nil
}

func (w *WhereDocumentFilterBase) String() string {
	return ""
}

func (w *WhereDocumentFilterBase) MarshalJSON() ([]byte, error) {
	return nil, errors.New("not implemented")
}

func (w *WhereDocumentFilterBase) UnmarshalJSON(b []byte) error {
	return errors.New("not implemented")
}

type WhereDocumentClauseContainsOrNotContains struct {
	WhereDocumentFilterBase
	content string
}

func (w *WhereDocumentClauseContainsOrNotContains) MarshalJSON() ([]byte, error) {
	err := w.Validate()
	if err != nil {
		return nil, err
	}
	var x = map[WhereDocumentFilterOperator]string{
		w.operator: w.content,
	}
	return json.Marshal(x)
}

func (w *WhereDocumentClauseContainsOrNotContains) UnmarshalJSON(b []byte) error {
	return json.Unmarshal(b, w)
}

func (w *WhereDocumentClauseContainsOrNotContains) Validate() error {
	if w.operator != ContainsOperator && w.operator != NotContainsOperator {
		return errors.New("invalid operator, expected in contains or not contains")
	}
	return nil
}

func (w *WhereDocumentClauseContainsOrNotContains) String() string {
	return fmt.Sprintf("%s: %s", w.operator, w.content)
}

type WhereDocumentClauseRegexNotRegex struct {
	WhereDocumentFilterBase
	content string
}

func (w *WhereDocumentClauseRegexNotRegex) MarshalJSON() ([]byte, error) {
	err := w.Validate()
	if err != nil {
		return nil, err
	}
	var x = map[WhereDocumentFilterOperator]string{
		w.operator: w.content,
	}
	return json.Marshal(x)
}

func (w *WhereDocumentClauseRegexNotRegex) UnmarshalJSON(b []byte) error {
	return json.Unmarshal(b, w)
}

func (w *WhereDocumentClauseRegexNotRegex) Validate() error {
	if w.operator != RegexOperator && w.operator != NotRegexOperator {
		return errors.New("invalid operator, expected in regex or not regex")
	}
	return nil
}

func (w *WhereDocumentClauseRegexNotRegex) String() string {
	return fmt.Sprintf("%s: %s", w.operator, w.content)
}

type WhereDocumentClauseOr struct {
	WhereDocumentFilterBase
	content []WhereDocumentFilter
}

func (w *WhereDocumentClauseOr) MarshalJSON() ([]byte, error) {
	err := w.Validate()
	if err != nil {
		return nil, err
	}
	var x = map[WhereDocumentFilterOperator][]WhereDocumentFilter{
		w.operator: w.content,
	}
	return json.Marshal(x)
}

func (w *WhereDocumentClauseOr) UnmarshalJSON(b []byte) error {
	return json.Unmarshal(b, w)
}

func (w *WhereDocumentClauseOr) Validate() error {
	if w.operator != OrDocumentOperator {
		return errors.New("invalid operator, expected in or")
	}
	if len(w.content) == 0 {
		return errors.New("invalid content, expected at least one")
	}
	for _, v := range w.content {
		if err := v.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (w *WhereDocumentClauseOr) String() string {
	return fmt.Sprintf("%s: %s", w.operator, w.content)
}

type WhereDocumentClauseAnd struct {
	WhereDocumentFilterBase
	content []WhereDocumentFilter
}

func (w *WhereDocumentClauseAnd) MarshalJSON() ([]byte, error) {
	err := w.Validate()
	if err != nil {
		return nil, err
	}
	var x = map[WhereDocumentFilterOperator][]WhereDocumentFilter{
		w.operator: w.content,
	}
	return json.Marshal(x)
}

func (w *WhereDocumentClauseAnd) UnmarshalJSON(b []byte) error {
	return errors.New("not implemented")
}

func (w *WhereDocumentClauseAnd) Validate() error {
	if w.operator != AndDocumentOperator {
		return errors.New("invalid operator, expected in and")
	}
	if len(w.content) == 0 {
		return errors.New("invalid content, expected at least one")
	}
	for _, v := range w.content {
		if err := v.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (w *WhereDocumentClauseAnd) String() string {
	return fmt.Sprintf("%s: %s", w.operator, w.content)
}

func Contains(content string) WhereDocumentFilter {
	return &WhereDocumentClauseContainsOrNotContains{
		WhereDocumentFilterBase: WhereDocumentFilterBase{
			operator: ContainsOperator,
		},
		content: content,
	}
}

func NotContains(content string) WhereDocumentFilter {
	return &WhereDocumentClauseContainsOrNotContains{
		WhereDocumentFilterBase: WhereDocumentFilterBase{
			operator: NotContainsOperator,
		},
		content: content,
	}
}

func Regex(content string) WhereDocumentFilter {
	return &WhereDocumentClauseRegexNotRegex{
		WhereDocumentFilterBase: WhereDocumentFilterBase{
			operator: RegexOperator,
		},
		content: content,
	}
}

func NotRegex(content string) WhereDocumentFilter {
	return &WhereDocumentClauseRegexNotRegex{
		WhereDocumentFilterBase: WhereDocumentFilterBase{
			operator: NotRegexOperator,
		},
		content: content,
	}
}

func OrDocument(clauses ...WhereDocumentFilter) WhereDocumentFilter {
	return &WhereDocumentClauseOr{
		WhereDocumentFilterBase: WhereDocumentFilterBase{
			operator: OrDocumentOperator,
		},
		content: clauses,
	}
}

func AndDocument(clauses ...WhereDocumentFilter) WhereDocumentFilter {
	return &WhereDocumentClauseAnd{
		WhereDocumentFilterBase: WhereDocumentFilterBase{
			operator: AndDocumentOperator,
		},
		content: clauses,
	}
}
