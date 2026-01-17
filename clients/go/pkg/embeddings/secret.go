package embeddings

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"

	"github.com/go-playground/validator/v10"
	"github.com/pkg/errors"
)

const redactedText = "****"

// Secret is a string type that prevents accidental logging of sensitive values.
// It implements fmt.Stringer, fmt.GoStringer, fmt.Formatter, json.Marshaler,
// and slog.LogValuer to mask the value in all common output scenarios.
type Secret struct {
	value string
}

// NewSecret creates a new Secret from a string value.
func NewSecret(value string) Secret {
	return Secret{value: value}
}

// Value returns the actual secret value. Use this only when the value is needed
// (e.g., for HTTP headers). Never log the result of this method.
func (s Secret) Value() string {
	return s.value
}

// String implements fmt.Stringer, returning a redacted placeholder.
func (s Secret) String() string {
	return redactedText
}

// GoString implements fmt.GoStringer for %#v formatting.
func (s Secret) GoString() string {
	return "Secret(" + redactedText + ")"
}

// Format implements fmt.Formatter for complete control over all format verbs.
func (s Secret) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte(redactedText))
}

// MarshalJSON implements json.Marshaler, returning a redacted placeholder.
func (s Secret) MarshalJSON() ([]byte, error) {
	return json.Marshal(redactedText)
}

// UnmarshalJSON implements json.Unmarshaler.
// It returns an error to enforce the use of environment variables for secrets.
// Secrets should be passed via api_key_env_var configuration, not directly in JSON.
func (s *Secret) UnmarshalJSON(_ []byte) error {
	return errors.New("secrets cannot be deserialized from JSON; use environment variables via api_key_env_var config")
}

// LogValue implements slog.LogValuer for structured logging protection.
func (s Secret) LogValue() slog.Value {
	return slog.StringValue(redactedText)
}

// IsEmpty returns true if the secret value is empty.
func (s Secret) IsEmpty() bool {
	return s.value == ""
}

// NewValidator creates a validator configured to properly validate Secret fields.
// The validator extracts the underlying string value so `validate:"required"` works correctly.
func NewValidator() *validator.Validate {
	v := validator.New(validator.WithRequiredStructEnabled())
	v.RegisterCustomTypeFunc(func(field reflect.Value) any {
		if secret, ok := field.Interface().(Secret); ok {
			return secret.value
		}
		return nil
	}, Secret{})
	return v
}
