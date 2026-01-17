//go:build ef || rf

package cohere

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidations(t *testing.T) {
	tests := []struct {
		name          string
		options       []Option
		expectedError string
	}{
		{
			name: "Test empty API key",
			options: []Option{
				WithDefaultModel("model"),
			},
			expectedError: "'APIKey' failed on the 'required'",
		},
		{
			name: "Test without default model",
			options: []Option{
				WithAPIKey("dummy"),
			},
			expectedError: "'DefaultModel' failed on the 'required'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewCohereClient(tt.options...)
			fmt.Printf("Error: %v\n", err)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.expectedError)
		})
	}
}
