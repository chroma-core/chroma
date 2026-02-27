package chroma

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/chroma-core/chroma/clients/go/pkg/logger"
)

func TestTokenObfuscation(t *testing.T) {
	testCases := []struct {
		name     string
		token    string
		expected string
	}{
		{
			name:     "empty token",
			token:    "",
			expected: "",
		},
		{
			name:     "very short token (1 char)",
			token:    "a",
			expected: "a",
		},
		{
			name:     "short token (3 chars)",
			token:    "abc",
			expected: "a**",
		},
		{
			name:     "short token (4 chars)",
			token:    "abcd",
			expected: "a***",
		},
		{
			name:     "medium token (6 chars)",
			token:    "abcdef",
			expected: "ab...ef",
		},
		{
			name:     "medium token (8 chars)",
			token:    "abcdefgh",
			expected: "ab...gh",
		},
		{
			name:     "long token (16 chars)",
			token:    "abcdefghijklmnop",
			expected: "abcd...mnop",
		},
		{
			name:     "API key format",
			token:    "sk-1234567890abcdef",
			expected: "sk-1...cdef",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := _sanitizeToken(tc.token)
			assert.Equal(t, tc.expected, result)

			// Ensure sensitive data is not exposed
			if len(tc.token) > 8 {
				// For tokens longer than 8 chars, ensure middle part is hidden
				middleStart := 4
				middleEnd := len(tc.token) - 4
				if middleEnd > middleStart {
					assert.NotContains(t, result, tc.token[middleStart:middleEnd])
				}
			}
		})
	}
}

func TestRequestDumpObfuscation(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected []string // Parts that should be present
		hidden   []string // Parts that should NOT be present
	}{
		{
			name: "X-Chroma-Token with short token",
			input: `POST /api/v2/collections HTTP/1.1
Host: localhost:8000
X-Chroma-Token: abc

{"name":"test"}`,
			expected: []string{"X-Chroma-Token: a**"},
			hidden:   []string{"abc"},
		},
		{
			name: "Authorization Bearer with long token",
			input: `GET /api/v2/collections HTTP/1.1
Host: localhost:8000
Authorization: Bearer sk-verylongtokenhere1234567890

`,
			expected: []string{"Authorization: Bearer sk-v...7890"},
			hidden:   []string{"verylongtokenhere123456"},
		},
		{
			name: "Authorization Basic",
			input: `GET /api/v2/collections HTTP/1.1
Host: localhost:8000
Authorization: Basic dXNlcjpwYXNzd29yZA==

`,
			expected: []string{"Authorization: Basic dXNl...ZA=="},
			hidden:   []string{"cjpwYXNzd29"},
		},
		{
			name: "Multiple sensitive headers",
			input: `POST /api/v2/collections HTTP/1.1
Host: localhost:8000
X-Chroma-Token: mytoken123
Authorization: Bearer bearer-token-456

{"name":"test"}`,
			expected: []string{
				"X-Chroma-Token: myto...n123",
				"Authorization: Bearer bear...-456",
			},
			hidden: []string{"ken1", "er-token"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := _sanitizeRequestDump(tc.input)

			// Check expected obfuscated parts are present
			for _, exp := range tc.expected {
				assert.Contains(t, result, exp, "Expected obfuscated value not found")
			}

			// Check sensitive parts are hidden
			for _, hidden := range tc.hidden {
				assert.NotContains(t, result, hidden, "Sensitive data was not properly obfuscated")
			}
		})
	}
}

func TestResponseDumpSanitization(t *testing.T) {
	testCases := []struct {
		name   string
		input  string
		hidden []string // Sensitive values that should be redacted
	}{
		{
			name: "JSON response with API key",
			input: `HTTP/1.1 200 OK
Content-Type: application/json

{"api_key": "secret123", "user": "john"}`,
			hidden: []string{"secret123"},
		},
		{
			name: "JSON response with various sensitive fields",
			input: `HTTP/1.1 200 OK
Content-Type: application/json

{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
  "refresh_token": "refresh_token_value",
  "api_key": "sk-1234567890",
  "password": "userpassword",
  "secret": "topsecret",
  "data": "non-sensitive"
}`,
			hidden: []string{
				"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
				"refresh_token_value",
				"sk-1234567890",
				"userpassword",
				"topsecret",
			},
		},
		{
			name: "Response with auth header",
			input: `HTTP/1.1 200 OK
X-Chroma-Token: response-token-123
Content-Type: application/json

{"status": "ok"}`,
			hidden: []string{"sponse-token-1"}, // Middle part should be hidden
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := _sanitizeResponseDump(tc.input)

			// Check that sensitive values are hidden
			for _, hidden := range tc.hidden {
				assert.NotContains(t, result, hidden, "Sensitive data %q was not sanitized", hidden)
			}

			// Ensure REDACTED markers are present for JSON fields
			if strings.Contains(tc.input, `"api_key"`) {
				assert.Contains(t, result, "***REDACTED***")
			}
		})
	}
}

func TestTokenProviderString(t *testing.T) {
	// Test that String() method properly obfuscates tokens
	provider := &TokenAuthCredentialsProvider{
		Token:  "verysecrettoken",
		Header: XChromaTokenHeader,
	}

	str := provider.String()
	assert.Contains(t, str, "TokenAuthCredentialsProvider")
	assert.Contains(t, str, "very...oken")
	assert.NotContains(t, str, "secrett") // Middle part should be hidden
}

func TestBasicAuthObfuscation(t *testing.T) {
	result := _sanitizeBasicAuth("username", "password")
	assert.Equal(t, "username:****", result)
	assert.NotContains(t, result, "password")
}

func TestObfuscationEdgeCases(t *testing.T) {
	t.Run("nil context in extractContextFields", func(t *testing.T) {
		zapLogger, err := logger.NewDevelopmentZapLogger()
		require.NoError(t, err)
		require.NotPanics(t, func() {
			zapLogger.DebugWithContext(context.Background(), "test message")
		})
	})

	t.Run("malformed request dump", func(t *testing.T) {
		// Should handle gracefully without panicking
		malformed := "This is not a proper HTTP request"
		result := _sanitizeRequestDump(malformed)
		assert.Equal(t, malformed, result) // Should return unchanged if no patterns match
	})

	t.Run("empty request dump", func(t *testing.T) {
		result := _sanitizeRequestDump("")
		assert.Equal(t, "", result)
	})
}
