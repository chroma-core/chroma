package chroma

import (
	"encoding/base64"
	"fmt"
	"log"
	"regexp"
	"strings"
	"unicode"

	"github.com/pkg/errors"
)

// Package-level compiled regex patterns for better performance and safety
var (
	// Header patterns - case insensitive for better coverage
	reChromaToken *regexp.Regexp
	reBearerToken *regexp.Regexp
	reBasicAuth   *regexp.Regexp

	// JSON field patterns for response sanitization
	reJSONPatterns []*regexp.Regexp

	// Pre-compiled fallback pattern that matches nothing - guaranteed to never panic
	// This is used as a last resort if all regex compilation attempts fail
	// Using an impossible pattern: start of line followed by end of line with required content
	reFallback = regexp.MustCompile(`^\b$`) // word boundary between start and end - never matches
)

func init() {
	var err error

	// Using regexp.Compile instead of MustCompile to avoid panics
	// These patterns are critical for security - if they fail to compile,
	// we create simple fallback patterns that provide basic functionality

	// Compile header patterns with case-insensitive flag
	// nolint:gocritic // Using Compile instead of MustCompile to avoid panics per project guidelines
	reChromaToken, err = regexp.Compile(`(?im)^X-Chroma-Token:\s*(.+)$`)
	if err != nil {
		// Fallback to a simpler pattern that should always compile
		// nolint:gocritic // Intentionally using Compile for safety
		if reChromaToken, err = regexp.Compile(`X-Chroma-Token:\s*(.+)`); err != nil {
			// Last resort: use pre-compiled fallback that never matches
			reChromaToken = reFallback
			log.Printf("Warning: Failed to compile X-Chroma-Token regex, using fallback")
		}
	}

	// nolint:gocritic // Using Compile instead of MustCompile to avoid panics per project guidelines
	reBearerToken, err = regexp.Compile(`(?im)^Authorization:\s*Bearer\s+(.+)$`)
	if err != nil {
		// Fallback to a simpler pattern
		// nolint:gocritic // Intentionally using Compile for safety
		if reBearerToken, err = regexp.Compile(`Authorization:\s*Bearer\s+(.+)`); err != nil {
			// Use pre-compiled fallback
			reBearerToken = reFallback
			log.Printf("Warning: Failed to compile Bearer token regex, using fallback")
		}
	}

	// nolint:gocritic // Using Compile instead of MustCompile to avoid panics per project guidelines
	reBasicAuth, err = regexp.Compile(`(?im)^Authorization:\s*Basic\s+(.+)$`)
	if err != nil {
		// Fallback to a simpler pattern
		// nolint:gocritic // Intentionally using Compile for safety
		if reBasicAuth, err = regexp.Compile(`Authorization:\s*Basic\s+(.+)`); err != nil {
			// Use pre-compiled fallback
			reBasicAuth = reFallback
			log.Printf("Warning: Failed to compile Basic auth regex, using fallback")
		}
	}

	// Compile JSON patterns - simplified to avoid ReDoS with bounded quantifiers
	jsonPatterns := []string{
		`"(api_key|apiKey|api_token|apiToken|secret|password|token|auth|credential)":\s{0,10}"[^"]{1,1000}"`,
		`"(access_token|accessToken|refresh_token|refreshToken|id_token|idToken)":\s{0,10}"[^"]{1,1000}"`,
		`"(private_key|privateKey|secret_key|secretKey)":\s{0,10}"[^"]{1,1000}"`,
		`"(authorization|Authorization)":\s{0,10}"[^"]{1,1000}"`,
	}

	reJSONPatterns = make([]*regexp.Regexp, 0, len(jsonPatterns))
	for _, pattern := range jsonPatterns {
		re, err := regexp.Compile(pattern)
		if err != nil {
			// Try a simpler fallback pattern with bounded quantifiers
			simplePattern := `"(\w{1,100})":\s{0,10}"[^"]{1,1000}"`
			if fallback, err := regexp.Compile(simplePattern); err == nil {
				reJSONPatterns = append(reJSONPatterns, fallback)
			}
		} else {
			reJSONPatterns = append(reJSONPatterns, re)
		}
	}

	// If we have no JSON patterns at all, add a basic one as last resort
	if len(reJSONPatterns) == 0 {
		// nolint:gocritic // Using Compile instead of MustCompile to avoid panics
		if basicPattern, err := regexp.Compile(`"\w{1,100}":\s{0,10}"[^"]{1,1000}"`); err == nil {
			reJSONPatterns = append(reJSONPatterns, basicPattern)
		}
	}
}

type CredentialsProvider interface {
	Authenticate(apiClient *BaseAPIClient) error
}

type BasicAuthCredentialsProvider struct {
	Username string
	Password string
}

func NewBasicAuthCredentialsProvider(username, password string) *BasicAuthCredentialsProvider {
	return &BasicAuthCredentialsProvider{
		Username: username,
		Password: password,
	}
}

func (b *BasicAuthCredentialsProvider) Authenticate(client *BaseAPIClient) error {
	auth := b.Username + ":" + b.Password
	encodedAuth := base64.StdEncoding.EncodeToString([]byte(auth))
	client.defaultHeaders["Authorization"] = "Basic " + encodedAuth
	return nil
}

func (b *BasicAuthCredentialsProvider) String() string {
	return "BasicAuthCredentialsProvider {" + _sanitizeBasicAuth(b.Username, b.Password) + "}"
}

type TokenTransportHeader string

const (
	AuthorizationTokenHeader TokenTransportHeader = "Authorization"
	XChromaTokenHeader       TokenTransportHeader = "X-Chroma-Token"
)

type TokenAuthCredentialsProvider struct {
	Token  string
	Header TokenTransportHeader
}

func NewTokenAuthCredentialsProvider(token string, header TokenTransportHeader) *TokenAuthCredentialsProvider {
	return &TokenAuthCredentialsProvider{
		Token:  token,
		Header: header,
	}
}

func (t *TokenAuthCredentialsProvider) Authenticate(client *BaseAPIClient) error {
	switch t.Header {
	case AuthorizationTokenHeader:
		client.defaultHeaders[string(t.Header)] = "Bearer " + t.Token
		return nil
	case XChromaTokenHeader:
		client.defaultHeaders[string(t.Header)] = t.Token
		return nil
	default:
		return errors.Errorf("unsupported token header: %v", t.Header)
	}
}

func (t *TokenAuthCredentialsProvider) String() string {
	return "TokenAuthCredentialsProvider {" + string(t.Header) + ": " + _sanitizeToken(t.Token) + "}"
}

func _sanitizeBasicAuth(username, _ string) string {
	// This is a placeholder for any obfuscation logic you might want to implement.
	// For now, it just returns the username and password as is.
	return username + ":****"
}

// sanitizeForLogging escapes control characters to prevent log injection attacks
func sanitizeForLogging(input string) string {
	if input == "" {
		return ""
	}

	// Replace control characters with their escaped representations
	var result strings.Builder
	result.Grow(len(input))

	for _, r := range input {
		switch r {
		case '\n':
			result.WriteString("\\n")
		case '\r':
			result.WriteString("\\r")
		case '\t':
			result.WriteString("\\t")
		case '\b':
			result.WriteString("\\b")
		case '\f':
			result.WriteString("\\f")
		case '\v':
			result.WriteString("\\v")
		default:
			if unicode.IsControl(r) {
				// Escape other control characters as unicode
				result.WriteString(fmt.Sprintf("\\u%04x", r))
			} else {
				result.WriteRune(r)
			}
		}
	}

	return result.String()
}

func _sanitizeRequestDump(reqDump string) (result string) {
	// Size limit check to prevent ReDoS attacks (10MB max)
	const maxSize = 10 * 1024 * 1024
	if len(reqDump) > maxSize {
		return "***REQUEST_TOO_LARGE_FOR_SANITIZATION***"
	}

	// Add panic protection - return safe placeholder if panic occurs
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Warning: Panic in _sanitizeRequestDump: %v. Returning safe placeholder.", sanitizeForLogging(fmt.Sprintf("%v", r)))
			result = "***REQUEST_SANITIZATION_FAILED***" // Return safe placeholder on panic
		}
	}()

	result = reqDump

	// X-Chroma-Token obfuscation - handle tokens of any length
	if reChromaToken != nil {
		result = reChromaToken.ReplaceAllStringFunc(result, func(match string) string {
			parts := strings.SplitN(match, ":", 2)
			if len(parts) != 2 {
				return match
			}
			token := strings.TrimSpace(parts[1])
			return "X-Chroma-Token: " + _sanitizeToken(token)
		})
	}

	// Bearer token obfuscation - handle tokens of any length
	if reBearerToken != nil {
		result = reBearerToken.ReplaceAllStringFunc(result, func(match string) string {
			parts := strings.SplitN(match, "Bearer ", 2)
			if len(parts) != 2 {
				return match
			}
			token := strings.TrimSpace(parts[1])
			return "Authorization: Bearer " + _sanitizeToken(token)
		})
	}

	// Basic auth obfuscation - handle tokens of any length
	if reBasicAuth != nil {
		result = reBasicAuth.ReplaceAllStringFunc(result, func(match string) string {
			parts := strings.SplitN(match, "Basic ", 2)
			if len(parts) != 2 {
				return match
			}
			token := strings.TrimSpace(parts[1])
			return "Authorization: Basic " + _sanitizeToken(token)
		})
	}

	return result
}

// _sanitizeToken safely obfuscates tokens of any length
func _sanitizeToken(token string) (result string) {
	// Add panic protection for string operations
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Warning: Panic in _sanitizeToken: %v. Returning stars.", sanitizeForLogging(fmt.Sprintf("%v", r)))
			// Return a safe fallback - all stars
			result = "****"
		}
	}()

	tokenLen := len(token)
	if tokenLen == 0 {
		result = ""
		return
	}
	if tokenLen <= 4 {
		// For very short tokens, show only first character
		// Add bounds check even though we know tokenLen > 0
		if tokenLen >= 1 {
			result = string(token[0]) + strings.Repeat("*", tokenLen-1)
		} else {
			result = strings.Repeat("*", tokenLen)
		}
		return
	}
	if tokenLen <= 8 {
		// For short tokens, show first 2 and last 2 characters
		// Bounds are guaranteed by the condition (tokenLen > 4)
		if tokenLen >= 4 {
			result = token[:2] + "..." + token[tokenLen-2:]
		} else {
			result = strings.Repeat("*", tokenLen)
		}
		return
	}
	// For longer tokens, show first 4 and last 4 characters
	// Bounds are guaranteed by the condition (tokenLen > 8)
	if tokenLen >= 8 {
		result = token[:4] + "..." + token[tokenLen-4:]
	} else {
		result = strings.Repeat("*", tokenLen)
	}
	return
}

// _sanitizeResponseDump sanitizes response dumps to remove sensitive data
func _sanitizeResponseDump(respDump string) (result string) {
	// Size limit check to prevent ReDoS attacks (10MB max)
	const maxSize = 10 * 1024 * 1024
	if len(respDump) > maxSize {
		return "***RESPONSE_TOO_LARGE_FOR_SANITIZATION***"
	}

	// Add panic protection - return safe placeholder if panic occurs
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Warning: Panic in _sanitizeResponseDump: %v. Returning safe placeholder.", sanitizeForLogging(fmt.Sprintf("%v", r)))
			result = "***RESPONSE_SANITIZATION_FAILED***" // Return safe placeholder on panic
		}
	}()

	// First obfuscate any tokens that might be in headers
	result = _sanitizeRequestDump(respDump)

	// Sanitize potential sensitive data in JSON responses using pre-compiled patterns
	for _, re := range reJSONPatterns {
		if re != nil {
			result = re.ReplaceAllString(result, `"$1": "***REDACTED***"`)
		}
	}

	return result
}
