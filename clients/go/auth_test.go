//go:build !cloud

package chroma

import (
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObfuscateRequestDump(t *testing.T) {

	t.Run("Obfuscate API Key in Request Dump", func(t *testing.T) {
		apiKey := "ck-1dummyapikey1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
		reqDump := `Host: api.trychroma.com:8000
User-Agent: chroma-go-client/1.0
Accept: application/json
Content-Type: application/json
X-Chroma-Token: ` + apiKey + `
Accept-Encoding: gzip
`
		obfuscatedDump := _sanitizeRequestDump(reqDump)
		require.NotContains(t, obfuscatedDump, apiKey, "API key should be obfuscated in request dump")
		fmt.Println(obfuscatedDump)
	})
	t.Run("Authorization Bearer Token Obfuscation", func(t *testing.T) {
		token := "my-super-secret-token"
		reqDump := `Host: api.trychroma.com:8000
User-Agent: chroma-go-client/1.0
Accept: application/json
Content-Type: application/json
Authorization: Bearer ` + token + `
Accept-Encoding: gzip
`
		obfuscatedDump := _sanitizeRequestDump(reqDump)
		require.NotContains(t, obfuscatedDump, token, "Token should be obfuscated in request dump")
		fmt.Println(obfuscatedDump)
	})

	t.Run("Authorization Basic", func(t *testing.T) {
		auth := base64.StdEncoding.EncodeToString([]byte("user:password"))
		reqDump := `Host: api.trychroma.com:8000
User-Agent: chroma-go-client/1.0
Accept: application/json
Content-Type: application/json
Authorization: Basic ` + auth + `
Accept-Encoding: gzip
`
		obfuscatedDump := _sanitizeRequestDump(reqDump)
		require.NotContains(t, obfuscatedDump, auth, "Token should be obfuscated in request dump")
		fmt.Println(obfuscatedDump)
	})

}
