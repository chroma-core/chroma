//go:build unix

package defaultef

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDownload(t *testing.T) {
	t.Run("Download", func(t *testing.T) {
		// Set up test environment with temp directory
		tempDir := t.TempDir()
		t.Setenv("HOME", tempDir)

		// Reset config to pick up new HOME
		resetConfigForTesting()

		cfg := getConfig()
		fmt.Println(cfg.OnnxLibPath)
		err := os.RemoveAll(cfg.OnnxLibPath)
		require.NoError(t, err)
		err = EnsureOnnxRuntimeSharedLibrary()
		require.NoError(t, err)
	})
	t.Run("Download Model", func(t *testing.T) {
		// Set up test environment with temp directory
		tempDir := t.TempDir()
		t.Setenv("HOME", tempDir)

		// Reset config to pick up new HOME
		resetConfigForTesting()

		cfg := getConfig()
		err := os.RemoveAll(cfg.OnnxModelCachePath)
		require.NoError(t, err)
		err = EnsureDefaultEmbeddingFunctionModel()
		require.NoError(t, err)
	})
}
