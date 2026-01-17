package defaultef

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Default_EF(t *testing.T) {
	ef, closeEf, err := NewDefaultEmbeddingFunction()
	require.NoError(t, err)
	t.Cleanup(func() {
		err := closeEf()
		if err != nil {
			t.Logf("error while closing embedding function: %v", err)
		}
	})
	require.NotNil(t, ef)
	embeddings, err := ef.EmbedDocuments(context.TODO(), []string{"Hello Chroma!", "Hello world!"})
	require.NoError(t, err)
	require.NotNil(t, embeddings)
	require.Len(t, embeddings, 2)
	for _, embedding := range embeddings {
		require.Equal(t, embedding.Len(), 384)
	}
}

func TestClose(t *testing.T) {
	ef, closeEf, err := NewDefaultEmbeddingFunction()
	require.NoError(t, err)
	require.NotNil(t, ef)
	err = closeEf()
	require.NoError(t, err)
	_, err = ef.EmbedQuery(context.TODO(), "Hello Chroma!")
	require.Error(t, err)
	require.Contains(t, err.Error(), "embedding function is closed")
}
func TestCloseClosed(t *testing.T) {
	ef := &DefaultEmbeddingFunction{}
	err := ef.Close()
	require.NoError(t, err)
}

func TestCustomOnnxRuntimeVersion(t *testing.T) {
	// Test that CHROMAGO_ONNX_RUNTIME_VERSION env var correctly sets the version
	tempDir := t.TempDir()
	t.Setenv("HOME", tempDir)

	// Test with custom version
	customVersion := "1.21.0"
	t.Setenv("CHROMAGO_ONNX_RUNTIME_VERSION", customVersion)

	// Reset config to pick up the new env var
	resetConfigForTesting()

	cfg := getConfig()
	require.NotNil(t, cfg)
	require.Equal(t, customVersion, cfg.LibOnnxRuntimeVersion, "Config should use custom ONNX Runtime version from env var")

	// Verify the library path contains the version
	require.Contains(t, cfg.OnnxLibPath, customVersion, "Library path should contain the custom version")
}

func TestCustomOnnxRuntimePath(t *testing.T) {
	// This test downloads a specific ONNX Runtime version from GitHub
	// and tests using CHROMAGO_ONNX_RUNTIME_PATH
	// Set RUN_SLOW_TESTS=1 to enable this test
	if os.Getenv("RUN_SLOW_TESTS") != "1" {
		t.Skip("This test requires downloading ~33MB from GitHub and takes time - set RUN_SLOW_TESTS=1 to run")
	}

	// Set up temp directory
	tempDir := t.TempDir()
	t.Setenv("HOME", tempDir)

	// Get platform info
	cos, carch := getOSAndArch()
	if carch == "amd64" {
		carch = "x64"
	}
	if cos == "darwin" {
		cos = "osx"
		if carch == "x64" {
			carch = "x86_64"
		}
	}

	// Download ONNX Runtime 1.23.0 (different from default 1.22.0)
	version := "1.23.0"
	url := "https://github.com/microsoft/onnxruntime/releases/download/v" + version + "/onnxruntime-" + cos + "-" + carch + "-" + version + ".tgz"

	t.Logf("Downloading ONNX Runtime %s for %s-%s from GitHub", version, cos, carch)
	targetArchive := tempDir + "/onnxruntime-" + version + ".tgz"
	err := downloadFile(targetArchive, url)
	require.NoError(t, err, "Failed to download ONNX Runtime from GitHub")

	// Extract the library file
	extractDir := tempDir + "/extracted"
	err = os.MkdirAll(extractDir, 0755)
	require.NoError(t, err)

	// Determine the library filename pattern in the archive
	// Note: tar archives have a leading "./" in the path
	var targetFile string
	if cos == "linux" {
		targetFile = "./onnxruntime-" + cos + "-" + carch + "-" + version + "/lib/libonnxruntime." + getExtensionForOs() + "." + version
	} else {
		targetFile = "./onnxruntime-" + cos + "-" + carch + "-" + version + "/lib/libonnxruntime." + version + "." + getExtensionForOs()
	}

	t.Logf("Extracting %s from archive", targetFile)
	err = extractSpecificFile(targetArchive, targetFile, extractDir)
	require.NoError(t, err, "Failed to extract library from archive")

	// Get the extracted library path - extractSpecificFile uses filepath.Base
	// so we need to construct the path with just the filename
	libFilename := ""
	if cos == "linux" {
		libFilename = "libonnxruntime." + getExtensionForOs() + "." + version
	} else {
		libFilename = "libonnxruntime." + version + "." + getExtensionForOs()
	}
	libPath := extractDir + "/" + libFilename

	// Verify the library file exists
	_, err = os.Stat(libPath)
	require.NoError(t, err, "Extracted library not found at %s", libPath)

	t.Logf("Using custom ONNX Runtime library at: %s", libPath)

	// Set the custom path environment variable
	t.Setenv("CHROMAGO_ONNX_RUNTIME_PATH", libPath)

	// Reset config to pick up the new environment variable
	resetConfigForTesting()

	// Create embedding function - should use the custom library
	ef, closeEf, err := NewDefaultEmbeddingFunction()
	require.NoError(t, err, "Failed to create embedding function with custom ONNX Runtime path")
	t.Cleanup(func() {
		err := closeEf()
		if err != nil {
			t.Logf("error while closing embedding function: %v", err)
		}
	})
	require.NotNil(t, ef)

	// Test that embeddings work with the custom library
	embeddings, err := ef.EmbedDocuments(context.TODO(), []string{"Testing custom ONNX Runtime path"})
	require.NoError(t, err, "Failed to generate embeddings with custom library")
	require.NotNil(t, embeddings)
	require.Len(t, embeddings, 1)
	require.Equal(t, 384, embeddings[0].Len(), "Expected 384-dimensional embeddings")

	t.Logf("✓ Successfully used ONNX Runtime %s from custom path", version)
}
