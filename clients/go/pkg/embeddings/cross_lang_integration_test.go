//go:build crosslang

package embeddings_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	v2 "github.com/chroma-core/chroma/clients/go"

	// Import embedding providers to register them for auto-wiring
	_ "github.com/chroma-core/chroma/clients/go/pkg/embeddings/openai"
)

func init() {
	_ = godotenv.Load("../../../.env")
	// Python chromadb uses CHROMA_OPENAI_API_KEY, ensure it's set from OPENAI_API_KEY
	if os.Getenv("CHROMA_OPENAI_API_KEY") == "" && os.Getenv("OPENAI_API_KEY") != "" {
		_ = os.Setenv("CHROMA_OPENAI_API_KEY", os.Getenv("OPENAI_API_KEY"))
	}
}

// PythonTestResult represents the JSON output from the Python harness
type PythonTestResult struct {
	Status         string   `json:"status"`
	Error          string   `json:"error,omitempty"`
	CollectionName string   `json:"collection_name"`
	EFType         string   `json:"ef_type"`
	EFName         string   `json:"ef_name"`
	DocumentCount  int      `json:"document_count"`
	IDs            []string `json:"ids"`
	Documents      []string `json:"documents"`
	Verification   struct {
		QueryText   string   `json:"query_text"`
		ExpectedIDs []string `json:"expected_ids"`
		NResults    int      `json:"n_results"`
	} `json:"verification"`
	Config map[string]interface{} `json:"config,omitempty"`
}

func findProjectRoot(t *testing.T) string {
	dir, err := os.Getwd()
	require.NoError(t, err)

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find project root")
		}
		dir = parent
	}
}

func setupCrossLangChromaContainer(t *testing.T) (string, func()) {
	ctx := context.Background()

	chromaImage := "ghcr.io/chroma-core/chroma:1.3.3"
	if img := os.Getenv("CHROMA_IMAGE"); img != "" {
		chromaImage = img
	}

	req := testcontainers.ContainerRequest{
		Image:        chromaImage,
		ExposedPorts: []string{"8000/tcp"},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort(nat.Port("8000/tcp")),
			wait.ForHTTP("/api/v2/heartbeat").WithPort("8000/tcp"),
		).WithDeadline(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	host, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.MappedPort(ctx, "8000")
	require.NoError(t, err)

	baseURL := fmt.Sprintf("http://%s:%s", host, port.Port())

	cleanup := func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}

	return baseURL, cleanup
}

func runPythonHarness(t *testing.T, endpoint, efType, collectionPrefix string) *PythonTestResult {
	projectRoot := findProjectRoot(t)
	scriptPath := filepath.Join(projectRoot, "scripts", "cross_lang_ef_test.py")
	venvPython := filepath.Join(projectRoot, ".venv", "bin", "python")

	require.FileExists(t, scriptPath, "Python harness script not found")

	pythonExec := "python3"
	if _, err := os.Stat(venvPython); err == nil {
		pythonExec = venvPython
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, pythonExec, scriptPath,
		"--endpoint", endpoint,
		"--ef-type", efType,
		"--collection-prefix", collectionPrefix,
	)

	cmd.Env = os.Environ()

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		t.Logf("Python stdout: %s", stdout.String())
		t.Logf("Python stderr: %s", stderr.String())
	}
	require.NoError(t, err, "Python harness failed: stdout=%s stderr=%s", stdout.String(), stderr.String())

	var result PythonTestResult
	err = json.Unmarshal(stdout.Bytes(), &result)
	require.NoError(t, err, "Failed to parse Python output: %s", stdout.String())
	require.Equal(t, "success", result.Status, "Python harness error: %s", result.Error)

	return &result
}

func TestCrossLanguage_DefaultEF(t *testing.T) {
	baseURL, cleanup := setupCrossLangChromaContainer(t)
	defer cleanup()

	collectionPrefix := fmt.Sprintf("crosslang_%d_", time.Now().Unix())

	pyResult := runPythonHarness(t, baseURL, "default", collectionPrefix)

	ctx := context.Background()
	client, err := v2.NewHTTPClient(
		v2.WithBaseURL(baseURL),
		v2.WithDatabaseAndTenant(v2.DefaultDatabase, v2.DefaultTenant),
	)
	require.NoError(t, err)
	defer client.Close()

	collection, err := client.GetCollection(ctx, pyResult.CollectionName)
	require.NoError(t, err)
	require.NotNil(t, collection)

	results, err := collection.Query(ctx,
		v2.WithQueryTexts(pyResult.Verification.QueryText),
		v2.WithNResults(pyResult.Verification.NResults),
	)
	require.NoError(t, err)
	require.NotEmpty(t, results.GetIDGroups())

	goIDs := results.GetIDGroups()[0]
	require.Len(t, goIDs, len(pyResult.Verification.ExpectedIDs))

	t.Logf("Python expected IDs: %v", pyResult.Verification.ExpectedIDs)
	t.Logf("Go returned IDs: %v", goIDs)

	err = collection.Add(ctx,
		v2.WithIDs("go_added_doc"),
		v2.WithTexts("This document was added by the Go client"),
	)
	require.NoError(t, err)

	count, err := collection.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, pyResult.DocumentCount+1, count)
}

func TestCrossLanguage_OpenAIEF(t *testing.T) {
	if os.Getenv("OPENAI_API_KEY") == "" {
		t.Skip("OPENAI_API_KEY not set, skipping OpenAI cross-language test")
	}

	baseURL, cleanup := setupCrossLangChromaContainer(t)
	defer cleanup()

	collectionPrefix := fmt.Sprintf("crosslang_%d_", time.Now().Unix())

	pyResult := runPythonHarness(t, baseURL, "openai", collectionPrefix)

	ctx := context.Background()
	client, err := v2.NewHTTPClient(
		v2.WithBaseURL(baseURL),
		v2.WithDatabaseAndTenant(v2.DefaultDatabase, v2.DefaultTenant),
	)
	require.NoError(t, err)
	defer client.Close()

	collection, err := client.GetCollection(ctx, pyResult.CollectionName)
	require.NoError(t, err)
	require.NotNil(t, collection)

	results, err := collection.Query(ctx,
		v2.WithQueryTexts(pyResult.Verification.QueryText),
		v2.WithNResults(pyResult.Verification.NResults),
	)
	require.NoError(t, err)
	require.NotEmpty(t, results.GetIDGroups())

	goIDs := results.GetIDGroups()[0]
	t.Logf("Python expected IDs: %v", pyResult.Verification.ExpectedIDs)
	t.Logf("Go returned IDs: %v", goIDs)

	err = collection.Add(ctx,
		v2.WithIDs("go_added_openai_doc"),
		v2.WithTexts("OpenAI embedding test from Go client"),
	)
	require.NoError(t, err)

	count, err := collection.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, pyResult.DocumentCount+1, count)
}

func TestCrossLanguage_ListCollections_AutoWire(t *testing.T) {
	baseURL, cleanup := setupCrossLangChromaContainer(t)
	defer cleanup()

	collectionPrefix := fmt.Sprintf("crosslang_list_%d_", time.Now().Unix())

	pyResult := runPythonHarness(t, baseURL, "default", collectionPrefix)

	ctx := context.Background()
	client, err := v2.NewHTTPClient(
		v2.WithBaseURL(baseURL),
		v2.WithDatabaseAndTenant(v2.DefaultDatabase, v2.DefaultTenant),
	)
	require.NoError(t, err)
	defer client.Close()

	collections, err := client.ListCollections(ctx)
	require.NoError(t, err)

	var foundCollection v2.Collection
	for _, c := range collections {
		if c.Name() == pyResult.CollectionName {
			foundCollection = c
			break
		}
	}
	require.NotNil(t, foundCollection, "Collection not found in list")

	err = foundCollection.Add(ctx,
		v2.WithIDs("from_list"),
		v2.WithTexts("Added via ListCollections result"),
	)
	require.NoError(t, err)

	count, err := foundCollection.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, pyResult.DocumentCount+1, count)
}

// Cloud tests

func hasCloudCredentials() bool {
	return os.Getenv("CHROMA_API_KEY") != "" &&
		os.Getenv("CHROMA_TENANT") != "" &&
		os.Getenv("CHROMA_DATABASE") != ""
}

func runPythonHarnessCloud(t *testing.T, efType, collectionPrefix string) *PythonTestResult {
	projectRoot := findProjectRoot(t)
	scriptPath := filepath.Join(projectRoot, "scripts", "cross_lang_ef_test.py")
	venvPython := filepath.Join(projectRoot, ".venv", "bin", "python")

	require.FileExists(t, scriptPath, "Python harness script not found")

	pythonExec := "python3"
	if _, err := os.Stat(venvPython); err == nil {
		pythonExec = venvPython
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, pythonExec, scriptPath,
		"--cloud",
		"--ef-type", efType,
		"--collection-prefix", collectionPrefix,
		"--cleanup",
	)

	cmd.Env = os.Environ()

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		t.Logf("Python stdout: %s", stdout.String())
		t.Logf("Python stderr: %s", stderr.String())
	}
	require.NoError(t, err, "Python harness (cloud) failed: stdout=%s stderr=%s", stdout.String(), stderr.String())

	var result PythonTestResult
	err = json.Unmarshal(stdout.Bytes(), &result)
	require.NoError(t, err, "Failed to parse Python output: %s", stdout.String())
	require.Equal(t, "success", result.Status, "Python harness error: %s", result.Error)

	return &result
}

func TestCrossLanguage_Cloud_DefaultEF(t *testing.T) {
	if !hasCloudCredentials() {
		t.Skip("Cloud credentials not set, skipping cloud cross-language test")
	}

	collectionPrefix := fmt.Sprintf("crosslang_cloud_%d_", time.Now().Unix())

	pyResult := runPythonHarnessCloud(t, "default", collectionPrefix)

	ctx := context.Background()
	client, err := v2.NewCloudClient(
		v2.WithDatabaseAndTenant(os.Getenv("CHROMA_DATABASE"), os.Getenv("CHROMA_TENANT")),
		v2.WithCloudAPIKey(os.Getenv("CHROMA_API_KEY")),
	)
	require.NoError(t, err)
	defer client.Close()

	t.Cleanup(func() {
		_ = client.DeleteCollection(ctx, pyResult.CollectionName)
	})

	collection, err := client.GetCollection(ctx, pyResult.CollectionName)
	require.NoError(t, err)
	require.NotNil(t, collection)

	results, err := collection.Query(ctx,
		v2.WithQueryTexts(pyResult.Verification.QueryText),
		v2.WithNResults(pyResult.Verification.NResults),
	)
	require.NoError(t, err)
	require.NotEmpty(t, results.GetIDGroups())

	goIDs := results.GetIDGroups()[0]
	t.Logf("Cloud - Python expected IDs: %v", pyResult.Verification.ExpectedIDs)
	t.Logf("Cloud - Go returned IDs: %v", goIDs)

	err = collection.Add(ctx,
		v2.WithIDs("go_cloud_added_doc"),
		v2.WithTexts("This document was added by the Go client via Cloud"),
	)
	require.NoError(t, err)

	count, err := collection.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, pyResult.DocumentCount+1, count)
}

func TestCrossLanguage_Cloud_OpenAIEF(t *testing.T) {
	if !hasCloudCredentials() {
		t.Skip("Cloud credentials not set, skipping cloud cross-language test")
	}
	if os.Getenv("OPENAI_API_KEY") == "" {
		t.Skip("OPENAI_API_KEY not set, skipping OpenAI cloud cross-language test")
	}

	collectionPrefix := fmt.Sprintf("crosslang_cloud_%d_", time.Now().Unix())

	pyResult := runPythonHarnessCloud(t, "openai", collectionPrefix)

	ctx := context.Background()
	client, err := v2.NewCloudClient(
		v2.WithDatabaseAndTenant(os.Getenv("CHROMA_DATABASE"), os.Getenv("CHROMA_TENANT")),
		v2.WithCloudAPIKey(os.Getenv("CHROMA_API_KEY")),
	)
	require.NoError(t, err)
	defer client.Close()

	t.Cleanup(func() {
		_ = client.DeleteCollection(ctx, pyResult.CollectionName)
	})

	collection, err := client.GetCollection(ctx, pyResult.CollectionName)
	require.NoError(t, err)
	require.NotNil(t, collection)

	results, err := collection.Query(ctx,
		v2.WithQueryTexts(pyResult.Verification.QueryText),
		v2.WithNResults(pyResult.Verification.NResults),
	)
	require.NoError(t, err)
	require.NotEmpty(t, results.GetIDGroups())

	goIDs := results.GetIDGroups()[0]
	t.Logf("Cloud - Python expected IDs: %v", pyResult.Verification.ExpectedIDs)
	t.Logf("Cloud - Go returned IDs: %v", goIDs)

	err = collection.Add(ctx,
		v2.WithIDs("go_cloud_openai_added_doc"),
		v2.WithTexts("OpenAI embedding test from Go client via Cloud"),
	)
	require.NoError(t, err)

	count, err := collection.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, pyResult.DocumentCount+1, count)
}
