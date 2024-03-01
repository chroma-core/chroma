package utils

import (
	"os"
	"testing"
)

const environmentVariable = "CHROMA_KUBERNETES_INTEGRATION"

// ShouldRunTests checks if the tests should be run based on an environment variable.
func ShouldRunIntegrationTests() bool {
	// Get the environment variable.
	envVarValue := os.Getenv(environmentVariable)
	// Return true if the environment variable is set to "true", "yes", or "1".
	return envVarValue == "true" || envVarValue == "yes" || envVarValue == "1"
}

// This helper function can be used to skip tests if the environment variable is not set appropriately.
func RunKubernetesIntegrationTest(t *testing.T, testFunc func(t *testing.T)) {
	if ShouldRunIntegrationTests() {
		testFunc(t)
	} else {
		t.Skipf("Skipping test because environment variable %s is not set to run tests", environmentVariable)
	}
}
