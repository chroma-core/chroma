package chroma

import (
	"github.com/chroma-core/chroma/clients/go/pkg/logger"
)

// testLogger returns a text-based slog logger for use in tests.
// It outputs debug-level logs to stdout in a human-readable format.
func testLogger() logger.Logger {
	l, err := logger.NewTextSlogLogger()
	if err != nil {
		// This should never happen as NewTextSlogLogger only creates handlers
		panic("failed to create test logger: " + err.Error())
	}
	return l
}
