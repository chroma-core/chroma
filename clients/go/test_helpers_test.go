package chroma

import (
	"os"

	"github.com/chroma-core/chroma/clients/go/pkg/logger"
)

// testLogger returns a text-based slog logger for use in tests.
// By default it outputs debug-level logs. Set CI=true env var to use info level
// to reduce output volume in CI environments.
func testLogger() logger.Logger {
	var l logger.Logger
	var err error

	// Use info level in CI to reduce output volume and avoid runner termination
	if os.Getenv("CI") == "true" {
		l, err = logger.NewInfoSlogLogger()
	} else {
		l, err = logger.NewTextSlogLogger()
	}

	if err != nil {
		// This should never happen as the logger constructors only create handlers
		panic("failed to create test logger: " + err.Error())
	}
	return l
}
