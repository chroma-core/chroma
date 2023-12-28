package utils

import (
	"io"

	"github.com/rs/zerolog/log"
)

func RunProcess(startProcess func() (io.Closer, error)) {
	process, err := startProcess()
	if err != nil {
		log.Fatal().Err(err).
			Msg("Failed to start the process")
	}

	WaitUntilSignal(
		process,
	)
}
