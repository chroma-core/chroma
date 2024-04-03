package utils

import (
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog/log"
)

func WaitUntilSignal(closers ...io.Closer) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	sig := <-c
	log.Info().
		Str("signal", sig.String()).
		Msg("Received signal, exiting")

	code := 0
	for _, closer := range closers {
		if err := closer.Close(); err != nil {
			log.Error().
				Err(err).
				Msg("Failed when shutting down server")
			os.Exit(1)
		}
	}

	if code == 0 {
		log.Info().Msg("Shutdown Completed")
	}
	os.Exit(code)
}
