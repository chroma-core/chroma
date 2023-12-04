package utils

import (
	"encoding/json"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	"google.golang.org/protobuf/encoding/protojson"
	pb "google.golang.org/protobuf/proto"
)

const DefaultLogLevel = zerolog.InfoLevel

var (
	// LogLevel Used for flags
	LogLevel zerolog.Level
	// LogJson Used for flags
	LogJson bool
)

func ConfigureLogger() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack

	protoMarshal := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	zerolog.InterfaceMarshalFunc = func(i any) ([]byte, error) {
		if m, ok := i.(pb.Message); ok {
			return protoMarshal.Marshal(m)
		}
		return json.Marshal(i)
	}

	log.Logger = zerolog.New(os.Stdout).
		With().
		Timestamp().
		Stack().
		Logger()

	if !LogJson {
		log.Logger = log.Output(zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.StampMicro,
		})
	}
	zerolog.SetGlobalLevel(LogLevel)
}
