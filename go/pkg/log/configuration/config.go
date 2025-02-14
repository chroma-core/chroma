package configuration

import (
	"os"
	"strconv"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type LogServiceConfiguration struct {
	PORT                  string
	DATABASE_URL          string
	OPTL_TRACING_ENDPOINT string
	SYSDB_CONN            string
	MAX_CONNS             int32
}

func getEnvWithDefault(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func getEnvWithDefaultInt(key string, defaultValue int32) int32 {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	i, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		log.Error("cannot parse from env", zap.String("key", key), zap.Error(err))
		return defaultValue
	}
	return int32(i)
}

func NewLogServiceConfiguration() *LogServiceConfiguration {
	return &LogServiceConfiguration{
		PORT:                  getEnvWithDefault("PORT", "50051"),
		DATABASE_URL:          getEnvWithDefault("CHROMA_DATABASE_URL", "postgresql://chroma:chroma@postgres.chroma.svc.cluster.local:5432/log"),
		OPTL_TRACING_ENDPOINT: getEnvWithDefault("OPTL_TRACING_ENDPOINT", "jaeger:4317"),
		SYSDB_CONN:            getEnvWithDefault("SYSDB_CONN", "sysdb"),
		MAX_CONNS:             getEnvWithDefaultInt("MAX_CONNS", 100),
	}
}
