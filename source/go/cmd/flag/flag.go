package flag

import (
	"fmt"

	"github.com/spf13/cobra"
)

const (
	DefaultGRPCPort = 50051
)

func GRPCAddr(cmd *cobra.Command, conf *string) {
	cmd.Flags().StringVarP(conf, "grpc-addr", "g", fmt.Sprintf("0.0.0.0:%d", DefaultGRPCPort), "GRPC service bind address")
}
