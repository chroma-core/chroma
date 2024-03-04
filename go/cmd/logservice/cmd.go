package main

import (
	"github.com/chroma-core/chroma/go/cmd/flag"
	"github.com/chroma-core/chroma/go/pkg/grpcutils"
	"github.com/chroma-core/chroma/go/pkg/logservice/grpc"
	"github.com/chroma-core/chroma/go/pkg/utils"
	"github.com/spf13/cobra"
	"io"
)

var (
	conf = grpc.Config{
		GrpcConfig: &grpcutils.GrpcConfig{},
	}

	Cmd = &cobra.Command{
		Use:   "logservice",
		Short: "Start a logservice service",
		Long:  `RecordLog root command`,
		Run:   exec,
	}
)

func init() {
	// GRPC
	flag.GRPCAddr(Cmd, &conf.GrpcConfig.BindAddress)
	Cmd.Flags().BoolVar(&conf.StartGrpc, "start-grpc", true, "start grpc server or not")

	// DB provider
	Cmd.Flags().StringVar(&conf.DBProvider, "db-provider", "postgres", "DB provider")

	// DB dev
	Cmd.Flags().StringVar(&conf.DBConfig.Address, "db-host", "postgres", "DB host")
	Cmd.Flags().IntVar(&conf.DBConfig.Port, "db-port", 5432, "DB port")
	Cmd.Flags().StringVar(&conf.DBConfig.Username, "db-user", "chroma", "DB user")
	Cmd.Flags().StringVar(&conf.DBConfig.Password, "db-password", "chroma", "DB password")
	Cmd.Flags().StringVar(&conf.DBConfig.DBName, "db-name", "chroma", "DB name")
	Cmd.Flags().StringVar(&conf.DBConfig.SslMode, "ssl-mode", "disable", "SSL mode for database connection")
}

func exec(*cobra.Command, []string) {
	utils.RunProcess(func() (io.Closer, error) {
		return grpc.New(conf)
	})
}
