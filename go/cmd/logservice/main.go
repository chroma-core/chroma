package main

import (
	"context"
	"github.com/chroma-core/chroma/go/pkg/log/configuration"
	"github.com/chroma-core/chroma/go/pkg/log/repository"
	"github.com/chroma-core/chroma/go/pkg/log/server"
	"github.com/chroma-core/chroma/go/pkg/proto/logservicepb"
	"github.com/chroma-core/chroma/go/pkg/utils"
	libs "github.com/chroma-core/chroma/go/shared/libs"
	"github.com/pingcap/log"
	"github.com/rs/zerolog"
	"go.uber.org/automaxprocs/maxprocs"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"net"
)

func main() {
	ctx := context.Background()
	// Configure logger
	utils.LogLevel = zerolog.DebugLevel
	utils.ConfigureLogger()
	if _, err := maxprocs.Set(); err != nil {
		log.Fatal("can't set maxprocs", zap.Error(err))
	}
	log.Info("Starting log service")
	config := configuration.NewLogServiceConfiguration()
	conn, err := libs.NewPgConnection(ctx, config)
	if err != nil {
		log.Fatal("failed to connect to postgres", zap.Error(err))
	}
	lr := repository.NewLogRepository(conn)
	server := server.NewLogServer(lr)
	var listener net.Listener
	listener, err = net.Listen("tcp", ":"+config.PORT)
	if err != nil {
		log.Fatal("failed to listen", zap.Error(err))
	}
	s := grpc.NewServer()
	logservicepb.RegisterLogServiceServer(s, server)
	log.Info("log service started", zap.String("address", listener.Addr().String()))
	if err := s.Serve(listener); err != nil {
		log.Fatal("failed to serve", zap.Error(err))
	}
}
