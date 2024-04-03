package grpc

import (
	"context"
	"errors"
	"github.com/chroma-core/chroma/go/pkg/grpcutils"
	"github.com/chroma-core/chroma/go/pkg/logservice"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbcore"
	"github.com/chroma-core/chroma/go/pkg/proto/logservicepb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
)

type Config struct {
	// GrpcConfig config
	GrpcConfig *grpcutils.GrpcConfig

	// System catalog provider
	DBProvider string

	// Postgres config
	DBConfig dbcore.DBConfig

	// whether to start grpc service
	StartGrpc bool
}

type Server struct {
	logservicepb.UnimplementedLogServiceServer
	logService   logservice.IRecordLog
	grpcServer   grpcutils.GrpcServer
	healthServer *health.Server
}

func New(config Config) (*Server, error) {
	log.Info("New Log Service...")

	if config.DBProvider == "postgres" {
		dBConfig := config.DBConfig
		_, err := dbcore.ConnectPostgres(dBConfig)
		if err != nil {
			log.Error("Error connecting to Postgres DB.", zap.Error(err))
			panic(err)
		}
	} else {
		log.Error("invalid DB provider, only postgres is supported")
		return nil, errors.New("invalid DB provider, only postgres is supported")
	}

	s := startLogService()
	if config.StartGrpc {
		s.grpcServer = startGrpcService(s, config.GrpcConfig)
	}

	log.Info("New Log Service Completed.")
	return s, nil
}

func startLogService() *Server {
	log.Info("Staring Log Service...")
	ctx := context.Background()
	s := &Server{
		healthServer: health.NewServer(),
	}

	logService, err := logservice.NewLogService(ctx)
	if err != nil {
		log.Error("Error creating Log Service.", zap.Error(err))
		panic(err)
	}
	s.logService = logService
	err = s.logService.Start()
	if err != nil {
		log.Error("Error starting Log Service.", zap.Error(err))
		panic(err)
	}
	log.Info("Log Service Started.")
	return s
}

func startGrpcService(s *Server, grpcConfig *grpcutils.GrpcConfig) grpcutils.GrpcServer {
	log.Info("Staring Grpc Service...")
	server, err := grpcutils.Default.StartGrpcServer("logservice", grpcConfig, func(registrar grpc.ServiceRegistrar) {
		logservicepb.RegisterLogServiceServer(registrar, s)
	})
	if err != nil {
		log.Error("Error starting grpc Service.", zap.Error(err))
		panic(err)
	}
	return server
}

func (s *Server) Close() error {
	s.healthServer.Shutdown()
	err := s.logService.Stop()
	if err != nil {
		log.Error("Failed to stop log service", zap.Error(err))
		return err
	}
	log.Info("Server closed")
	return nil
}
