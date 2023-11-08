package grpccoordinator

import (
	"context"
	"errors"

	"github.com/chroma/chroma-coordinator/internal/coordinator"
	"github.com/chroma/chroma-coordinator/internal/grpccoordinator/grpcutils"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbcore"
	"github.com/chroma/chroma-coordinator/internal/proto/coordinatorpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"gorm.io/gorm"
)

type Config struct {
	// GRPC config
	BindAddress string

	// System catalog provider
	SystemCatalogProvider string

	// MetaTable config
	Username     string
	Password     string
	Address      string
	DBName       string
	MaxIdleConns int
	MaxOpenConns int

	// Config for testing
	Testing bool
}

// Server wraps Coordinator with GRPC services.
//
// When Testing is set to true, the GRPC services will not be intialzed. This is
// convenient for end-to-end property based testing.
type Server struct {
	coordinatorpb.UnimplementedSysDBServer
	coordinator  coordinator.ICoordinator
	grpcServer   grpcutils.GrpcServer
	healthServer *health.Server
}

func New(config Config) (*Server, error) {
	if config.SystemCatalogProvider == "memory" {
		return NewWithGrpcProvider(config, grpcutils.Default, nil)
	} else if config.SystemCatalogProvider == "database" {
		dBConfig := dbcore.DBConfig{
			Username:     config.Username,
			Password:     config.Password,
			Address:      config.Address,
			DBName:       config.DBName,
			MaxIdleConns: config.MaxIdleConns,
			MaxOpenConns: config.MaxOpenConns,
		}
		db, err := dbcore.Connect(dBConfig)
		if err != nil {
			return nil, err
		}
		return NewWithGrpcProvider(config, grpcutils.Default, db)
	} else {
		return nil, errors.New("invalid system catalog provider, only memory and database are supported")
	}

}

func NewWithGrpcProvider(config Config, provider grpcutils.GrpcProvider, db *gorm.DB) (*Server, error) {
	ctx := context.Background()
	s := &Server{
		healthServer: health.NewServer(),
	}
	assignmentPolicy := coordinator.NewSimpleAssignmentPolicy("test-tenant", "test-topic")
	coordinator, err := coordinator.NewCoordinator(ctx, assignmentPolicy, db)
	if err != nil {
		return nil, err
	}
	s.coordinator = coordinator
	s.coordinator.Start()

	if !config.Testing {
		s.grpcServer, err = provider.StartGrpcServer("coordinator", config.BindAddress, func(registrar grpc.ServiceRegistrar) {
			coordinatorpb.RegisterSysDBServer(registrar, s)
		})
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Server) Close() error {
	s.healthServer.Shutdown()
	return nil
}
