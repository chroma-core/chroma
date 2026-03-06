package grpc

import (
	"context"
	"errors"
	"time"

	"github.com/chroma-core/chroma/go/pkg/grpcutils"
	"github.com/chroma-core/chroma/go/pkg/leader"
	"github.com/chroma-core/chroma/go/pkg/memberlist_manager"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/sysdb/coordinator"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbcore"
	s3metastore "github.com/chroma-core/chroma/go/pkg/sysdb/metastore/s3"
	"github.com/chroma-core/chroma/go/pkg/utils"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type Config struct {
	// GrpcConfig config
	GrpcConfig *grpcutils.GrpcConfig

	// System catalog provider
	SystemCatalogProvider string

	// MetaTable config
	DBConfig dbcore.DBConfig

	// Kubernetes config
	KubernetesNamespace string

	// Memberlist config
	ReconcileInterval time.Duration
	ReconcileCount    uint

	// Query service memberlist config
	QueryServiceMemberlistName string
	QueryServicePodLabel       string

	// Watcher config
	WatchInterval time.Duration

	// Compaction service memberlist config
	CompactionServiceMemberlistName string
	CompactionServicePodLabel       string

	// Garbage collection service memberlist config
	GarbageCollectionServiceMemberlistName string
	GarbageCollectionServicePodLabel       string

	// Log service memberlist config
	LogServiceMemberlistName string
	LogServicePodLabel       string

	// Heap service config (colocated with log service)
	HeapServiceEnabled          bool
	HeapServicePort             int    // Default: 50052
	HeapServiceAssignmentHasher string // Assignment policy hasher: "murmur3" (default), etc.

	// Config for testing
	Testing bool

	MetaStoreConfig s3metastore.S3MetaStoreConfig

	// VersionFileEnabled is used to enable/disable version file.
	VersionFileEnabled bool
}

// Server wraps Coordinator with GRPC services.
//
// When Testing is set to true, the GRPC services will not be intialzed. This is
// convenient for end-to-end property based testing.
type Server struct {
	coordinatorpb.UnimplementedSysDBServer
	coordinator  coordinator.Coordinator
	grpcServer   grpcutils.GrpcServer
	healthServer *health.Server
}

func New(config Config) (*Server, error) {
	if config.SystemCatalogProvider == "memory" {
		return NewWithGrpcProvider(config, grpcutils.Default)
	} else if config.SystemCatalogProvider == "database" {
		dBConfig := config.DBConfig
		err := dbcore.ConnectDB(dBConfig)
		if err != nil {
			return nil, err
		}
		return NewWithGrpcProvider(config, grpcutils.Default)
	} else {
		return nil, errors.New("invalid system catalog provider, only memory and database are supported")
	}
}

func StartMemberListManagers(leaderCtx context.Context, config Config) error {
	namespace := config.KubernetesNamespace

	// Store managers for cleanup
	managers := []struct {
		serviceType    string
		manager        *memberlist_manager.MemberlistManager
		memberlistName string
		podLabel       string
	}{
		{"query", nil, config.QueryServiceMemberlistName, config.QueryServicePodLabel},
		{"compaction", nil, config.CompactionServiceMemberlistName, config.CompactionServicePodLabel},
		{"garbage_collection", nil, config.GarbageCollectionServiceMemberlistName, config.GarbageCollectionServicePodLabel},
		{"log", nil, config.LogServiceMemberlistName, config.LogServicePodLabel},
	}

	for i, m := range managers {
		manager, err := createMemberlistManager(namespace, m.memberlistName, m.podLabel, config.WatchInterval, config.ReconcileInterval, config.ReconcileCount)
		if err != nil {
			log.Error("Failed to create memberlist manager for service", zap.String("service", m.serviceType), zap.Error(err))
			return err
		}
		managers[i].manager = manager
	}

	// Start all memberlist managers
	for _, m := range managers {
		if err := m.manager.Start(); err != nil {
			log.Error("Failed to start memberlist manager for service", zap.String("service", m.serviceType), zap.Error(err))
		}
	}

	// Wait for context cancellation (leadership lost)
	<-leaderCtx.Done()

	// Stop all memberlist managers
	for _, m := range managers {
		m.manager.Stop()
	}
	return nil
}

func NewWithGrpcProvider(config Config, provider grpcutils.GrpcProvider) (*Server, error) {
	log.Info("Creating new GRPC server with config", zap.Any("config", config))
	ctx := context.Background()
	s := &Server{
		healthServer: health.NewServer(),
	}

	s3MetaStore, err := s3metastore.NewS3MetaStore(ctx, config.MetaStoreConfig)
	if err != nil {
		return nil, err
	}

	coordinator, err := coordinator.NewCoordinator(ctx, coordinator.CoordinatorConfig{
		ObjectStore:                 s3MetaStore,
		VersionFileEnabled:          config.VersionFileEnabled,
		HeapServiceEnabled:          config.HeapServiceEnabled,
		HeapServicePort:             config.HeapServicePort,
		HeapServiceAssignmentHasher: config.HeapServiceAssignmentHasher,
		KubernetesNamespace:         config.KubernetesNamespace,
		LogServiceMemberlistName:    config.LogServiceMemberlistName,
	})
	if err != nil {
		return nil, err
	}
	s.coordinator = *coordinator
	if !config.Testing {
		// Start leader election for memberlist management
		go leader.AcquireLeaderLock(context.Background(), func(leaderCtx context.Context) {
			log.Info("Acquired leadership for memberlist management")
			if err := StartMemberListManagers(leaderCtx, config); err != nil {
				log.Error("Failed to start memberlist manager", zap.Error(err))
			}
			log.Info("Released leadership for memberlist management")
		})
		log.Info("Starting GRPC server")
		s.grpcServer, err = provider.StartGrpcServer("coordinator", config.GrpcConfig, func(registrar grpc.ServiceRegistrar) {
			coordinatorpb.RegisterSysDBServer(registrar, s)
			healthgrpc.RegisterHealthServer(registrar, s.healthServer)
		})
		if err != nil {
			return nil, err
		}

		s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	}
	return s, nil
}

func createMemberlistManager(namespace string, memberlistName string, podLabel string, watchInterval time.Duration, reconcileInterval time.Duration, reconcileCount uint) (*memberlist_manager.MemberlistManager, error) {
	log.Info("Creating memberlist manager for {}", zap.String("memberlist", memberlistName))
	clientset, err := utils.GetKubernetesInterface()
	if err != nil {
		return nil, err
	}
	memberlistStore, err := memberlist_manager.NewCRMemberlistStoreFromK8s(namespace, memberlistName)
	if err != nil {
		return nil, err
	}
	nodeWatcher := memberlist_manager.NewKubernetesWatcher(clientset, namespace, podLabel, watchInterval)
	memberlist_manager := memberlist_manager.NewMemberlistManager(nodeWatcher, memberlistStore)
	memberlist_manager.SetReconcileInterval(reconcileInterval)
	memberlist_manager.SetReconcileCount(reconcileCount)
	return memberlist_manager, nil
}

func (s *Server) Close() error {
	s.healthServer.Shutdown()
	return nil
}
