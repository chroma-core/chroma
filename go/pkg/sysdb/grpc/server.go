package grpc

import (
	"context"
	"errors"
	"time"

	"github.com/chroma-core/chroma/go/pkg/grpcutils"

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

func NewWithGrpcProvider(config Config, provider grpcutils.GrpcProvider) (*Server, error) {
	log.Info("Creating new GRPC server with config", zap.Any("config", config))
	ctx := context.Background()
	s := &Server{
		healthServer: health.NewServer(),
	}

	s3MetaStore, err := s3metastore.NewS3MetaStore(config.MetaStoreConfig)
	if err != nil {
		return nil, err
	}

	coordinator, err := coordinator.NewCoordinator(ctx, s3MetaStore, config.VersionFileEnabled)
	if err != nil {
		return nil, err
	}
	s.coordinator = *coordinator
	if !config.Testing {
		namespace := config.KubernetesNamespace
		// Create memberlist manager for query service
		queryMemberlistManager, err := createMemberlistManager(namespace, config.QueryServiceMemberlistName, config.QueryServicePodLabel, config.WatchInterval, config.ReconcileInterval, config.ReconcileCount)
		if err != nil {
			return nil, err
		}

		// Create memberlist manager for compaction service
		compactionMemberlistManager, err := createMemberlistManager(namespace, config.CompactionServiceMemberlistName, config.CompactionServicePodLabel, config.WatchInterval, config.ReconcileInterval, config.ReconcileCount)
		if err != nil {
			return nil, err
		}

		// Create memberlist manager for garbage collection service
		garbageCollectionMemberlistManager, err := createMemberlistManager(namespace, config.GarbageCollectionServiceMemberlistName, config.GarbageCollectionServicePodLabel, config.WatchInterval, config.ReconcileInterval, config.ReconcileCount)
		if err != nil {
			return nil, err
		}

		// Create memberlist manager for log service
		logServiceMemberlistManager, err := createMemberlistManager(namespace, config.LogServiceMemberlistName, config.LogServicePodLabel, config.WatchInterval, config.ReconcileInterval, config.ReconcileCount)
		if err != nil {
			return nil, err
		}

		// Start the memberlist manager for query service
		err = queryMemberlistManager.Start()
		if err != nil {
			return nil, err
		}
		// Start the memberlist manager for compaction service
		err = compactionMemberlistManager.Start()
		if err != nil {
			return nil, err
		}

		// Start the memberlist manager for garbage collection service
		err = garbageCollectionMemberlistManager.Start()
		if err != nil {
			return nil, err
		}

		// Start the memberlist manager for log service
		err = logServiceMemberlistManager.Start()
		if err != nil {
			return nil, err
		}

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
	dynamicClient, err := utils.GetKubernetesDynamicInterface()
	if err != nil {
		return nil, err
	}
	nodeWatcher := memberlist_manager.NewKubernetesWatcher(clientset, namespace, podLabel, watchInterval)
	memberlistStore := memberlist_manager.NewCRMemberlistStore(dynamicClient, namespace, memberlistName)
	memberlist_manager := memberlist_manager.NewMemberlistManager(nodeWatcher, memberlistStore)
	memberlist_manager.SetReconcileInterval(reconcileInterval)
	memberlist_manager.SetReconcileCount(reconcileCount)
	return memberlist_manager, nil
}

func (s *Server) Close() error {
	s.healthServer.Shutdown()
	return nil
}
