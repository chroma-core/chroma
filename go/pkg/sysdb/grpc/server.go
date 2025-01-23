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

	// Config for soft deletes.
	SoftDeleteEnabled          bool
	SoftDeleteCleanupInterval  time.Duration
	SoftDeleteMaxAge           time.Duration
	SoftDeleteCleanupBatchSize uint

	// Config for testing
	Testing bool

	// Block store provider
	// In production, this should be set to "s3".
	// For local testing, this can be set to "minio".
	BlockStoreProvider string

	// VersionFileEnabled is used to enable/disable version file.
	VersionFileEnabled bool
}

// Server wraps Coordinator with GRPC services.
//
// When Testing is set to true, the GRPC services will not be intialzed. This is
// convenient for end-to-end property based testing.
type Server struct {
	coordinatorpb.UnimplementedSysDBServer
	coordinator       coordinator.Coordinator
	grpcServer        grpcutils.GrpcServer
	healthServer      *health.Server
	softDeleteCleaner *SoftDeleteCleaner
}

func New(config Config) (*Server, error) {
	if config.VersionFileEnabled == true && config.BlockStoreProvider == "none" {
		return nil, errors.New("version file enabled is true but block store provider is none")
	}
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
	ctx := context.Background()
	s := &Server{
		healthServer: health.NewServer(),
	}

	var deleteMode coordinator.DeleteMode
	if config.SoftDeleteEnabled {
		deleteMode = coordinator.SoftDelete
	} else {
		deleteMode = coordinator.HardDelete
	}

	blockStoreProvider := s3metastore.BlockStoreProviderType(config.BlockStoreProvider)
	if !blockStoreProvider.IsValid() {
		log.Error("invalid block store provider", zap.String("provider", string(blockStoreProvider)))
		return nil, errors.New("invalid block store provider")
	}

	s3MetaStore, err := s3metastore.NewS3MetaStore(s3metastore.S3MetaStoreConfig{
		BlockStoreProvider: blockStoreProvider,
	})
	if err != nil {
		return nil, err
	}
	coordinator, err := coordinator.NewCoordinator(ctx, deleteMode, s3MetaStore, config.VersionFileEnabled)
	if err != nil {
		return nil, err
	}
	s.coordinator = *coordinator
	s.softDeleteCleaner = NewSoftDeleteCleaner(*coordinator, config.SoftDeleteCleanupInterval, config.SoftDeleteMaxAge, config.SoftDeleteCleanupBatchSize)
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

		s.grpcServer, err = provider.StartGrpcServer("coordinator", config.GrpcConfig, func(registrar grpc.ServiceRegistrar) {
			coordinatorpb.RegisterSysDBServer(registrar, s)
		})
		if err != nil {
			return nil, err
		}

		s.softDeleteCleaner.Start()
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
