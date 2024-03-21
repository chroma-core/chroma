package grpc

import (
	"context"
	"errors"
	"time"

	"github.com/chroma-core/chroma/go/pkg/grpcutils"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/chroma-core/chroma/go/pkg/coordinator"
	"github.com/chroma-core/chroma/go/pkg/memberlist_manager"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dao"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbcore"
	"github.com/chroma-core/chroma/go/pkg/notification"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/utils"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"gorm.io/gorm"
)

type Config struct {
	// GrpcConfig config
	GrpcConfig *grpcutils.GrpcConfig

	// System catalog provider
	SystemCatalogProvider string

	// MetaTable config
	DBConfig dbcore.DBConfig

	// Notification config
	NotificationStoreProvider string
	NotifierProvider          string
	NotificationTopic         string

	// Pulsar config
	PulsarAdminURL  string
	PulsarURL       string
	PulsarTenant    string
	PulsarNamespace string

	// Kubernetes config
	KubernetesNamespace  string
	WorkerMemberlistName string
	WorkerPodLabel       string

	// Assignment policy config can be "simple" or "rendezvous"
	AssignmentPolicy string

	// Watcher config
	WatchInterval time.Duration

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
		dBConfig := config.DBConfig
		db, err := dbcore.ConnectPostgres(dBConfig)
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

	var assignmentPolicy coordinator.CollectionAssignmentPolicy
	if config.AssignmentPolicy == "simple" {
		log.Info("Using simple assignment policy")
		assignmentPolicy = coordinator.NewSimpleAssignmentPolicy(config.PulsarTenant, config.PulsarNamespace)
	} else if config.AssignmentPolicy == "rendezvous" {
		log.Info("Using rendezvous assignment policy")
		//err := utils.CreateTopics(config.PulsarAdminURL, config.PulsarTenant, config.PulsarNamespace, coordinator.Topics[:])
		//if err != nil {
		//	log.Error("Failed to create topics", zap.Error(err))
		//	return nil, err
		//}
		assignmentPolicy = coordinator.NewRendezvousAssignmentPolicy(config.PulsarTenant, config.PulsarNamespace)
	} else {
		return nil, errors.New("invalid assignment policy, only simple and rendezvous are supported")
	}

	var notificationStore notification.NotificationStore
	if config.NotificationStoreProvider == "memory" {
		log.Info("Using memory notification store")
		notificationStore = notification.NewMemoryNotificationStore()
	} else if config.NotificationStoreProvider == "database" {
		txnImpl := dbcore.NewTxImpl()
		metaDomain := dao.NewMetaDomain()
		notificationStore = notification.NewDatabaseNotificationStore(txnImpl, metaDomain)
	} else {
		return nil, errors.New("invalid notification store provider, only memory and database are supported")
	}

	var notifier notification.Notifier
	var client pulsar.Client
	var producer pulsar.Producer
	if config.NotifierProvider == "memory" {
		log.Info("Using memory notifier")
		notifier = notification.NewMemoryNotifier()
	} else if config.NotifierProvider == "pulsar" {
		log.Info("Using pulsar notifier")
		pulsarNotifier, pulsarClient, pulsarProducer, err := createPulsarNotifer(config.PulsarURL, config.NotificationTopic)
		notifier = pulsarNotifier
		client = pulsarClient
		producer = pulsarProducer
		if err != nil {
			log.Error("Failed to create pulsar notifier", zap.Error(err))
			return nil, err
		}
	} else {
		return nil, errors.New("invalid notifier provider, only memory and pulsar are supported")
	}

	if client != nil {
		defer client.Close()
	}
	if producer != nil {
		defer producer.Close()
	}

	coordinator, err := coordinator.NewCoordinator(ctx, assignmentPolicy, db, notificationStore, notifier)
	if err != nil {
		return nil, err
	}
	s.coordinator = coordinator
	s.coordinator.Start()
	if !config.Testing {
		memberlist_manager, err := createMemberlistManager(config)
		if err != nil {
			return nil, err
		}

		// Start the memberlist manager
		err = memberlist_manager.Start()
		if err != nil {
			return nil, err
		}

		s.grpcServer, err = provider.StartGrpcServer("coordinator", config.GrpcConfig, func(registrar grpc.ServiceRegistrar) {
			coordinatorpb.RegisterSysDBServer(registrar, s)
		})
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

func createMemberlistManager(config Config) (*memberlist_manager.MemberlistManager, error) {
	log.Info("Creating memberlist manager")
	memberlist_name := config.WorkerMemberlistName
	namespace := config.KubernetesNamespace
	clientset, err := utils.GetKubernetesInterface()
	if err != nil {
		return nil, err
	}
	dynamicClient, err := utils.GetKubernetesDynamicInterface()
	if err != nil {
		return nil, err
	}
	nodeWatcher := memberlist_manager.NewKubernetesWatcher(clientset, namespace, config.WorkerPodLabel, config.WatchInterval)
	memberlistStore := memberlist_manager.NewCRMemberlistStore(dynamicClient, namespace, memberlist_name)
	memberlist_manager := memberlist_manager.NewMemberlistManager(nodeWatcher, memberlistStore)
	return memberlist_manager, nil
}

func createPulsarNotifer(pulsarURL string, notificationTopic string) (*notification.PulsarNotifier, pulsar.Client, pulsar.Producer, error) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: pulsarURL,
	})
	if err != nil {
		log.Error("Failed to create pulsar client", zap.Error(err))
		return nil, nil, nil, err
	}

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: notificationTopic,
	})
	if err != nil {
		log.Error("Failed to create producer", zap.Error(err))
		return nil, nil, nil, err
	}

	notifier := notification.NewPulsarNotifier(producer)
	return notifier, client, producer, nil
}

func (s *Server) Close() error {
	s.healthServer.Shutdown()
	s.coordinator.Stop()
	return nil
}
