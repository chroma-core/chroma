package main

import (
	"github.com/chroma-core/chroma/go/internal/coordinator/grpc"
	"github.com/chroma-core/chroma/go/internal/grpcutils"
	"io"
	"time"

	"github.com/chroma-core/chroma/go/cmd/flag"
	"github.com/chroma-core/chroma/go/internal/utils"
	"github.com/spf13/cobra"
)

var (
	conf = grpc.Config{
		GrpcConfig: &grpcutils.GrpcConfig{},
	}

	Cmd = &cobra.Command{
		Use:   "coordinator",
		Short: "Start a coordinator",
		Long:  `Long description`,
		Run:   exec,
	}
)

func init() {

	// GRPC
	flag.GRPCAddr(Cmd, &conf.GrpcConfig.BindAddress)

	// System Catalog
	Cmd.Flags().StringVar(&conf.SystemCatalogProvider, "system-catalog-provider", "database", "System catalog provider")
	Cmd.Flags().StringVar(&conf.DBConfig.Username, "username", "chroma", "MetaTable username")
	Cmd.Flags().StringVar(&conf.DBConfig.Password, "password", "chroma", "MetaTable password")
	Cmd.Flags().StringVar(&conf.DBConfig.Address, "db-address", "postgres", "MetaTable db address")
	Cmd.Flags().IntVar(&conf.DBConfig.Port, "db-port", 5432, "MetaTable db port")
	Cmd.Flags().StringVar(&conf.DBConfig.DBName, "db-name", "chroma", "MetaTable db name")
	Cmd.Flags().IntVar(&conf.DBConfig.MaxIdleConns, "max-idle-conns", 10, "MetaTable max idle connections")
	Cmd.Flags().IntVar(&conf.DBConfig.MaxOpenConns, "max-open-conns", 10, "MetaTable max open connections")
	Cmd.Flags().StringVar(&conf.DBConfig.SslMode, "ssl-mode", "disable", "SSL mode for database connection")

	// Pulsar
	Cmd.Flags().StringVar(&conf.PulsarAdminURL, "pulsar-admin-url", "http://localhost:8080", "Pulsar admin url")
	Cmd.Flags().StringVar(&conf.PulsarURL, "pulsar-url", "pulsar://localhost:6650", "Pulsar url")
	Cmd.Flags().StringVar(&conf.PulsarTenant, "pulsar-tenant", "default", "Pulsar tenant")
	Cmd.Flags().StringVar(&conf.PulsarNamespace, "pulsar-namespace", "default", "Pulsar namespace")

	// Notification
	Cmd.Flags().StringVar(&conf.NotificationStoreProvider, "notification-store-provider", "memory", "Notification store provider")
	Cmd.Flags().StringVar(&conf.NotifierProvider, "notifier-provider", "memory", "Notifier provider")
	Cmd.Flags().StringVar(&conf.NotificationTopic, "notification-topic", "chroma-notification", "Notification topic")

	// Memberlist
	Cmd.Flags().StringVar(&conf.KubernetesNamespace, "kubernetes-namespace", "chroma", "Kubernetes namespace")
	Cmd.Flags().StringVar(&conf.WorkerMemberlistName, "worker-memberlist-name", "worker-memberlist", "Worker memberlist name")
	Cmd.Flags().StringVar(&conf.AssignmentPolicy, "assignment-policy", "rendezvous", "Assignment policy")
	Cmd.Flags().DurationVar(&conf.WatchInterval, "watch-interval", 60*time.Second, "Watch interval")
}

func exec(*cobra.Command, []string) {
	utils.RunProcess(func() (io.Closer, error) {
		return grpc.New(conf)
	})
}
