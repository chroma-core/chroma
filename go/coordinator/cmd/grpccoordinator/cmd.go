package grpccoordinator

import (
	"io"
	"time"

	"github.com/chroma/chroma-coordinator/cmd/flag"
	"github.com/chroma/chroma-coordinator/internal/grpccoordinator"
	"github.com/chroma/chroma-coordinator/internal/utils"
	"github.com/spf13/cobra"
)

var (
	conf = grpccoordinator.Config{}

	Cmd = &cobra.Command{
		Use:   "coordinator",
		Short: "Start a coordinator",
		Long:  `Long description`,
		Run:   exec,
	}
)

func init() {
	// GRPC
	flag.GRPCAddr(Cmd, &conf.BindAddress)

	// System Catalog
	Cmd.Flags().StringVar(&conf.SystemCatalogProvider, "system-catalog-provider", "memory", "System catalog provider")
	Cmd.Flags().StringVar(&conf.Username, "username", "root", "MetaTable username")
	Cmd.Flags().StringVar(&conf.Password, "password", "", "MetaTable password")
	Cmd.Flags().StringVar(&conf.Address, "db-address", "127.0.0.1", "MetaTable db address")
	Cmd.Flags().IntVar(&conf.Port, "db-port", 5432, "MetaTable db port")
	Cmd.Flags().StringVar(&conf.DBName, "db-name", "", "MetaTable db name")
	Cmd.Flags().IntVar(&conf.MaxIdleConns, "max-idle-conns", 10, "MetaTable max idle connections")
	Cmd.Flags().IntVar(&conf.MaxOpenConns, "max-open-conns", 10, "MetaTable max open connections")

	// Pulsar
	Cmd.Flags().StringVar(&conf.PulsarAdminURL, "pulsar-admin-url", "http://localhost:8080", "Pulsar admin url")
	Cmd.Flags().StringVar(&conf.PulsarURL, "pulsar-url", "pulsar://localhost:6650", "Pulsar url")
	Cmd.Flags().StringVar(&conf.PulsarTenant, "pulsar-tenant", "public", "Pulsar tenant")
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
		return grpccoordinator.New(conf)
	})
}
