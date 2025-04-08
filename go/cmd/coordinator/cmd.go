package main

import (
	"io"
	"time"

	"github.com/chroma-core/chroma/go/pkg/grpcutils"
	"github.com/chroma-core/chroma/go/pkg/sysdb/grpc"

	"github.com/chroma-core/chroma/go/cmd/flag"
	"github.com/chroma-core/chroma/go/pkg/utils"
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
	Cmd.Flags().Uint32Var(&conf.GrpcConfig.MaxConcurrentStreams, "max-concurrent-streams", 100, "Max concurrent streams")
	Cmd.Flags().Uint32Var(&conf.GrpcConfig.NumStreamWorkers, "num-stream-workers", 100, "Number of stream workers")

	// System Catalog
	Cmd.Flags().StringVar(&conf.SystemCatalogProvider, "system-catalog-provider", "database", "System catalog provider")
	Cmd.Flags().StringVar(&conf.DBConfig.Username, "username", "chroma", "MetaTable username")
	Cmd.Flags().StringVar(&conf.DBConfig.Password, "password", "chroma", "MetaTable password")
	Cmd.Flags().StringVar(&conf.DBConfig.Address, "db-address", "postgres", "MetaTable db address")
	Cmd.Flags().StringVar(&conf.DBConfig.ReadAddress, "read-db-address", "postgres", "MetaTable db read only address")
	Cmd.Flags().IntVar(&conf.DBConfig.Port, "db-port", 5432, "MetaTable db port")
	Cmd.Flags().StringVar(&conf.DBConfig.DBName, "db-name", "sysdb", "MetaTable db name")
	Cmd.Flags().IntVar(&conf.DBConfig.MaxIdleConns, "max-idle-conns", 10, "MetaTable max idle connections")
	Cmd.Flags().IntVar(&conf.DBConfig.MaxOpenConns, "max-open-conns", 10, "MetaTable max open connections")
	Cmd.Flags().StringVar(&conf.DBConfig.SslMode, "ssl-mode", "disable", "SSL mode for database connection")

	// Soft deletes
	Cmd.Flags().BoolVar(&conf.SoftDeleteEnabled, "soft-delete-enabled", true, "Enable soft deletes")
	Cmd.Flags().DurationVar(&conf.SoftDeleteCleanupInterval, "soft-delete-cleanup-interval", 30*time.Second, "Soft delete cleanup interval")
	Cmd.Flags().DurationVar(&conf.SoftDeleteMaxAge, "soft-delete-max-age", 72*time.Hour, "Soft delete max age")
	Cmd.Flags().UintVar(&conf.SoftDeleteCleanupBatchSize, "soft-delete-cleanup-batch-size", 100, "Soft delete cleanup batch size")
	// With above values, the soft delete cleaner can remove around 1000 collections in 5 minutes.

	// Memberlist
	Cmd.Flags().StringVar(&conf.KubernetesNamespace, "kubernetes-namespace", "chroma", "Kubernetes namespace")
	Cmd.Flags().DurationVar(&conf.ReconcileInterval, "reconcile-interval", 100*time.Millisecond, "Reconcile interval")
	Cmd.Flags().UintVar(&conf.ReconcileCount, "reconcile-count", 10, "Reconcile count")

	// Query service memberlist
	Cmd.Flags().StringVar(&conf.QueryServiceMemberlistName, "query-memberlist-name", "query-service-memberlist", "Query service memberlist name")
	Cmd.Flags().StringVar(&conf.QueryServicePodLabel, "query-pod-label", "query-service", "Query pod label")
	Cmd.Flags().DurationVar(&conf.WatchInterval, "watch-interval", 10*time.Second, "Watch interval")

	// Compaction service Memberlist
	Cmd.Flags().StringVar(&conf.CompactionServiceMemberlistName, "compaction-memberlist-name", "compaction-service-memberlist", "Compaction memberlist name")
	Cmd.Flags().StringVar(&conf.CompactionServicePodLabel, "compaction-pod-label", "compaction-service", "Compaction pod label")

	// S3 config
	Cmd.Flags().BoolVar(&conf.MetaStoreConfig.CreateBucketIfNotExists, "create-bucket-if-not-exists", false, "Create bucket if not exists")
	Cmd.Flags().StringVar(&conf.MetaStoreConfig.BucketName, "bucket-name", "chroma-storage", "Bucket name")
	Cmd.Flags().StringVar(&conf.MetaStoreConfig.Region, "s3-region", "us-east-1", "Region")
	Cmd.Flags().StringVar(&conf.MetaStoreConfig.Endpoint, "s3-endpoint", "", "S3 endpoint")
	Cmd.Flags().StringVar(&conf.MetaStoreConfig.AccessKeyID, "s3-access-key-id", "", "S3 access key ID")
	Cmd.Flags().StringVar(&conf.MetaStoreConfig.SecretAccessKey, "s3-secret-access-key", "", "S3 secret access key")
	Cmd.Flags().BoolVar(&conf.MetaStoreConfig.ForcePathStyle, "s3-force-path-style", false, "S3 force path style")

	// Version file
	Cmd.Flags().BoolVar(&conf.VersionFileEnabled, "version-file-enabled", false, "Enable version file")
}

func exec(*cobra.Command, []string) {
	utils.RunProcess(func() (io.Closer, error) {
		return grpc.New(conf)
	})
}
