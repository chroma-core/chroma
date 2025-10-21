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
	Cmd.Flags().BoolVar(&conf.DBConfig.EnableOptimizedCollectionQueries, "enable-optimized-collection-queries", false, "Enable optimized collection queries with CTE (off by default)")

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

	// Garbage collection service Memberlist
	Cmd.Flags().StringVar(&conf.GarbageCollectionServiceMemberlistName, "garbage-collection-memberlist-name", "garbage-collection-service-memberlist", "Garbage collection memberlist name")
	Cmd.Flags().StringVar(&conf.GarbageCollectionServicePodLabel, "garbage-collection-pod-label", "garbage-collection-service", "Garbage collection pod label")

	// Log service Memberlist
	Cmd.Flags().StringVar(&conf.LogServiceMemberlistName, "log-memberlist-name", "rust-log-service-memberlist", "Log service memberlist name")
	Cmd.Flags().StringVar(&conf.LogServicePodLabel, "log-pod-label", "rust-log-service", "Log service pod label")

	// Heap service config (colocated with log service)
	Cmd.Flags().BoolVar(&conf.HeapServiceEnabled, "heap-service-enabled", false, "Enable heap service client")
	Cmd.Flags().IntVar(&conf.HeapServicePort, "heap-service-port", 50052, "Heap service port (colocated with log service)")
	Cmd.Flags().StringVar(&conf.HeapServiceAssignmentHasher, "heap-service-assignment-hasher", "murmur3", "Heap service assignment policy hasher (murmur3, rendezvous)")

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
