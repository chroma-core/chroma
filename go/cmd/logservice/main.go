package main

import (
	"context"
	"github.com/chroma-core/chroma/go/pkg/log/configuration"
	"github.com/chroma-core/chroma/go/pkg/log/purging"
	"github.com/chroma-core/chroma/go/pkg/log/repository"
	"github.com/chroma-core/chroma/go/pkg/log/server"
	"github.com/chroma-core/chroma/go/pkg/proto/logservicepb"
	"github.com/chroma-core/chroma/go/pkg/utils"
	libs "github.com/chroma-core/chroma/go/shared/libs"
	"github.com/chroma-core/chroma/go/shared/otel"
	"github.com/pingcap/log"
	"github.com/rs/zerolog"
	"go.uber.org/automaxprocs/maxprocs"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"net"
)

//func runPurging(lg *repository.LogRepository) (err error) {
//	defer func() {
//		if err != nil {
//			log.Error("failed to run purging", zap.Error(err))
//
//		}
//	}()
//	log.Info("starting purging")
//	ctx := context.Background()
//	podName, _ := os.LookupEnv("POD_NAME")
//	fmt.Println(podName)
//	namespace, _ := os.LookupEnv("POD_NAMESPACE")
//	fmt.Println(namespace)
//	lockName := "log-purging-lock"
//	config, err := rest.InClusterConfig()
//	if err != nil {
//		return err
//	}
//	var client *clientset.Clientset
//	client, err = clientset.NewForConfig(config)
//	if err != nil {
//		return err
//	}
//	lock := &resourcelock.LeaseLock{
//		LeaseMeta: metav1.ObjectMeta{
//			Name:      lockName,
//			Namespace: namespace,
//		},
//		Client: client.CoordinationV1(),
//		LockConfig: resourcelock.ResourceLockConfig{
//			Identity: podName,
//		},
//	}
//	var l *leaderelection.LeaderElector
//	l, err = leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
//		Lock:            lock,
//		ReleaseOnCancel: true,
//		LeaseDuration:   15 * time.Second,
//		RenewDeadline:   10 * time.Second,
//		RetryPeriod:     2 * time.Second,
//
//		Callbacks: leaderelection.LeaderCallbacks{
//			OnStartedLeading: func(ctx context.Context) {
//				log.Info("started leading")
//				ticker := time.NewTicker(10 * time.Second)
//				log.Info("Start loop for purging")
//				for {
//					select {
//					case <-ctx.Done():
//						return
//
//					case <-ticker.C:
//						{
//							log.Info("checking leader status")
//							if l.IsLeader() {
//								log.Info("leader is active")
//								err = lg.PurgeRecords(ctx)
//								if err != nil {
//									log.Error("failed to purge records", zap.Error(err))
//									continue
//								}
//								log.Info("purged records")
//							} else {
//								log.Info("leader is inactive")
//							}
//						}
//					}
//				}
//			},
//			OnStoppedLeading: func() {
//				log.Info("stop leading")
//			},
//		},
//	})
//	if err != nil {
//		return
//	}
//	l.Run(ctx)
//
//	return
//}

func main() {
	ctx := context.Background()

	// Configure logger
	utils.LogLevel = zerolog.DebugLevel
	utils.ConfigureLogger()
	if _, err := maxprocs.Set(); err != nil {
		log.Fatal("can't set maxprocs", zap.Error(err))
	}
	log.Info("Starting log service")
	config := configuration.NewLogServiceConfiguration()
	err := otel.InitTracing(ctx, &otel.TracingConfig{
		Service:  "log-service",
		Endpoint: config.OPTL_TRACING_ENDPOINT,
	})
	if err != nil {
		log.Fatal("failed to initialize tracing", zap.Error(err))
	}
	conn, err := libs.NewPgConnection(ctx, config)
	if err != nil {
		log.Fatal("failed to connect to postgres", zap.Error(err))
	}
	lr := repository.NewLogRepository(conn)
	server := server.NewLogServer(lr)
	var listener net.Listener
	listener, err = net.Listen("tcp", ":"+config.PORT)
	if err != nil {
		log.Fatal("failed to listen", zap.Error(err))
	}
	s := grpc.NewServer(grpc.UnaryInterceptor(otel.ServerGrpcInterceptor))
	logservicepb.RegisterLogServiceServer(s, server)
	log.Info("log service started", zap.String("address", listener.Addr().String()))
	go purging.RunPurging(ctx, lr)
	if err := s.Serve(listener); err != nil {
		log.Fatal("failed to serve", zap.Error(err))
	}
}
