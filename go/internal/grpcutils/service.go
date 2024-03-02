package grpcutils

import (
	"crypto/tls"
	"crypto/x509"
	"github.com/chroma/chroma-coordinator/internal/otel"
	"io"
	"net"
	"os"

	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	maxGrpcFrameSize = 256 * 1024 * 1024

	ReadinessProbeService = "chroma-readiness"
)

type GrpcServer interface {
	io.Closer

	Port() int
}

type GrpcProvider interface {
	StartGrpcServer(name string, grpcConfig *GrpcConfig, registerFunc func(grpc.ServiceRegistrar)) (GrpcServer, error)
}

var Default = &defaultProvider{}

type defaultProvider struct {
}

func (d *defaultProvider) StartGrpcServer(name string, grpcConfig *GrpcConfig, registerFunc func(grpc.ServiceRegistrar)) (GrpcServer, error) {
	return newDefaultGrpcProvider(name, grpcConfig, registerFunc)
}

type defaultGrpcServer struct {
	io.Closer
	server *grpc.Server
	port   int
}

func newDefaultGrpcProvider(name string, grpcConfig *GrpcConfig, registerFunc func(grpc.ServiceRegistrar)) (GrpcServer, error) {
	var opts []grpc.ServerOption
	opts = append(opts, grpc.MaxRecvMsgSize(maxGrpcFrameSize))
	if grpcConfig.MTLSEnabled() {
		cert, err := tls.LoadX509KeyPair(grpcConfig.CertPath, grpcConfig.KeyPath)
		if err != nil {
			return nil, err
		}

		ca := x509.NewCertPool()
		caBytes, err := os.ReadFile(grpcConfig.CAPath)
		if err != nil {
			return nil, err
		}
		if !ca.AppendCertsFromPEM(caBytes) {
			return nil, err
		}

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			ClientCAs:    ca,
			ClientAuth:   tls.RequireAndVerifyClientCert,
		}

		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}
	opts = append(opts, grpc.UnaryInterceptor(otel.ServerGrpcInterceptor))

	c := &defaultGrpcServer{
		server: grpc.NewServer(opts...),
	}
	registerFunc(c.server)

	listener, err := net.Listen("tcp", grpcConfig.BindAddress)
	if err != nil {
		return nil, err
	}

	c.port = listener.Addr().(*net.TCPAddr).Port

	log.Info("Started Grpc server")
	if err := c.server.Serve(listener); err != nil {
		log.Fatal("Failed to start serving", zap.Error(err))
	}

	return c, nil
}

func (c *defaultGrpcServer) Port() int {
	return c.port
}

func (c *defaultGrpcServer) Close() error {
	c.server.GracefulStop()
	log.Info("Stopped Grpc server")
	return nil
}
