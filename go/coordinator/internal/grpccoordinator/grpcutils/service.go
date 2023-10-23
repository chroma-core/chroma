package grpcutils

import (
	"io"
	"net"

	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
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
	StartGrpcServer(name, bindAddress string, registerFunc func(grpc.ServiceRegistrar)) (GrpcServer, error)
}

var Default = &defaultProvider{}

type defaultProvider struct {
}

func (d *defaultProvider) StartGrpcServer(name, bindAddress string, registerFunc func(grpc.ServiceRegistrar)) (GrpcServer, error) {
	return newDefaultGrpcProvider(name, bindAddress, registerFunc)
}

type defaultGrpcServer struct {
	io.Closer
	server *grpc.Server
	port   int
}

func newDefaultGrpcProvider(name, bindAddress string, registerFunc func(grpc.ServiceRegistrar)) (GrpcServer, error) {
	c := &defaultGrpcServer{
		server: grpc.NewServer(
			grpc.MaxRecvMsgSize(maxGrpcFrameSize),
		),
	}
	registerFunc(c.server)

	listener, err := net.Listen("tcp", bindAddress)
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
