package grpcutils

type GrpcConfig struct {
	// BindAddress is the address to bind the GRPC server to.
	BindAddress string

	MaxConcurrentStreams uint32
	NumStreamWorkers     uint32

	// GRPC mTLS config
	CertPath string
	KeyPath  string
	CAPath   string
}

func (c *GrpcConfig) MTLSEnabled() bool {
	return c.CertPath != "" && c.KeyPath != "" && c.CAPath != ""
}
