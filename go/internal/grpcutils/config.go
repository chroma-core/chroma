package grpcutils

type GrpcConfig struct {
	// BindAddress is the address to bind the GRPC server to.
	BindAddress string

	// GRPC mTLS config
	CertPath string
	KeyPath  string
	CAPath   string
}

func (c *GrpcConfig) MTLSEnabled() bool {
	return c.CertPath != "" && c.KeyPath != "" && c.CAPath != ""
}
