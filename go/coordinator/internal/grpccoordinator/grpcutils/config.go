package grpcutils

type GrpcConfig struct {
	// BindAddress is the address to bind the GRPC server to.
	BindAddress string

	// GRPC TLS config
	CertPath    string
	KeyPath     string
}

func (c *GrpcConfig) TLSEnabled() bool {
	return c.CertPath != "" && c.KeyPath != ""
}