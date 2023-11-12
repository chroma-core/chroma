package grpcutils

import "testing"

func TestGrpcConfig_TLSEnabled(t *testing.T) {
	// Create a list of configs and expected check result (true/false)
	cfgs := []*GrpcConfig{
		{
			CertPath: "cert",
			KeyPath:  "key",
			CAPath:   "ca",
		},
		{
			CertPath: "",
			KeyPath:  "",
			CAPath:   "",
		},
		{
			CertPath: "cert",
			KeyPath:  "",
			CAPath:   "ca",
		},
		{
			CertPath: "",
			KeyPath:  "key",
			CAPath:   "ca",
		},
	}
	expected := []bool{true, false, false, false}

	// Iterate through the list of configs and check if the result matches the expected result
	for i, cfg := range cfgs {
		if cfg.MTLSEnabled() != expected[i] {
			t.Errorf("Expected %v, got %v", expected[i], cfg.MTLSEnabled())
		}
	}
}
