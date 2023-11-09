package grpcutils

import "testing"

func TestGrpcConfig_TLSEnabled(t *testing.T) {
	// Create a list of configs and expected check result (true/false)
	cfgs := []*GrpcConfig{
		{
			CertPath: "cert",
			KeyPath:  "key",
		},
		{
			CertPath: "",
			KeyPath:  "",
		},
		{
			CertPath: "cert",
			KeyPath:  "",
		},
		{
			CertPath: "",
			KeyPath:  "key",
		},
	}
	expected := []bool{true, false, false, false}

	// Iterate through the list of configs and check if the result matches the expected result
	for i, cfg := range cfgs {
		if cfg.TLSEnabled() != expected[i] {
			t.Errorf("Expected %v, got %v", expected[i], cfg.TLSEnabled())
		}
	}
}
