package grpc

import "testing"

func TestMemberlistManagerConfigsIncludesFnConsumer(t *testing.T) {
	config := Config{
		FnConsumerMemberlistName: "fn-consumer-memberlist",
		FnConsumerPodLabel:       "fn-consumer",
	}

	managers := memberlistManagerConfigs(config)
	if len(managers) != 5 {
		t.Fatalf("expected 5 memberlist managers, got %d", len(managers))
	}

	fnConsumer := managers[4]
	if fnConsumer.serviceType != "fn_consumer" {
		t.Fatalf("expected fn_consumer manager, got %q", fnConsumer.serviceType)
	}
	if fnConsumer.memberlistName != config.FnConsumerMemberlistName {
		t.Fatalf("expected memberlist %q, got %q", config.FnConsumerMemberlistName, fnConsumer.memberlistName)
	}
	if fnConsumer.podLabel != config.FnConsumerPodLabel {
		t.Fatalf("expected pod label %q, got %q", config.FnConsumerPodLabel, fnConsumer.podLabel)
	}
}
