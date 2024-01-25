package utils

import (
	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/apache/pulsar-client-go/pulsaradmin"
	pulsar_utils "github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
)

// This function creates topics in Pulsar. It takes in a list of topics and creates them in pulsar.
// It assumes that the tenant and namespace already exist in Pulsar.
func CreateTopics(pulsarAdminURL string, tenant string, namespace string, topics []string) error {
	cfg := &pulsaradmin.Config{
		WebServiceURL: pulsarAdminURL,
	}
	admin, err := pulsaradmin.NewClient(cfg)
	if err != nil {
		log.Error("Failed to create pulsar admin client", zap.Error(err))
		return err
	}

	for _, topic := range topics {
		topicSchema := "persistent://" + tenant + "/" + namespace + "/" + topic
		topicName, err := pulsar_utils.GetTopicName(topicSchema)
		if err != nil {
			log.Error("Failed to get topic name", zap.Error(err))
			return err
		}
		metadata, err := admin.Topics().GetMetadata(*topicName)
		if err != nil {
			log.Info("Failed to get topic metadata, needs to create", zap.Error(err))
		} else {
			log.Info("Topic already exists", zap.String("topic", topic), zap.Any("metadata", metadata))
			continue
		}
		err = admin.Topics().Create(*topicName, 0)
		if err != nil {
			log.Error("Failed to create topic", zap.Error(err))
			return err
		}
	}
	return nil
}
