package sysdb

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

type ISysDB interface {
	CheckCollection(ctx context.Context, collectionId string) (bool, error)
	AddCollection(ctx context.Context, collectionId string) error
}

type SysDB struct {
	client coordinatorpb.SysDBClient
}

func NewSysDB(conn string) *SysDB {
	backoffConfig := backoff.Config{
		BaseDelay:  1 * time.Second, // Initial delay before retrying
		Multiplier: 1.5,             // Factor to increase delay each retry
		MaxDelay:   5 * time.Second, // Maximum delay between retries
	}

	grpcClient, err := grpc.NewClient(conn,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoffConfig,
			MinConnectTimeout: 5 * time.Second,
		}),
	)

	if err != nil {
		log.Fatalf("Failed to connect to sysdb: %v", err)
	}

	client := coordinatorpb.NewSysDBClient(grpcClient)
	return &SysDB{
		client: client,
	}
}

func (s *SysDB) CheckCollection(ctx context.Context, collectionId string) (bool, error) {
	// TODO: make this check a batch API
	request := &coordinatorpb.GetCollectionsRequest{
		Id: &collectionId,
	}
	response, err := s.client.GetCollections(ctx, request)
	if err != nil {
		return false, err
	}
	for _, collection := range response.Collections {
		if collection.Id == collectionId {
			return true, nil
		}
	}
	return false, nil
}

func (s *SysDB) AddCollection(ctx context.Context, collectionId string) error {
	// TODO: We only use this for testing.
	panic(errors.New("unimplemented"))
}
