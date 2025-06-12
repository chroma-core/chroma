package s3metastore

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	defaultMinioImage = "minio/minio:latest"
	defaultAccessKey  = "minioadmin"
	defaultSecretKey  = "minioadmin"
)

type MinioContainer struct {
	testcontainers.Container
	URI      string
	Port     string
	Username string
	Password string
}

func NewMinioContainer(ctx context.Context) (*MinioContainer, error) {
	req := testcontainers.ContainerRequest{
		Image:        defaultMinioImage,
		ExposedPorts: []string{"9000/tcp"},
		Env: map[string]string{
			"MINIO_ACCESS_KEY": defaultAccessKey,
			"MINIO_SECRET_KEY": defaultSecretKey,
		},
		Cmd: []string{"server", "/data"},
		WaitingFor: wait.ForAll(
			wait.ForLog("MinIO Object Storage Server"),
			wait.ForListeningPort("9000/tcp"),
		).WithDeadline(2 * time.Minute),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start container: %w", err)
	}

	mappedPort, err := container.MappedPort(ctx, "9000")
	if err != nil {
		return nil, fmt.Errorf("failed to get mapped port: %w", err)
	}

	hostIP, err := container.Host(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get host: %w", err)
	}

	uri := fmt.Sprintf("%s:%s", hostIP, mappedPort.Port())

	return &MinioContainer{
		Container: container,
		URI:       uri,
		Port:      mappedPort.Port(),
		Username:  defaultAccessKey,
		Password:  defaultSecretKey,
	}, nil
}

func NewS3MetaStoreWithContainer(ctx context.Context, bucketName, basePathSysDB string) (*S3MetaStore, *MinioContainer, error) {
	minioContainer, err := NewMinioContainer(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create minio container: %w", err)
	}

	s3Store, err := NewS3MetaStoreForTesting(ctx, bucketName, "us-east-1", basePathSysDB, minioContainer.URI, defaultAccessKey, defaultSecretKey)
	if err != nil {
		minioContainer.Terminate(ctx)
		return nil, nil, fmt.Errorf("failed to create s3 store: %w", err)
	}

	// Create bucket if it doesn't exist
	_, err = s3Store.S3.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
		CreateBucketConfiguration: &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint("us-east-1"),
		},
	})
	if err != nil {
		if !strings.Contains(err.Error(), "BucketAlreadyExists") &&
			!strings.Contains(err.Error(), "InvalidLocationConstraint") {
			minioContainer.Terminate(ctx)
			return nil, nil, fmt.Errorf("failed to create bucket: %w", err)
		}
	}

	return s3Store, minioContainer, nil
}
