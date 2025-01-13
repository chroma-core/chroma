package s3metastore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const (
	versionFilesPathFormat = "%s/%s/collections/%s/versionfiles/"
)

// Add this at the top of the file, after the package declaration
type S3MetaStoreInterface interface {
	GetVersionFile(tenantID, collectionID string, version int64, fileName string) (*coordinatorpb.CollectionVersionFile, error)
	PutVersionFile(tenantID, collectionID, fileName string, file *coordinatorpb.CollectionVersionFile) error
	HasObjectWithPrefix(ctx context.Context, prefix string) (bool, error)
}

// S3MetaStore wraps the S3 connection and related parameters for the metadata store.
type S3MetaStore struct {
	S3            *s3.S3
	BucketName    string
	Region        string
	BasePathSysDB string
}

func NewS3MetaStoreForTesting(bucketName, region, basePathSysDB string) (*S3MetaStore, error) {
	// Configure AWS session for MinIO
	creds := credentials.NewStaticCredentials("minio", "minio123", "")
	sess, err := session.NewSession(&aws.Config{
		Credentials:      creds,
		Endpoint:         aws.String("localhost:9000"), // Default MinIO endpoint
		Region:           aws.String(region),
		DisableSSL:       aws.Bool(true), // Disable SSL for local testing
		S3ForcePathStyle: aws.Bool(true), // Required for MinIO
	})
	if err != nil {
		return nil, err
	}

	return &S3MetaStore{
		S3:            s3.New(sess),
		BucketName:    bucketName,
		Region:        region,
		BasePathSysDB: basePathSysDB,
	}, nil
}

// NewS3MetaStore constructs and returns an S3MetaStore.
func NewS3MetaStore(bucketName, region, basePathSysDB string) (*S3MetaStore, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		return nil, err
	}

	return &S3MetaStore{
		S3:            s3.New(sess),
		BucketName:    bucketName,
		Region:        region,
		BasePathSysDB: basePathSysDB,
	}, nil
}

// TODO: Get the version file from S3. Return the protobuf.
func (store *S3MetaStore) GetVersionFile(tenantID, collectionID string, version int64, versionFileName string) (*coordinatorpb.CollectionVersionFile, error) {
	path := fmt.Sprintf("%s/%s",
		store.GetVersionFilePath(tenantID, collectionID),
		versionFileName)

	input := &s3.GetObjectInput{
		Bucket: aws.String(store.BucketName),
		Key:    aws.String(path),
	}

	result, err := store.S3.GetObject(input)
	if err != nil {
		return nil, err
	}
	defer result.Body.Close()

	data, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, err
	}

	versionFile := &coordinatorpb.CollectionVersionFile{}
	if err := proto.Unmarshal(data, versionFile); err != nil {
		return nil, fmt.Errorf("failed to unmarshal version file: %w", err)
	}

	return versionFile, nil
}

// TODO: Put the version file to S3. Serialize the protobuf to bytes.
func (store *S3MetaStore) PutVersionFile(tenantID, collectionID string, versionFileName string, versionFile *coordinatorpb.CollectionVersionFile) error {
	path := fmt.Sprintf("%s/%s",
		store.GetVersionFilePath(tenantID, collectionID),
		versionFileName)

	data, err := proto.Marshal(versionFile)
	if err != nil {
		return fmt.Errorf("failed to marshal version file: %w", err)
	}

	input := &s3.PutObjectInput{
		Bucket: aws.String(store.BucketName),
		Key:    aws.String(path),
		Body:   bytes.NewReader(data),
	}

	_, err = store.S3.PutObject(input)
	return err
}

// GetVersionFilePath constructs the S3 path for a version file
func (store *S3MetaStore) GetVersionFilePath(tenantID, collectionID string) string {
	return fmt.Sprintf(versionFilesPathFormat,
		store.BasePathSysDB, tenantID, collectionID)
}

// DeleteOldVersionFiles removes version files older than the specified version
func (store *S3MetaStore) DeleteOldVersionFiles(tenantID, collectionID string, olderThanVersion int64) error {
	// TODO: Implement this
	return errors.New("not implemented")
}

func (store *S3MetaStore) HasObjectWithPrefix(ctx context.Context, prefix string) (bool, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(store.BucketName),
		Prefix: aws.String(prefix),
	}

	log.Info("listing objects with prefix", zap.String("prefix", prefix))
	result, err := store.S3.ListObjectsV2(input)
	if err != nil {
		return false, err
	}

	return len(result.Contents) > 0, nil
}
