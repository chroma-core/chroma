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

// Path to Version Files in S3.
// Example:
// s3://<bucket-name>/<sysdbPathPrefix>/<tenant_id>/collections/<collection_id>/versionfiles/file_name
const (
	versionFilesPathFormat = "%s/%s/collections/%s/versionfiles/%s"
	minioEndpoint          = "minio:9000"
	minioAccessKeyID       = "minio"
	minioSecretAccessKey   = "minio123"
)

// BlockStoreProviderType represents the type of block store provider
type BlockStoreProviderType string

const (
	BlockStoreProviderNone  BlockStoreProviderType = "none"
	BlockStoreProviderS3    BlockStoreProviderType = "s3"
	BlockStoreProviderMinio BlockStoreProviderType = "minio"
)

func (t BlockStoreProviderType) IsValid() bool {
	return t == BlockStoreProviderS3 || t == BlockStoreProviderMinio || t == BlockStoreProviderNone
}

type S3MetaStoreConfig struct {
	BucketName         string
	Region             string
	BasePathSysDB      string
	Endpoint           string
	BlockStoreProvider BlockStoreProviderType
}

type S3MetaStoreInterface interface {
	GetVersionFile(tenantID, collectionID string, version int64, fileName string) (*coordinatorpb.CollectionVersionFile, error)
	PutVersionFile(tenantID, collectionID, fileName string, file *coordinatorpb.CollectionVersionFile) (string, error)
	HasObjectWithPrefix(ctx context.Context, prefix string) (bool, error)
	DeleteVersionFile(tenantID, collectionID, fileName string) error
}

// S3MetaStore wraps the S3 connection and related parameters for the metadata store.
type S3MetaStore struct {
	S3            *s3.S3
	BucketName    string
	Region        string
	BasePathSysDB string
}

func NewS3MetaStoreForTesting(bucketName, region, basePathSysDB, endpoint, accessKey, secretKey string) (*S3MetaStore, error) {
	// Configure AWS session for MinIO
	creds := credentials.NewStaticCredentials(accessKey, secretKey, "")
	sess, err := session.NewSession(&aws.Config{
		Credentials:      creds,
		Endpoint:         aws.String(endpoint),
		Region:           aws.String(region),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
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
func NewS3MetaStore(config S3MetaStoreConfig) (*S3MetaStore, error) {
	var sess *session.Session
	var err error
	bucketName := config.BucketName

	if config.BlockStoreProvider == BlockStoreProviderNone {
		// TODO(rohit): Remove this once the feature is enabled.
		// This is valid till the feature is not enabled.
		return nil, nil
	}

	if config.BlockStoreProvider == BlockStoreProviderMinio {
		sess, err = session.NewSession(&aws.Config{
			Credentials:      credentials.NewStaticCredentials(minioAccessKeyID, minioSecretAccessKey, ""),
			Endpoint:         aws.String(minioEndpoint),
			Region:           aws.String("us-east-1"),
			DisableSSL:       aws.Bool(true),
			S3ForcePathStyle: aws.Bool(true),
		})
		bucketName = "chroma-storage"
	} else {
		sess, err = session.NewSession(&aws.Config{
			Region: aws.String(config.Region),
		})
	}

	if err != nil {
		return nil, err
	}

	return &S3MetaStore{
		S3:            s3.New(sess),
		BucketName:    bucketName,
		Region:        config.Region,
		BasePathSysDB: config.BasePathSysDB,
	}, nil
}

// Get the version file from S3. Return the protobuf.
func (store *S3MetaStore) GetVersionFile(tenantID, collectionID string, version int64, versionFileName string) (*coordinatorpb.CollectionVersionFile, error) {
	// path := store.GetVersionFilePath(tenantID, collectionID, versionFileName)
	path := versionFileName
	log.Info("getting version file from S3", zap.String("path", path))

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

	numVersions := len(versionFile.VersionHistory.Versions)
	lastVersion := versionFile.VersionHistory.Versions[numVersions-1]
	lastVersionSegmentInfo := lastVersion.GetSegmentInfo()
	if lastVersionSegmentInfo == nil {
		log.Info("Last version segment info is nil")
	} else {
		lastVersionSegmentCompactionInfo := lastVersionSegmentInfo.SegmentCompactionInfo
		log.Info("Last version segment compaction info", zap.Any("lastVersionSegmentCompactionInfo", lastVersionSegmentCompactionInfo))
	}

	return versionFile, nil
}

// Put the version file to S3. Serialize the protobuf to bytes.
func (store *S3MetaStore) PutVersionFile(tenantID, collectionID string, versionFileName string, versionFile *coordinatorpb.CollectionVersionFile) (string, error) {
	path := store.GetVersionFilePath(tenantID, collectionID, versionFileName)

	data, err := proto.Marshal(versionFile)
	if err != nil {
		return "", fmt.Errorf("failed to marshal version file: %w", err)
	}

	log.Info("putting version file", zap.String("path", path))
	numVersions := len(versionFile.VersionHistory.Versions)
	lastVersion := versionFile.VersionHistory.Versions[numVersions-1]
	lastVersionSegmentInfo := lastVersion.GetSegmentInfo()
	if lastVersionSegmentInfo == nil {
		log.Info("Current version segment info is nil")
	} else {
		lastVersionSegmentCompactionInfo := lastVersionSegmentInfo.SegmentCompactionInfo
		log.Info("Current version segment compaction info", zap.Any("lastVersionSegmentCompactionInfo", lastVersionSegmentCompactionInfo))
	}

	input := &s3.PutObjectInput{
		Bucket: aws.String(store.BucketName),
		Key:    aws.String(path),
		Body:   bytes.NewReader(data),
	}

	output, err := store.S3.PutObject(input)
	log.Info("put object output", zap.Any("output", output), zap.Error(err))
	return path, err
}

// GetVersionFilePath constructs the S3 path for a version file
func (store *S3MetaStore) GetVersionFilePath(tenantID, collectionID, versionFileName string) string {
	return fmt.Sprintf(versionFilesPathFormat,
		store.BasePathSysDB, tenantID, collectionID, versionFileName)
}

// DeleteOldVersionFiles removes version files older than the specified version
func (store *S3MetaStore) DeleteOldVersionFiles(tenantID, collectionID string, olderThanVersion int64) error {
	// TODO: Implement this
	return errors.New("not implemented")
}

func (store *S3MetaStore) DeleteVersionFile(tenantID, collectionID, fileName string) error {
	path := store.GetVersionFilePath(tenantID, collectionID, fileName)

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(store.BucketName),
		Key:    aws.String(path),
	}

	_, err := store.S3.DeleteObject(input)
	if err != nil {
		return err
	}
	return nil
}

func (store *S3MetaStore) HasObjectWithPrefix(ctx context.Context, prefix string) (bool, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(store.BucketName),
		Prefix: aws.String(prefix),
	}

	log.Info("listing objects with prefix", zap.String("prefix", prefix))
	result, err := store.S3.ListObjectsV2(input)
	if err != nil {
		log.Error("error listing objects with prefix", zap.Error(err))
		return false, err
	}

	log.Info("listing objects with prefix result", zap.Any("result", result))
	return len(result.Contents) > 0, nil
}
