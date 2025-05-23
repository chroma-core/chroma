package s3metastore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
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
// s3://<bucket-name>/<sysdbPathPrefix>/<tenant_id>/databases/<database_id>/collections/<collection_id>/versionfiles/file_name
const (
	lineageFilesPathFormat = "%s/databases/%s/collections/%s/lineagefiles/%s"
	versionFilesPathFormat = "%s/databases/%s/collections/%s/versionfiles/%s"
)

type S3MetaStoreConfig struct {
	CreateBucketIfNotExists bool
	BucketName              string
	Region                  string
	BasePathSysDB           string
	Endpoint                string
	AccessKeyID             string
	SecretAccessKey         string
	ForcePathStyle          bool
}

type S3MetaStoreInterface interface {
	GetLineageFile(lineageFileName string) (*coordinatorpb.CollectionLineageFile, error)
	PutLineageFile(tenantID, databaseID, collectionID, fileName string, file *coordinatorpb.CollectionLineageFile) (string, error)
	GetVersionFile(versionFileName string) (*coordinatorpb.CollectionVersionFile, error)
	PutVersionFile(tenantID, databaseID, collectionID, fileName string, file *coordinatorpb.CollectionVersionFile) (string, error)
	HasObjectWithPrefix(ctx context.Context, prefix string) (bool, error)
	DeleteVersionFile(tenantID, databaseID, collectionID, fileName string) error
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
	bucketName := config.BucketName

	aws_config := aws.Config{
		Region:           aws.String("us-east-1"),
		S3ForcePathStyle: aws.Bool(config.ForcePathStyle),
	}

	if config.Region != "" {
		aws_config.Region = aws.String(config.Region)
	}
	if config.Endpoint != "" {
		aws_config.Endpoint = aws.String(config.Endpoint)
	}
	if config.AccessKeyID != "" && config.SecretAccessKey != "" {
		aws_config.Credentials = credentials.NewStaticCredentials(config.AccessKeyID, config.SecretAccessKey, "")
	}

	sess, err := session.NewSession(&aws_config)
	if err != nil {
		return nil, err
	}

	s3Client := s3.New(sess)

	if config.CreateBucketIfNotExists {
		_, err = s3Client.CreateBucket(&s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			if err.(awserr.Error).Code() != s3.ErrCodeBucketAlreadyOwnedByYou {
				return nil, fmt.Errorf("unable to create bucket %s: %w", bucketName, err)
			}
			log.Info("Bucket already exists, continuing", zap.String("bucket", bucketName))
		}
	}

	// Verify we have access to the bucket
	_, err = s3Client.HeadBucket(&s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to access bucket %s: %w", bucketName, err)
	}

	return &S3MetaStore{
		S3:            s3Client,
		BucketName:    bucketName,
		Region:        config.Region,
		BasePathSysDB: config.BasePathSysDB,
	}, nil
}

func (store *S3MetaStore) GetLineageFile(lineageFileName string) (*coordinatorpb.CollectionLineageFile, error) {
	log.Info("Getting lineage file from S3", zap.String("path", lineageFileName))

	input := &s3.GetObjectInput{
		Bucket: aws.String(store.BucketName),
		Key:    aws.String(lineageFileName),
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

	lineageFile := &coordinatorpb.CollectionLineageFile{}
	if err := proto.Unmarshal(data, lineageFile); err != nil {
		return nil, fmt.Errorf("failed to unmarshal version file: %w", err)
	}

	return lineageFile, nil
}

func (store *S3MetaStore) GetLineageFilePath(tenantID string, databaseID string, collectionID string, versionFileName string) string {
	return fmt.Sprintf(lineageFilesPathFormat,
		tenantID, databaseID, collectionID, versionFileName)
}

func (store *S3MetaStore) PutLineageFile(tenantID string, databaseID string, collectionID string, lineageFileName string, lineageFile *coordinatorpb.CollectionLineageFile) (string, error) {
	path := store.GetLineageFilePath(tenantID, databaseID, collectionID, lineageFileName)

	data, err := proto.Marshal(lineageFile)
	if err != nil {
		return "", fmt.Errorf("Failed to marshal lineage file: %w", err)
	}

	numForks := len(lineageFile.Dependencies)
	log.Info("Putting lineage file", zap.String("collectionID", collectionID), zap.Int("numForks", numForks))

	input := &s3.PutObjectInput{
		Bucket: aws.String(store.BucketName),
		Key:    aws.String(path),
		Body:   bytes.NewReader(data),
	}

	output, err := store.S3.PutObject(input)
	log.Info("Put object output", zap.Any("output", output), zap.Error(err))

	return path, err
}

// Get the version file from S3. Return the protobuf.
func (store *S3MetaStore) GetVersionFile(versionFileName string) (*coordinatorpb.CollectionVersionFile, error) {
	log.Info("getting version file from S3", zap.String("path", versionFileName))

	input := &s3.GetObjectInput{
		Bucket: aws.String(store.BucketName),
		Key:    aws.String(versionFileName),
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
	if numVersions > 0 {
		lastVersion := versionFile.VersionHistory.Versions[numVersions-1]
		lastVersionSegmentInfo := lastVersion.GetSegmentInfo()
		if lastVersionSegmentInfo == nil {
			log.Info("Last version segment info is nil")
		} else {
			lastVersionSegmentCompactionInfo := lastVersionSegmentInfo.SegmentCompactionInfo
			log.Info("Last version segment compaction info", zap.Any("lastVersionSegmentCompactionInfo", lastVersionSegmentCompactionInfo))
		}
	}

	return versionFile, nil
}

// Put the version file to S3. Serialize the protobuf to bytes.
func (store *S3MetaStore) PutVersionFile(tenantID, databaseID, collectionID string, versionFileName string, versionFile *coordinatorpb.CollectionVersionFile) (string, error) {
	path := store.GetVersionFilePath(tenantID, databaseID, collectionID, versionFileName)

	data, err := proto.Marshal(versionFile)
	if err != nil {
		return "", fmt.Errorf("failed to marshal version file: %w", err)
	}

	log.Info("putting version file", zap.String("path", path))
	numVersions := len(versionFile.VersionHistory.Versions)
	if numVersions > 0 {
		lastVersion := versionFile.VersionHistory.Versions[numVersions-1]
		lastVersionSegmentInfo := lastVersion.GetSegmentInfo()
		if lastVersionSegmentInfo == nil {
			log.Info("Current version segment info is nil")
		} else {
			lastVersionSegmentCompactionInfo := lastVersionSegmentInfo.SegmentCompactionInfo
			log.Info("Current version segment compaction info", zap.Any("lastVersionSegmentCompactionInfo", lastVersionSegmentCompactionInfo))
		}
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
func (store *S3MetaStore) GetVersionFilePath(tenantID, databaseID, collectionID, versionFileName string) string {
	return fmt.Sprintf(versionFilesPathFormat,
		tenantID, databaseID, collectionID, versionFileName)
}

// DeleteOldVersionFiles removes version files older than the specified version
func (store *S3MetaStore) DeleteOldVersionFiles(tenantID, collectionID string, olderThanVersion int64) error {
	// TODO: Implement this
	return errors.New("not implemented")
}

func (store *S3MetaStore) DeleteVersionFile(tenantID, databaseID, collectionID, fileName string) error {
	path := store.GetVersionFilePath(tenantID, databaseID, collectionID, fileName)

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
