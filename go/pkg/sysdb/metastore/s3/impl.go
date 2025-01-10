package s3metastore

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"google.golang.org/protobuf/proto"
)

const (
	versionFilesPathFormat = "%s/%s/collections/%s/versionfiles/%d"
)

// S3MetaStore wraps the S3 connection and related parameters for the metadata store.
type S3MetaStore struct {
	S3             *s3.S3
	BucketName     string
	Region         string
	BasePathBlocks string
	BasePathSysDB  string
}

// NewS3MetaStore constructs and returns an S3MetaStore.
func NewS3MetaStore(bucketName, region, basePathBlocks, basePathSysDB string) (*S3MetaStore, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		return nil, err
	}

	return &S3MetaStore{
		S3:             s3.New(sess),
		BucketName:     bucketName,
		Region:         region,
		BasePathBlocks: basePathBlocks,
		BasePathSysDB:  basePathSysDB,
	}, nil
}

// TODO: Get the version file from S3. Return the protobuf.
func (store *S3MetaStore) GetVersionFile(tenantID, collectionID string, version int64, versionFileName string) (*coordinatorpb.CollectionVersionFile, error) {
	path := fmt.Sprintf("%s/%s",
		store.GetVersionFilePath(tenantID, collectionID, version),
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
		store.GetVersionFilePath(tenantID, collectionID, versionFile.Version),
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
func (store *S3MetaStore) GetVersionFilePath(tenantID, collectionID string, version int64) string {
	return fmt.Sprintf(versionFilesPathFormat,
		store.BasePathSysDB, tenantID, collectionID, version)
}

// DeleteOldVersionFiles removes version files older than the specified version
func (store *S3MetaStore) DeleteOldVersionFiles(tenantID, collectionID string, olderThanVersion int64) error {
	// TODO: Implement this
	return errors.New("not implemented")
}
