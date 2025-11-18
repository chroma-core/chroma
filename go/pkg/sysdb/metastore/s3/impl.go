package s3metastore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/pingcap/log"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// NOTE(sicheng): As a temporary solution we use the AWS SDK with GCS, but this approach needs a few tweaks:
// https://stackoverflow.com/questions/73717477/gcp-cloud-storage-golang-aws-sdk2-upload-file-with-s3-interoperability-creds
//
// In summary, the AWS SDK we are using (1.36.3) is not fully compatible with the GCS because it uses and additional header
// for signing. We need to add a middleware to remove the offending header, as suggested by the thread above.
//
// If we are upgrading AWS SDK to version higher than 1.73.0, we need additional tweak:
// cfg.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
type RecalculateV4Signature struct {
	next   http.RoundTripper
	signer *v4.Signer
	cfg    aws.Config
}

// NOTE(sicheng): Code borrowed from https://stackoverflow.com/questions/73717477/gcp-cloud-storage-golang-aws-sdk2-upload-file-with-s3-interoperability-creds
func (lt *RecalculateV4Signature) RoundTrip(req *http.Request) (*http.Response, error) {
	// store for later use
	val := req.Header.Get("Accept-Encoding")

	// delete the header so the header doesn't account for in the signature
	req.Header.Del("Accept-Encoding")

	// sign with the same date
	timeString := req.Header.Get("X-Amz-Date")
	timeDate, _ := time.Parse("20060102T150405Z", timeString)

	creds, _ := lt.cfg.Credentials.Retrieve(req.Context())
	err := lt.signer.SignHTTP(req.Context(), creds, req, v4.GetPayloadHash(req.Context()), "s3", lt.cfg.Region, timeDate)
	if err != nil {
		return nil, err
	}
	// Reset Accept-Encoding if desired
	req.Header.Set("Accept-Encoding", val)

	fmt.Println("AfterAdjustment")
	rrr, _ := httputil.DumpRequest(req, false)
	fmt.Println(string(rrr))

	// follows up the original round tripper
	return lt.next.RoundTrip(req)
}

// Path to Version Files in S3.
// Example:
// s3://<bucket-name>/tenant/<tenant_id>/databases/<database_id>/collections/<collection_id>/versionfiles/file_name
const (
	lineageFilesPathFormat = "tenant/%s/database/%s/collection/%s/lineagefiles/%s"
	versionFilesPathFormat = "tenant/%s/database/%s/collection/%s/versionfiles/%s"
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
	GCSInterop              bool
}

type S3MetaStoreInterface interface {
	GetLineageFile(ctx context.Context, lineageFileName string) (*coordinatorpb.CollectionLineageFile, error)
	PutLineageFile(ctx context.Context, tenantID, databaseID, collectionID, fileName string, file *coordinatorpb.CollectionLineageFile) (string, error)
	GetVersionFile(ctx context.Context, versionFileName string) (*coordinatorpb.CollectionVersionFile, error)
	PutVersionFile(ctx context.Context, tenantID, databaseID, collectionID, fileName string, file *coordinatorpb.CollectionVersionFile) (string, error)
	HasObjectWithPrefix(ctx context.Context, prefix string) (bool, error)
	DeleteVersionFile(ctx context.Context, tenantID, databaseID, collectionID, fileName string) error
}

// S3MetaStore wraps the S3 connection and related parameters for the metadata store.
type S3MetaStore struct {
	S3            *s3.Client
	BucketName    string
	Region        string
	BasePathSysDB string
}

func NewS3MetaStoreForTesting(ctx context.Context, bucketName, region, basePathSysDB, endpoint, accessKey, secretKey string) (*S3MetaStore, error) {
	// Configure AWS config for MinIO
	creds := credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")

	// Ensure endpoint has protocol scheme
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		endpoint = "http://" + endpoint
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(creds),
		config.WithRegion(region),
	)
	if err != nil {
		return nil, err
	}

	// Configure S3 client with path-style addressing and custom endpoint for MinIO
	otelaws.AppendMiddlewares(&cfg.APIOptions)
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(endpoint)
	})

	return &S3MetaStore{
		S3:            s3Client,
		BucketName:    bucketName,
		Region:        region,
		BasePathSysDB: basePathSysDB,
	}, nil
}

// NewS3MetaStore constructs and returns an S3MetaStore.
func NewS3MetaStore(ctx context.Context, cfg S3MetaStoreConfig) (*S3MetaStore, error) {
	bucketName := cfg.BucketName

	// Set up AWS config
	region := "us-east-1"
	if cfg.Region != "" {
		region = cfg.Region
	}

	var awsConfig aws.Config
	var err error

	awsConfigParts := []func(*config.LoadOptions) error{config.WithRegion(region)}

	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		creds := credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, "")
		awsConfigParts = append(awsConfigParts, config.WithCredentialsProvider(creds))
	}

	if cfg.GCSInterop && cfg.Endpoint != "" {
		resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...any) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL:               cfg.Endpoint,
				SigningRegion:     cfg.Region,
				Source:            aws.EndpointSourceCustom,
				HostnameImmutable: true,
			}, nil
		})
		awsConfigParts = append(awsConfigParts, config.WithEndpointResolverWithOptions(resolver))
	}

	awsConfig, err = config.LoadDefaultConfig(ctx, awsConfigParts...)
	if err != nil {
		return nil, err
	}

	// Add middleware to remove offending header for signing for GCS Support
	if cfg.GCSInterop {
		awsConfig.HTTPClient = &http.Client{Transport: &RecalculateV4Signature{http.DefaultTransport, v4.NewSigner(), awsConfig}}
	}

	// Create S3 client with optional path-style addressing and custom endpoint
	otelaws.AppendMiddlewares(&awsConfig.APIOptions)
	s3Client := s3.NewFromConfig(awsConfig, func(o *s3.Options) {
		o.UsePathStyle = cfg.ForcePathStyle
		if cfg.Endpoint != "" {
			// Ensure endpoint has protocol scheme
			endpoint := cfg.Endpoint
			if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
				endpoint = "http://" + endpoint
			}
			o.BaseEndpoint = aws.String(endpoint)
		}
	})

	if cfg.CreateBucketIfNotExists {
		_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			var bucketAlreadyOwnedByYou *types.BucketAlreadyOwnedByYou
			var bucketAlreadyExists *types.BucketAlreadyExists
			if !errors.As(err, &bucketAlreadyOwnedByYou) && !errors.As(err, &bucketAlreadyExists) {
				return nil, fmt.Errorf("unable to create bucket %s: %w", bucketName, err)
			}
			log.Info("Bucket already exists, continuing", zap.String("bucket", bucketName))
		}
	}

	// Verify we have access to the bucket
	_, err = s3Client.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to access bucket %s: %w", bucketName, err)
	}

	return &S3MetaStore{
		S3:            s3Client,
		BucketName:    bucketName,
		Region:        cfg.Region,
		BasePathSysDB: cfg.BasePathSysDB,
	}, nil
}

func (store *S3MetaStore) GetLineageFile(ctx context.Context, lineageFileName string) (*coordinatorpb.CollectionLineageFile, error) {
	log.Info("Getting lineage file from S3", zap.String("path", lineageFileName))

	input := &s3.GetObjectInput{
		Bucket: aws.String(store.BucketName),
		Key:    aws.String(lineageFileName),
	}

	result, err := store.S3.GetObject(ctx, input)
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

func (store *S3MetaStore) PutLineageFile(ctx context.Context, tenantID string, databaseID string, collectionID string, lineageFileName string, lineageFile *coordinatorpb.CollectionLineageFile) (string, error) {
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

	output, err := store.S3.PutObject(ctx, input)
	log.Info("Put object output", zap.Any("output", output), zap.Error(err))

	return path, err
}

// Get the version file from S3. Return the protobuf.
func (store *S3MetaStore) GetVersionFile(ctx context.Context, versionFileName string) (*coordinatorpb.CollectionVersionFile, error) {
	log.Info("getting version file from S3", zap.String("path", versionFileName))

	input := &s3.GetObjectInput{
		Bucket: aws.String(store.BucketName),
		Key:    aws.String(versionFileName),
	}

	result, err := store.S3.GetObject(ctx, input)
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
func (store *S3MetaStore) PutVersionFile(ctx context.Context, tenantID, databaseID, collectionID string, versionFileName string, versionFile *coordinatorpb.CollectionVersionFile) (string, error) {
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

	output, err := store.S3.PutObject(ctx, input)
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

func (store *S3MetaStore) DeleteVersionFile(ctx context.Context, tenantID, databaseID, collectionID, fileName string) error {
	path := store.GetVersionFilePath(tenantID, databaseID, collectionID, fileName)

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(store.BucketName),
		Key:    aws.String(path),
	}

	_, err := store.S3.DeleteObject(ctx, input)
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
	result, err := store.S3.ListObjectsV2(ctx, input)
	if err != nil {
		log.Error("error listing objects with prefix", zap.Error(err))
		return false, err
	}

	log.Info("listing objects with prefix result", zap.Any("result", result))
	return len(result.Contents) > 0, nil
}
