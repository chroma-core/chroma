package server

import (
	"context"
	"errors"

	"github.com/chroma-core/chroma/go/pkg/log/repository"
	"github.com/chroma-core/chroma/go/pkg/proto/logservicepb"
)

type logServer struct {
	logservicepb.UnimplementedLogServiceServer
	lr *repository.LogRepository
}

func (s *logServer) PushLogs(ctx context.Context, req *logservicepb.PushLogsRequest) (res *logservicepb.PushLogsResponse, err error) {
	return nil, errors.New("Go log service doesn't support PushLogs; migrated to Rust")
}

func (s *logServer) ScoutLogs(ctx context.Context, req *logservicepb.ScoutLogsRequest) (res *logservicepb.ScoutLogsResponse, err error) {
	return nil, errors.New("Go log service doesn't support ScoutLogs; migrated to Rust")
}

func (s *logServer) PullLogs(ctx context.Context, req *logservicepb.PullLogsRequest) (res *logservicepb.PullLogsResponse, err error) {
	return nil, errors.New("Go log service doesn't support PullLogs; migrated to Rust")
}

func (s *logServer) ForkLogs(ctx context.Context, req *logservicepb.ForkLogsRequest) (res *logservicepb.ForkLogsResponse, err error) {
	return nil, errors.New("Go log service doesn't support ForkLogs; migrated to Rust")
}

func (s *logServer) GetAllCollectionInfoToCompact(ctx context.Context, req *logservicepb.GetAllCollectionInfoToCompactRequest) (res *logservicepb.GetAllCollectionInfoToCompactResponse, err error) {
	return nil, nil
}

func (s *logServer) UpdateCollectionLogOffset(ctx context.Context, req *logservicepb.UpdateCollectionLogOffsetRequest) (res *logservicepb.UpdateCollectionLogOffsetResponse, err error) {
	return nil, errors.New("Go log service doesn't support UpdateCollectionLogOffset; migrated to Rust")
}

func (s *logServer) RollbackCollectionLogOffset(ctx context.Context, req *logservicepb.UpdateCollectionLogOffsetRequest) (res *logservicepb.UpdateCollectionLogOffsetResponse, err error) {
	return nil, errors.New("Go log service doesn't support RollbackCollectionLogOffset; migrated to Rust")
}

func (s *logServer) PurgeDirtyForCollection(ctx context.Context, req *logservicepb.PurgeDirtyForCollectionRequest) (res *logservicepb.PurgeDirtyForCollectionResponse, err error) {
	return nil, errors.New("Go log service doesn't support PurgeDirtyForCollection; migrated to Rust")
}

func (s *logServer) InspectDirtyLog(ctx context.Context, req *logservicepb.InspectDirtyLogRequest) (res *logservicepb.InspectDirtyLogResponse, err error) {
	return nil, errors.New("Go log service doesn't support InspectDirtyLog; migrated to Rust")
}

func (s *logServer) SealLog(ctx context.Context, req *logservicepb.SealLogRequest) (res *logservicepb.SealLogResponse, err error) {
	return nil, errors.New("Go log service doesn't support SealLog; migrated to Rust")
}

func (s *logServer) MigrateLog(ctx context.Context, req *logservicepb.MigrateLogRequest) (res *logservicepb.MigrateLogResponse, err error) {
	return nil, errors.New("Go log service doesn't support MigrateLog; migrated to Rust")
}

func (s *logServer) InspectLogState(ctx context.Context, req *logservicepb.InspectLogStateRequest) (res *logservicepb.InspectLogStateResponse, err error) {
	return nil, errors.New("Go log service doesn't support InspectLogState; migrated to Rust")
}

func (s *logServer) ScrubLog(ctx context.Context, req *logservicepb.ScrubLogRequest) (res *logservicepb.ScrubLogResponse, err error) {
	return nil, errors.New("Go log service doesn't support ScrubLog; migrated to Rust")
}

func (s *logServer) GarbageCollectPhase2(ctx context.Context, req *logservicepb.GarbageCollectPhase2Request) (res *logservicepb.GarbageCollectPhase2Response, err error) {
	return nil, errors.New("Go log service doesn't support GarbageCollectPhase2; migrated to Rust")
}

func (s *logServer) PurgeFromCache(ctx context.Context, req *logservicepb.PurgeFromCacheRequest) (res *logservicepb.PurgeFromCacheResponse, err error) {
	return nil, errors.New("Go log service doesn't support PurgeFromCache; migrated to Rust")
}

func NewLogServer(lr *repository.LogRepository) logservicepb.LogServiceServer {
	return &logServer{
		lr: lr,
	}
}
