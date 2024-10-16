package grpc

import (
	"context"
	"errors"

	"github.com/chroma-core/chroma/go/pkg/grpcutils"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/sysdb/coordinator/model"
)

func (s *Server) CreateDatabase(ctx context.Context, req *coordinatorpb.CreateDatabaseRequest) (*coordinatorpb.CreateDatabaseResponse, error) {
	res := &coordinatorpb.CreateDatabaseResponse{}
	createDatabase := &model.CreateDatabase{
		ID:     req.GetId(),
		Name:   req.GetName(),
		Tenant: req.GetTenant(),
	}
	_, err := s.coordinator.CreateDatabase(ctx, createDatabase)
	if err != nil {
		if errors.Is(err, common.ErrDatabaseUniqueConstraintViolation) {
			res.Status = failResponseWithError(err, 409)
			return res, err
		}

		res.Status = failResponseWithError(err, errorCode)
		return res, nil
	}
	res.Status = setResponseStatus(successCode)
	return res, nil
}

func (s *Server) GetDatabase(ctx context.Context, req *coordinatorpb.GetDatabaseRequest) (*coordinatorpb.GetDatabaseResponse, error) {
	res := &coordinatorpb.GetDatabaseResponse{}
	getDatabase := &model.GetDatabase{
		Name:   req.GetName(),
		Tenant: req.GetTenant(),
	}
	database, err := s.coordinator.GetDatabase(ctx, getDatabase)
	if err != nil {
		if err == common.ErrDatabaseNotFound || err == common.ErrTenantNotFound {
			res.Status = failResponseWithError(err, 404)
			return res, nil
		}
		res.Status = failResponseWithError(err, errorCode)
	}
	res.Database = &coordinatorpb.Database{
		Id:     database.ID,
		Name:   database.Name,
		Tenant: database.Tenant,
	}
	res.Status = setResponseStatus(successCode)
	return res, nil
}

func (s *Server) CreateTenant(ctx context.Context, req *coordinatorpb.CreateTenantRequest) (*coordinatorpb.CreateTenantResponse, error) {
	res := &coordinatorpb.CreateTenantResponse{}
	createTenant := &model.CreateTenant{
		Name: req.GetName(),
	}
	_, err := s.coordinator.CreateTenant(ctx, createTenant)
	if err != nil {
		if err == common.ErrTenantUniqueConstraintViolation {
			res.Status = failResponseWithError(err, 409)
			return res, nil
		}
		res.Status = failResponseWithError(err, errorCode)
		return res, nil
	}
	res.Status = setResponseStatus(successCode)
	return res, nil
}

func (s *Server) GetTenant(ctx context.Context, req *coordinatorpb.GetTenantRequest) (*coordinatorpb.GetTenantResponse, error) {
	res := &coordinatorpb.GetTenantResponse{}
	getTenant := &model.GetTenant{
		Name: req.GetName(),
	}
	tenant, err := s.coordinator.GetTenant(ctx, getTenant)
	if err != nil {
		if err == common.ErrTenantNotFound {
			res.Status = failResponseWithError(err, 404)
			return res, nil
		}
		res.Status = failResponseWithError(err, errorCode)
		return res, nil
	}
	res.Tenant = &coordinatorpb.Tenant{
		Name: tenant.Name,
	}
	res.Status = setResponseStatus(successCode)
	return res, nil
}

func (s *Server) SetLastCompactionTimeForTenant(ctx context.Context, req *coordinatorpb.SetLastCompactionTimeForTenantRequest) (*emptypb.Empty, error) {
	err := s.coordinator.SetTenantLastCompactionTime(ctx, req.TenantLastCompactionTime.TenantId, req.TenantLastCompactionTime.LastCompactionTime)
	if err != nil {
		log.Error("error SetTenantLastCompactionTime", zap.Any("request", req.TenantLastCompactionTime), zap.Error(err))
		return nil, grpcutils.BuildInternalGrpcError(err.Error())
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) GetLastCompactionTimeForTenant(ctx context.Context, req *coordinatorpb.GetLastCompactionTimeForTenantRequest) (*coordinatorpb.GetLastCompactionTimeForTenantResponse, error) {
	res := &coordinatorpb.GetLastCompactionTimeForTenantResponse{}
	tenantIDs := req.TenantId
	tenants, err := s.coordinator.GetTenantsLastCompactionTime(ctx, tenantIDs)
	if err != nil {
		log.Error("error GetLastCompactionTimeForTenant", zap.Any("tenantIDs", tenantIDs), zap.Error(err))
		return nil, grpcutils.BuildInternalGrpcError(err.Error())
	}
	for _, tenant := range tenants {
		res.TenantLastCompactionTime = append(res.TenantLastCompactionTime, &coordinatorpb.TenantLastCompactionTime{
			TenantId:           tenant.ID,
			LastCompactionTime: tenant.LastCompactionTime,
		})
	}
	return res, nil
}
