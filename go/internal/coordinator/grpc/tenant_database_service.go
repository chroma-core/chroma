package grpc

import (
	"context"

	"github.com/chroma-core/chroma/go/internal/common"
	"github.com/chroma-core/chroma/go/internal/model"
	"github.com/chroma-core/chroma/go/internal/proto/coordinatorpb"
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
		if err == common.ErrDatabaseUniqueConstraintViolation {
			res.Status = failResponseWithError(err, 409)
			return res, nil
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
