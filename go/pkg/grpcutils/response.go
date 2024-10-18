package grpcutils

import (
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func BuildInvalidArgumentGrpcError(fieldName string, desc string) (error, error) {
	log.Info("InvalidArgument", zap.String("fieldName", fieldName), zap.String("desc", desc))
	st := status.New(codes.InvalidArgument, "invalid "+fieldName)
	v := &errdetails.BadRequest_FieldViolation{
		Field:       fieldName,
		Description: desc,
	}
	br := &errdetails.BadRequest{
		FieldViolations: []*errdetails.BadRequest_FieldViolation{v},
	}
	st, err := st.WithDetails(br)
	if err != nil {
		log.Error("Unexpected error attaching metadata", zap.Error(err))
		return nil, err
	}
	return st.Err(), nil
}

func BuildInternalGrpcError(msg string) error {
	return status.Error(codes.Internal, msg)
}

func BuildAlreadyExistsGrpcError(msg string) error {
	return status.Error(codes.AlreadyExists, msg)
}

func BuildNotFoundGrpcError(msg string) error {
	return status.Error(codes.NotFound, msg)
}

func BuildErrorForUUID(ID types.UniqueID, name string, err error) error {
	if err != nil || ID == types.NilUniqueID() {
		log.Error(name+"id format error", zap.String(name+".id", ID.String()))
		grpcError, err := BuildInvalidArgumentGrpcError(name+"_id", "wrong "+name+"_id format")
		if err != nil {
			log.Error("error building grpc error", zap.Error(err))
			return err
		}
		return grpcError
	}
	return nil
}
