package grpcutils

import (
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
