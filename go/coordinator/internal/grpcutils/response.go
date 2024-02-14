package grpcutils

import "github.com/chroma/chroma-coordinator/internal/proto/coordinatorpb"

const ErrorCode = 500
const SuccessCode = 200
const success = "ok"

func FailResponseWithError(err error, code int32) *coordinatorpb.Status {
	return &coordinatorpb.Status{
		Reason: err.Error(),
		Code:   code,
	}
}

func SetResponseStatus(code int32) *coordinatorpb.Status {
	return &coordinatorpb.Status{
		Reason: success,
		Code:   code,
	}
}
