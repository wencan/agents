package internal

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var (
	ErrVersionNotSupported = grpc.Errorf(codes.Aborted, "version not supported")
	ErrSessionLoss error = grpc.Errorf(codes.InvalidArgument, "session loss")
	ErrChannelLoss error = grpc.Errorf(codes.InvalidArgument, "channel loss")
	ErrSessionInvaild error = grpc.Errorf(codes.PermissionDenied, "session invaild")
	ErrChannelInvaild error = grpc.Errorf(codes.PermissionDenied, "channel invaild")
	ErrAckTimeout error = grpc.Errorf(codes.Canceled, "ack timeout")
)