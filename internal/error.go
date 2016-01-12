package internal

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var (
	ErrVersionNotSupported = grpc.Errorf(codes.Aborted, "version not supported")
	ErrArgsInvaild = grpc.Errorf(codes.InvalidArgument, "arguments invaild")
	ErrSessionLoss error = grpc.Errorf(codes.InvalidArgument, "session loss")
	ErrChannelLoss error = grpc.Errorf(codes.InvalidArgument, "channel loss")
	ErrSessionInvaild error = grpc.Errorf(codes.PermissionDenied, "session invaild")
	ErrChannelInvaild error = grpc.Errorf(codes.PermissionDenied, "channel invaild")
	ErrUnauthenticated error = grpc.Errorf(codes.Unauthenticated, "unauthenticated or authenticate fail")
	ErrAckTimeout error = grpc.Errorf(codes.Canceled, "ack timeout")
)