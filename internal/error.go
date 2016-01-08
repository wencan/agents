package internal

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var (
	gerrVersionNotSupported = grpc.Errorf(codes.Aborted, "version not supported")
	gerrSessionLoss error = grpc.Errorf(codes.DataLoss, "session loss")
	gerrChannelLoss error = grpc.Errorf(codes.DataLoss, "channel loss")
	gerrSessionInvaild error = grpc.Errorf(codes.PermissionDenied, "session invaild")
	gerrChannelInvaild error = grpc.Errorf(codes.PermissionDenied, "channel invaild")
	gerrUnauthenticated error = grpc.Errorf(codes.PermissionDenied, "unauthenticated or authenticate fail")
	gerrHeartbeatTimeout error = grpc.Errorf(codes.DeadlineExceeded, "heartbeat timeout")
	gerrNetworkNotSupported = grpc.Errorf(codes.Canceled, "network type not supported")
	gerrSessonEnded error = grpc.Errorf(codes.Canceled, "session ended")
	gerrAckTimeout error = grpc.Errorf(codes.Canceled, "ack timeout")
	gerrOther = grpc.Errorf(codes.Canceled, "other...")
)