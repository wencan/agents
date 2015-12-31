package internal

import (
	"../agent"
	"testing"
	"net"
	"google.golang.org/grpc"
)

func TestServer(t *testing.T) {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		t.Error(err)
		return
	}

	var opts []grpc.ServerOption

	grpcServer := grpc.NewServer(opts...)
	agent.RegisterAgentServer(grpcServer, NewAgentServer(nil))

	err = grpcServer.Serve(listener)
	if err != nil {
		t.Error(err)
	}
}