package internal

import (
	"testing"
	"google.golang.org/grpc"
)

func TestServer(t *testing.T) {
	srv := NewAgentServer(nil)
	opts := []grpc.ServerOption{}

	err := srv.ListenAndServe("tcp", ":8080", opts...)
	if err != nil {
		t.Error(err)
		return
	}
}