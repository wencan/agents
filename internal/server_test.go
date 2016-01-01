package internal

import (
	"../codec"
	"testing"
	"google.golang.org/grpc"
)

func TestServer(t *testing.T) {
	srv := NewAgentServer(nil)
	opts := []grpc.ServerOption{}

	//enable snappy decompress
	if c, err := codec.New("snappy"); err != nil {
		t.Error(err)
		return
	} else {
		if cc, err := codec.WithProto(c); err != nil {
			t.Error(err)
			return
		} else {
			opts = append(opts, grpc.CustomCodec(cc))
		}
	}

	err := srv.ListenAndServe("tcp", ":8080", opts...)
	if err != nil {
		t.Error(err)
		return
	}
}