package internal

import (
	"../codec"
	"testing"
	"google.golang.org/grpc"
	"net"
	"io"
	"log"
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

func TestEcho(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:8888")
	if err != nil {
		t.Error(err)
		return
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			t.Error(err)
		}

		log.Println("New connection form:", conn.RemoteAddr().String())
		go func() {
			_, err := io.Copy(conn, conn)
			if err != nil {
				log.Println(err)
			}
		}()
	}
}