package main

import (
	"github.com/wencan/agents/codec"
	"github.com/wencan/agents/internal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"log"
)

func run_as_remote() {
	opts := []grpc.ServerOption{}

	if len(*certFile) > 0 {
		//https
		creds, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
		if err != nil {
			log.Fatalln(err)
		}
		opts = append(opts, grpc.Creds(creds))
	}

	//enable snappy
	c, err := codec.New("snappy")
	if err != nil {
		log.Fatalln(err)
	}
	var cc grpc.Codec
	cc, err = codec.WithProto(c)
	if err != nil {
		log.Fatalln(err)
	}
	opts = append(opts, grpc.CustomCodec(cc))

	//listen
	srv := internal.NewServer()
	err = srv.ListenAndServe("tcp", *listen, opts...)
	if err != nil {
		log.Fatalln(err)
	}
}
