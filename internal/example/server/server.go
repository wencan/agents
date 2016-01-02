package main

import (
	"../../../codec"
	"../../../internal"
	"google.golang.org/grpc"
	"log"
)

func main() {
	srv := internal.NewAgentServer(nil)
	opts := []grpc.ServerOption{}

	//enable snappy decompress
	if c, err := codec.New("snappy"); err != nil {
		log.Fatalln(err)
	} else {
		if cc, err := codec.WithProto(c); err != nil {
			log.Fatalln(err)
		} else {
			opts = append(opts, grpc.CustomCodec(cc))
		}
	}

	err := srv.ListenAndServe("tcp", ":8080", opts...)
	if err != nil {
		log.Fatalln(err)
	}
}
