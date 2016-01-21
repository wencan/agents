package main

import (
	"./internal"
	"./codec"
	"./utils"
	"./socks/socks5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"log"
	"net"
)

const (
	clientPoolSize int = 10
)

func newClient() (x interface{}){
	opts := []grpc.DialOption{}

	if len(*certFile) > 0 {
		//https
		creds, err := credentials.NewClientTLSFromFile(*certFile, *server)
		if err != nil {
			log.Fatalln(err)
		}
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithInsecure())
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
	opts = append(opts, grpc.WithCodec(cc))

	var client *internal.Client
	client, err = internal.NewClient(*server, nil, opts...)
	if err != nil {
		log.Fatalln(err)
	}
	return client
}

func deleteClient(x interface{}) {
	client := x.(*internal.Client)

	err := client.Close()
	if err != nil {
		log.Println(err)
	}
}

type PoolDialer struct {
	Pool      *utils.Pool
}

func (pd *PoolDialer) Dial(network, address string) (conn net.Conn, err error) {
	dialer := pd.Pool.Get().(*internal.Client)
	conn, err = dialer.Dial(network, address)
	pd.Pool.Put(dialer)
	return
}

func run_as_local() {
	pool := utils.NewPool(newClient, deleteClient, clientPoolSize)
	pd := &PoolDialer{
		Pool: pool,
	}

	srv := socks5.Server{
		Dialer: pd,
	}
	err := srv.ListenAndServe("tcp", *listen)
	if err != nil {
		log.Fatalln(err)
	}
}