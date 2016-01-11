package main

import (
	"../../../codec"
	"../../../internal"
	"google.golang.org/grpc"
	"log"
	"net"
	"time"
	"fmt"
	"math/rand"
)


func main() {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())

	//enable snappy compress
	if c, err := codec.New("snappy"); err != nil {
		log.Fatalln(err)
	} else {
		if cc, err := codec.WithProto(c); err != nil {
			log.Fatalln(err)
		} else {
			opts = append(opts, grpc.WithCodec(cc))
		}
	}

	client, err := internal.NewAgentClient("127.0.0.1:8080", nil, opts...)
	if err != nil {
		log.Fatalln(err)
	}

	var conn net.Conn
	conn, err = client.Dial("tcp", "127.0.0.1:8888")
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close()

	ch := make(chan interface{}, 1)

	go func() {
		defer func() {
			ch <- nil
		}()

		buff := make([]byte, 1024)

		for {
			n, err := conn.Read(buff)
			if err != nil {
				return
			}

			log.Println("Read:", string(buff[:n]))
		}
	}()

	for {
		//now := <- time.After(time.Millisecond * 200)
		now := time.Now()

		s := fmt.Sprintf("%d: %s", rand.Int31(), now.String())
		log.Println("Write", s)
		_, err := conn.Write([]byte(s))
		if err != nil {
			log.Fatalln(err)
		}
	}

	<- ch
}
