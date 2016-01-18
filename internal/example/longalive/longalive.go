package main

import (
	"../../../codec"
	"../../../internal"
	"google.golang.org/grpc"
	"log"
	"net"
	"fmt"
	"runtime"
	"sync"
)

func main() {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())

	//enable snappy compress
	if c, err := codec.New("snappy"); err != nil {
		log.Println(err)
		return
	} else {
		if cc, err := codec.WithProto(c); err != nil {
			log.Println(err)
			return
		} else {
			opts = append(opts, grpc.WithCodec(cc))
		}
	}

	client, err := internal.NewClient("127.0.0.1:8080", nil, opts...)
	if err != nil {
		log.Println(err)
		return
	}

	var conn net.Conn
	conn, err = client.Dial("tcp", "127.0.0.1:8888")
	if err != nil {
		log.Println(err)
		return
	}
	defer conn.Close()

	var waitGroup sync.WaitGroup

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		buff := make([]byte, 1024)

		for {
			n, err := conn.Read(buff)
			if err != nil {
				log.Println(err)
				return
			}

			fmt.Println(string(buff[:n]))
		}
	}()

	sender := func() {
		for {
			bytes := []byte(blob)

			_, err := conn.Write(bytes)
			if err != nil {
				log.Println(err)
				return
			}
		}
	}

	for i:=0; i<runtime.NumCPU(); i++ {
		waitGroup.Add(1)
		go sender()
	}

	waitGroup.Wait()
}
