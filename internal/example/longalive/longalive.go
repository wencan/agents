package main

import (
	"github.com/wencan/agents/codec"
	"github.com/wencan/agents/internal"
	"google.golang.org/grpc"
	"log"
	"net"
//	"fmt"
	"runtime"
	"sync"
	"time"
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

	client, err := internal.NewClient("127.0.0.1:8080", opts...)
	if err != nil {
		log.Println(err)
		return
	}
	defer client.Close()

	var waitGroup sync.WaitGroup

	for i:=0; i<runtime.NumCPU(); i++ {
		waitGroup.Add(1)

		go func(i int) {
			defer waitGroup.Done()

			var conn net.Conn
			conn, err = client.Dial("tcp", "127.0.0.1:8888")
			if err != nil {
				log.Println("Dial", err)
				return
			}

			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()

				buff := make([]byte, 1024)

				for {
					_, err := conn.Read(buff)
					if err != nil {
						log.Println(err)
						return
					}

					//fmt.Println(string(buff[:n]))
				}
			}()

			done := time.After(time.Second * 3 * time.Duration(i))
			//done := make(chan struct{})

			DONE:
			for {
				bytes := []byte(blob)

				_, err := conn.Write(bytes)
				if err != nil {
					log.Println(err)
					return
				}

				select {
				case <- done:
					break DONE
				default:
				}
			}

			err := conn.Close()
			if err != nil {
				log.Println(err)
			}

		}(i)
	}

	waitGroup.Wait()
}
