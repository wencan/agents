package main

import (
	"./codec"
	"./internal"
	"./socks/socks5"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"log"
	"net"
)

const (
	clientPoolSize int = 10
	maxMultiplex   int = 5
)

func newClient() (client *internal.Client) {
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

	client, err = internal.NewClient(*server, opts...)
	if err != nil {
		log.Fatalln(err)
	}
	return client
}

func deleteClient(client *internal.Client) {
	err := client.Close()
	if err != nil {
		log.Println(err)
	}
}

type DialerRotation struct {
	New    func() *internal.Client
	Delete func(*internal.Client)

	storage chan *internal.Client
	backoff chan *internal.Client
}

func NewDialerRotation(new func() *internal.Client, delete func(*internal.Client)) *DialerRotation {
	return &DialerRotation{
		New:     new,
		Delete:  delete,
		storage: make(chan *internal.Client, clientPoolSize),
		backoff: make(chan *internal.Client, clientPoolSize),
	}
}

func (rotation *DialerRotation) backoffTry() (dialer *internal.Client) {
	//staging area
	staging := []*internal.Client{}

DONE:
	for {
		var entry *internal.Client
		select {
		case entry = <- rotation.backoff:
		default:
			break DONE
		}

		if entry.Multiplexing() < int32(maxMultiplex) {
			if dialer == nil {
				dialer = entry
			} else {
				select {
				case rotation.storage <- entry:
				default:
					rotation.Delete(entry)
				}
			}
		} else {
			staging = append(staging, entry)
		}
	}

	for _, entry := range staging {
		select {
		case rotation.backoff <- entry:
		default:
			rotation.Delete(entry)
		}
	}

	return dialer
}

func (rotation *DialerRotation) pop() (dialer *internal.Client) {
	for {
		select {
		case dialer = <-rotation.storage:
			if dialer.Multiplexing() < int32(maxMultiplex) {
				return dialer
			} else {
				select {
				case rotation.backoff <- dialer:
				default:
					//full
					rotation.Delete(dialer)
				}

				//continue
			}
		default:
			dialer = rotation.backoffTry()
			if dialer == nil {
				dialer = rotation.New()
			}
			return dialer
		}
	}

	return nil
}

func (rotation *DialerRotation) push(dialer *internal.Client) {
	select {
	case rotation.storage <- dialer:
	default:
		rotation.Delete(dialer)
	}
}

func (rotation *DialerRotation) Dial(network, address string) (conn net.Conn, err error) {
	//dialer rotate
	dialer := rotation.pop()
	if dialer == nil {
		panic(errors.New("dialer is nil"))
	}

	rotation.push(dialer)

	log.Println("Connecting", fmt.Sprintf("%s/%s", network, address))
	return dialer.Dial(network, address)
}

func run_as_local() {
	dialer := NewDialerRotation(newClient, deleteClient)

	srv := socks5.Server{
		Dialer: dialer,
	}
	err := srv.ListenAndServe("tcp", *listen)
	if err != nil {
		log.Fatalln(err)
	}
}
