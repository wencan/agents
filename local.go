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
	"time"
	"sync/atomic"
)

const (
	clientPoolSize int = 10
	maxMultiplex   int = 5
	clientCompactDelay time.Duration = time.Second * 15
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

type dialerRef struct {
	*internal.Client
	ref int32
	dropped bool
}

type DialerRotation struct {
	New    func() *internal.Client
	Delete func(*internal.Client)

	storage chan *dialerRef
	backoff chan *dialerRef
}

func NewDialerRotation(new func() *internal.Client, delete func(*internal.Client)) *DialerRotation {
	rotation := &DialerRotation{
		New:     new,
		Delete:  delete,
		storage: make(chan *dialerRef, clientPoolSize),
		backoff: make(chan *dialerRef, clientPoolSize),
	}

	go func(){
		ticker := time.NewTicker(clientCompactDelay)
		for range ticker.C {
			rotation.Compact()
		}
	}()

	return rotation
}

func (rotation *DialerRotation) create() *dialerRef {
	return &dialerRef{
		Client: rotation.New(),
	}
}

func (roration *DialerRotation) destory(dialer *dialerRef) {
	go roration.Delete(dialer.Client)
}

func (rotation *DialerRotation) backoffTry() (dialer *dialerRef) {
	//staging area
	staging := []*dialerRef{}

DONE:
	for {
		var entry *dialerRef
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
					rotation.destory(entry)
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
			rotation.destory(entry)
		}
	}

	return dialer
}

func (rotation *DialerRotation) pop() (dialer *dialerRef) {
	for {
		select {
		case dialer = <-rotation.storage:
			if dialer.Multiplexing() < int32(maxMultiplex) {
				atomic.AddInt32(&dialer.ref, 1)
				return dialer
			} else {
				select {
				case rotation.backoff <- dialer:
				default:
					//full
					rotation.destory(dialer)
				}

				//continue
			}
		default:
			dialer = rotation.backoffTry()
			if dialer == nil {
				dialer = rotation.create()
			}
			atomic.AddInt32(&dialer.ref, 1)
			return dialer
		}
	}

	return nil
}

func (rotation *DialerRotation) push(dialer *dialerRef) {
	n := atomic.AddInt32(&dialer.ref, -1)
	if n == 0 && dialer.dropped{
		rotation.destory(dialer)
		return
	}

	select {
	case rotation.storage <- dialer:
	default:
		rotation.destory(dialer)
	}
}

func (rotation *DialerRotation) Dial(network, address string) (conn net.Conn, err error) {
	//dialer rotate
	dialer := rotation.pop()
	if dialer == nil {
		panic(errors.New("dialer is nil"))
	}

	defer rotation.push(dialer)

	log.Println("Connecting", fmt.Sprintf("%s/%s", network, address))
	return dialer.Dial(network, address)
}

func (rotation *DialerRotation) Compact() {
	staging := []*dialerRef{}
	var total int32 = 0

	FIRST:
	for {
		select {
		case entry := <- rotation.storage:
			staging = append(staging, entry)
			total += entry.Multiplexing()
		default:
			break FIRST
		}
	}

	max := (total + int32(maxMultiplex) - 1) / int32(maxMultiplex)
	SECOND:
	for idx, entry := range staging {
		if idx <= int(max) {
			select {
			case rotation.storage <- entry:
				continue SECOND
			default:
			}
		}

		n := atomic.AddInt32(&entry.ref, -1)
		if n == 0 {
			rotation.destory(entry)
		} else {
			entry.dropped = true
		}
	}
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
