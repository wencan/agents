package socks5

import (
	"testing"
	"log"
	"net"
)


type TestDialer struct {

}

func (self TestDialer) Dial(network, address string) (proxy net.Conn, err error) {
	return net.Dial(network, address)
}

func TestSocks5(t *testing.T) {
	srv := Socks5Server{&TestDialer{}}
	if err := srv.ListenAndServe("tcp", ":8080"); err != nil {
		log.Fatalln(err)
	}
}
