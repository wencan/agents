package main

import (
	"net"
	"log"
	"io"
)

func main() {
	listener, err := net.Listen("tcp", "127.0.0.1:8888")
	if err != nil {
		log.Fatalln(err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatalln(err)
		}

		log.Println("New connection form:", conn.RemoteAddr().String())
		go func() {
			_, err := io.Copy(conn, conn)
			if err != nil {
				log.Fatalln(err)
			}
		}()
	}
}