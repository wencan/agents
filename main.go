package main

import (
	"flag"
	"log"
)

var (
	local    *bool   = flag.Bool("local", false, "run as local server")
	remote   *bool   = flag.Bool("remote", false, "run as remote server")
	listen   *string = flag.String("listen", ":1115", "sock5 server listen address, or remote server listen address")
	server   *string = flag.String("server", "example.com:1115", "remote server address")
	certFile *string = flag.String("certFile", "", "cert file path")
	keyFile  *string = flag.String("keyFile", "", "key file path")
)

func init() {
	flag.Parse()
}

func main() {
	if *local {
		run_as_local()
	} else if *remote {
		run_as_remote()
	} else {
		log.Fatalln("server mode must is local or remote")
	}
}
