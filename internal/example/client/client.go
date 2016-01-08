package main

import (
	"../../../codec"
	"../../../internal"
	"google.golang.org/grpc"
	"net/http"
	"io/ioutil"
	"log"
	"golang.org/x/net/context"
	"time"
)

func main() {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	//opts = append(opts, grpc.WithBlock())

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
	defer client.Close()

	ctx, _ := context.WithTimeout(context.Background(), time.Second * 10)
	err = client.Login(ctx, nil)
	if err != nil {
		log.Fatalln(err)
	}

	transport := &http.Transport{
		Dial: client.Dial,
	}
	httpc := http.Client{Transport: transport}

	websites := []string{
		"http://www.example.com",
		"http://api.ipify.org",
	}

	for _, website := range websites {
		buff, err := get(&httpc, website)
		if err != nil {
			log.Fatalln(err)
		}

		log.Println(string(buff))
	}
}

func get(httpc *http.Client, url string) (buff []byte, err error) {
	var response *http.Response
	response, err = httpc.Get(url)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	buff, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	return buff, err
}