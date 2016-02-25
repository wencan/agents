package main

import (
	"github.com/wencan/agents/codec"
	"github.com/wencan/agents/internal"
	"google.golang.org/grpc"
	"net/http"
	"io/ioutil"
	"log"
	"sync"
)

func main() {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	//opts = append(opts, grpc.WithBlock())

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

	transport := &http.Transport{
		Dial: client.Dial,
	}
	defer transport.CloseIdleConnections()
	httpc := http.Client{Transport: transport}

	websites := []string{
		"http://www.example.com",
		"http://api.ipify.org",
		"https://www.gnu.org/",
		"https://www.kernel.org/",
		"https://www.debian.org/",
	}

	waitGroup := sync.WaitGroup{}

	for _, website := range websites {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()

			buff, err := get(&httpc, website)
			if err != nil {
				log.Println(err)
				return
			}

			log.Println(string(buff))
		}()
	}

	waitGroup.Wait()
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