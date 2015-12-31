package internal

import (
	"testing"
	"google.golang.org/grpc"
	"net/http"
	"io/ioutil"
)

func TestAgentClient(t *testing.T) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())

	dialer, err := Dial("127.0.0.1:8080", opts...)
	if err != nil {
		t.Error(err)
		return
	}

	transport := http.Transport{
		Dial: dialer.Dial,
	}
	httpc := http.Client{Transport: transport}

	var response *http.Response
	response, err = httpc.Get("www.example.com")
	if err != nil {
		t.Error(err)
		return
	}
	defer response.Body.Close()

	var buff []byte
	buff, err = ioutil.ReadAll(response.Body)
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(buff)
}