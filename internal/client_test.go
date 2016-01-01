package internal

import (
	"testing"
	"google.golang.org/grpc"
	"net/http"
	"io/ioutil"
)

func TestClient(t *testing.T) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())

	client, err := Dial("127.0.0.1:8080", nil, opts...)
	if err != nil {
		t.Error(err)
		return
	}
	defer client.Close()

	//test Divide
	client, err = client.Divide()
	if err != nil {
		t.Error(err)
		return
	}
	defer client.Close()

	transport := &http.Transport{
		Dial: client.Dial,
	}
	httpc := http.Client{Transport: transport}

	var response *http.Response
	response, err = httpc.Get("http://www.example.com")
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
	t.Log(string(buff))


	response, err = httpc.Get("http://api.ipify.org")
	if err != nil {
		t.Error(err)
		return
	}
	defer response.Body.Close()

	buff, err = ioutil.ReadAll(response.Body)
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(string(buff))
}