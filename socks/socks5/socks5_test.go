package socks5

import (
	"golang.org/x/net/proxy"
	"testing"
	"net/http"
	"net"
	"io/ioutil"
)


type TestDialer struct {

}

func (self TestDialer) Dial(network, address string) (proxy net.Conn, err error) {
	return net.Dial(network, address)
}

func TestServer(t *testing.T) {
	srv := Server{&TestDialer{}}
	if err := srv.ListenAndServe("tcp", ":8080"); err != nil {
		t.Error(err)
	}
}

func TestClient(t *testing.T) {
	proxy, err := proxy.SOCKS5("tcp", "127.0.0.1:8080", nil, &net.Dialer{})
	if err != nil {
		t.Error(err)
		return
	}

	transport := &http.Transport{
		Dial: proxy.Dial,
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
}