package agents

import (
	"./agent"
	"google.golang.org/grpc/metadata"
	"golang.org/x/net/context"
	"log"
	"time"
	"sync"
	"net"
	"io"
	"bytes"
	"errors"
)

type AgentClient struct {
	ctx context.Context
	cancelFunc context.CancelFunc

	waitGroup *sync.WaitGroup

	conn agent.AgentClient

	session string
}

func NewAgentClient(ctx context.Context) *AgentClient {
	var cancelFunc context.CancelFunc
	ctx , cancelFunc = context.WithCancel(ctx)
	client := &AgentClient{
		ctx: ctx,
		cancelFunc: cancelFunc,
		waitGroup: &sync.WaitGroup{},
	}

	client.waitGroup.Add(1)
	go func () {
		defer client.waitGroup.Done()

		for {
			select {
			case ctx.Done():
				return
			case <- time.Tick(defaultPingDelay):
				if err := client.ping(); err != nil {
					client.cancelFunc()
					log.Println(err)
					return
				}
			}
		}
	}()

	return client
}

func Dial() (*AgentClient, error) {

}

func DialTLS() (*AgentClient, error) {

}

func (self *AgentClient) Dial(network, address string) (c net.Conn, err error) {
	req := &agent.ConnectRequest{
		Remote: &agent.Address{
			Network: network,
			Address: address,
		},
	}

	md := metadata.New(map[string][]string{
		"session": self.session,
	})
	ctx := metadata.NewContext(self.ctx, md)

	ctx, _ = context.WithTimeout(ctx, defaultContextTimeout)
	if reply, err := self.conn.Connect(ctx, req); err != nil {
		return nil, err
	} else {
		md := metadata.New(map[string][]string{
			"session": self.session,
			"channel": reply.Channel,
		})
		ctx := metadata.NewContext(self.ctx, md)
		if stream, err := self.conn.Transfer(ctx); err != nil {
			return err
		} else {
			//...
		}
	}
}

func (self *AgentClient) ping() error {
	ping := &agent.Ping{
		AppData: time.Now(),
	}

	md := metadata.New(map[string][]string{
		"session": self.session,
	})
	ctx := metadata.NewContext(self.ctx, md)

	ctx, _ = context.WithTimeout(ctx, defaultContextTimeout)
	if pong, err := self.conn.Heartbeat(ctx, ping); err != nil {
		return err
	} else if ping.AppData != pong.AppData {
		return errors.New("pong appData exception")
	}
}