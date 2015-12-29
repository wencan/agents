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

	waitGroup sync.WaitGroup

	conn agent.AgentClient

	session string

	pingTicker *time.Ticker
}

func NewAgentClient(ctx context.Context) *AgentClient {
	var cancelFunc context.CancelFunc
	ctx , cancelFunc = context.WithCancel(ctx)
	client := &AgentClient{
		ctx: ctx,
		cancelFunc: cancelFunc,
		pingTicker: time.NewTicker(defaultPingDelay),
	}

	client.waitGroup.Add(1)
	go client.ping_loop()

	return client
}

func (client *AgentClient) ping() error {
	ping := &agent.Ping{
		AppData: time.Now(),
	}

	md := metadata.New(map[string][]string{
		"session": client.session,
	})
	ctx := metadata.NewContext(client.ctx, md)

	ctx, _ = context.WithTimeout(ctx, defaultContextTimeout)
	if pong, err := client.conn.Heartbeat(ctx, ping); err != nil {
		return err
	} else if ping.AppData != pong.AppData {
		return errors.New("pong appData exception")
	}
	return nil
}

func (client *AgentClient) ping_loop() {
	defer client.waitGroup.Done()

	for {
		select {
		case <- client.ctx.Done():
			return
		case <- client.pingTicker.C :
			if err := client.ping(); err != nil {
				log.Println(err)
				client.cancelFunc()
				return
			}
		}
	}
}

func (client *AgentClient) Dial(network, address string) (conn net.Conn, err error) {
	req := &agent.ConnectRequest{
		Remote: &agent.Address{
			Network: network,
			Address: address,
		},
	}

	md := metadata.New(map[string][]string{
		"session": client.session,
	})
	ctx := metadata.NewContext(client.ctx, md)
	ctx, _ = context.WithTimeout(ctx, defaultContextTimeout)
	var reply *agent.ConnectReply
	if reply, err = client.conn.Connect(ctx, req); err != nil {
		return nil, err
	}

	md = metadata.New(map[string][]string{
		"session": client.session,
		"channel": reply.Channel,
	})
	ctx = metadata.NewContext(client.ctx, md)
	var stream agent.Agent_ExchangeClient
	if stream, err = client.conn.Exchange(ctx); err != nil {
		return err
	}
	//...

	client.waitGroup.Add(1)
	defer client.waitGroup.Done()
}

func (client *AgentClient) Close() error {
	client.cancelFunc()

	client.waitGroup.Wait()

	return nil
}