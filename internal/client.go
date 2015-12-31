package internal

import (
	"../agent"
	"google.golang.org/grpc/metadata"
	"golang.org/x/net/context"
	"log"
	"time"
	"sync"
	"net"
	"errors"
	"google.golang.org/grpc"
)

type AgentClient struct {
	ctx        context.Context
	cancelFunc context.CancelFunc

	waitGroup  sync.WaitGroup

	raw        agent.AgentClient

	session    string

	pingTicker *time.Ticker
}

func Dial(target string, opts ...grpc.DialOption) (client *AgentClient, err error) {
	conn, err := grpc.Dial(target, opts...)
	if err != nil {
		return nil, err
	}

	cc := agent.NewAgentClient(conn)

	ctx, cancelFunc := context.WithCancel(context.Background())
	client = &AgentClient{
		ctx: ctx,
		cancelFunc: cancelFunc,
		raw: cc,
		pingTicker: time.NewTicker(defaultPingDelay),
	}
	defer func() {
		if err != nil {
			client.Close()
		}
	}()

	if err = client.login(); err != nil {
		return nil, err
	}

	client.waitGroup.Add(1)
	go client.loop()

	return client, nil
}

func (client *AgentClient) login() (err error) {
	helloReq := &agent.HelloRequest{
		Major:versionMajor,
		Minor:versionMinor,
	}

	var helloReply *agent.HelloReply
	helloReply, err = client.raw.Hello(client.ctx, helloReq)
	if err != nil {
		return err
	}

	if helloReply.AuthMethod != agent.AuthMethod_NoAuth {
		return errors.New("no username and password")
	}

	client.session = helloReply.Session
	return nil
}

func (client *AgentClient) ping() error {
	ping := &agent.Ping{
		AppData: time.Now().String(),
	}

	md := metadata.New(map[string]string{
		"session": client.session,
	})
	ctx := metadata.NewContext(client.ctx, md)

	ctx, _ = context.WithTimeout(ctx, defaultContextTimeout)
	if pong, err := client.raw.Heartbeat(ctx, ping); err != nil {
		return err
	} else if ping.AppData != pong.AppData {
		return errors.New("pong appData exception")
	}
	return nil
}

func (client *AgentClient) loop() {
	defer client.waitGroup.Done()

	for {
		select {
		case <-client.ctx.Done():
			return
		case <-client.pingTicker.C:
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

	md := metadata.New(map[string]string{
		"session": client.session,
	})
	ctx := metadata.NewContext(client.ctx, md)
	ctx, _ = context.WithTimeout(ctx, defaultContextTimeout)
	var reply *agent.ConnectReply
	if reply, err = client.raw.Connect(ctx, req); err != nil {
		return nil, err
	}

	md = metadata.New(map[string]string{
		"session": client.session,
		"channel": reply.Channel,
	})
	ctx = metadata.NewContext(client.ctx, md)
	var stream agent.Agent_ExchangeClient
	if stream, err = client.raw.Exchange(ctx); err != nil {
		return nil, err
	}

	return NewStreamPipe(client.ctx, stream), nil
}

func (client *AgentClient) Close() (err error) {
	if len(client.session) == 0 {
		return nil
	}

	md := metadata.New(map[string]string{
		"session": client.session,
	})
	ctx := metadata.NewContext(client.ctx, md)
	req := &agent.Empty{}
	if _, err = client.raw.Bye(ctx, req); err != nil {
		return err
	}

	client.cancelFunc()		// Will trigger all the stream object is canceled
	client.waitGroup.Wait()

	return nil
}