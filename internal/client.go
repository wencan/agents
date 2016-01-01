package internal

import (
	"../agent"
	"google.golang.org/grpc/metadata"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"time"
	"sync"
	"net"
	"errors"
)

type AgentClient struct {
	target     string
	opts       []grpc.DialOption

	ctx        context.Context
	cancelFunc context.CancelFunc

	waitGroup  sync.WaitGroup

	raw        agent.AgentClient

	session    string

	pingTicker *time.Ticker
}

func Dial(target string, pass Passport, opts ...grpc.DialOption) (client *AgentClient, err error) {
	conn, err := grpc.Dial(target, opts...)
	if err != nil {
		return nil, err
	}

	cc := agent.NewAgentClient(conn)

	ctx, cancelFunc := context.WithCancel(context.Background())
	client = &AgentClient{
		target: target,
		opts: append([]grpc.DialOption{}, opts...),
		ctx: ctx,
		cancelFunc: cancelFunc,
		raw: cc,
		pingTicker: time.NewTicker(defaultPingDelay),
	}
	defer func() {
		if err != nil {
			client.Close()
			client = nil
		}
	}()

	if err = client.login(pass); err != nil {
		return
	}

	client.waitGroup.Add(1)
	go client.loop()

	return
}

func (client *AgentClient) login(pass Passport) (err error) {
	req := &agent.HelloRequest{
		Major:versionMajor,
		Minor:versionMinor,
	}

	var reply *agent.HelloReply
	reply, err = client.raw.Hello(client.ctx, req)
	if err != nil {
		return err
	}

	if reply.AuthMethod == agent.AuthMethod_NoAuth {
		client.session = reply.Session
	} else {
		if pass == nil {
			return errors.New("need authenticate")
		}
		if reply.AuthMethod != pass.Type() {
			return errors.New("passport type unrecognized")
		}

		var authReq *agent.AuthRequest
		authReq, err = pass.ToProto()
		if err != nil {
			return
		}

		var authReply *agent.AuthReply
		authReply, err = client.raw.Auth(client.ctx, authReq)
		if err != nil {
			return
		}

		client.session = authReply.Session
	}

	return nil
}

func (client *AgentClient) bind(session string) (err error) {
	md := metadata.New(map[string]string{
		"session": session,
	})
	ctx := metadata.NewContext(client.ctx, md)

	req := &agent.BindRequest{}

	var reply *agent.BindReply
	reply, err = client.raw.Bind(ctx, req)
	if err != nil {
		return err
	}

	client.session = reply.Session
	return nil
}

func (client *AgentClient) ping() error {
	md := metadata.New(map[string]string{
		"session": client.session,
	})
	ctx := metadata.NewContext(client.ctx, md)

	ping := &agent.Ping{
		AppData: time.Now().String(),
	}

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
				client.cancelFunc()
				return
			}
		}
	}
}

func (client *AgentClient) Divide() (newClient *AgentClient, err error) {
	conn, err := grpc.Dial(client.target, client.opts...)
	if err != nil {
		return nil, err
	}

	cc := agent.NewAgentClient(conn)

	ctx, cancelFunc := context.WithCancel(context.Background())
	newClient = &AgentClient{
		target: client.target,
		opts: append([]grpc.DialOption{}, client.opts...),
		ctx: ctx,
		cancelFunc: cancelFunc,
		raw: cc,
		pingTicker: time.NewTicker(defaultPingDelay),
	}
	defer func() {
		if err != nil {
			newClient.Close()
			newClient = nil
		}
	}()

	if err = newClient.bind(client.session); err != nil {
		return
	}

	newClient.waitGroup.Add(1)
	go newClient.loop()

	return newClient, nil
}

func (client *AgentClient) Dial(network, address string) (conn net.Conn, err error) {
	md := metadata.New(map[string]string{
		"session": client.session,
	})
	ctx := metadata.NewContext(client.ctx, md)

	req := &agent.ConnectRequest{
		Remote: &agent.Address{
			Network: network,
			Address: address,
		},
	}

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