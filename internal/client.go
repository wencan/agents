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
	"unsafe"
	"sync/atomic"
	"io"
)

type AgentClientState int

const (
	Idle AgentClientState = iota
	Offline		//connecting
//	Online		//need login
	Logoff
	Logon
	Die
)

type AgentClient struct {
	target     string
	opts       []grpc.DialOption

	pass       Passport

	ctx        context.Context
	cancelFunc context.CancelFunc

	waitGroup  sync.WaitGroup

	raw        agent.AgentClient
	conn       *grpc.ClientConn

	session    string

	stateMutex sync.Mutex
	stateWait  sync.Cond
	state      AgentClientState

	loginMutex sync.Mutex

	err        unsafe.Pointer

	pingTicker *time.Ticker
}

func NewAgentClient(target string, pass Passport, opts ...grpc.DialOption) (client *AgentClient, err error) {
	conn, err := grpc.Dial(target, opts...)
	if err != nil {
		return nil, err
	}

	cc := agent.NewAgentClient(conn)

	ctx, cancelFunc := context.WithCancel(context.Background())
	client = &AgentClient{
		target: target,
		opts: append([]grpc.DialOption{}, opts...),	//deep copy
		pass: pass,
		ctx: ctx,
		cancelFunc: cancelFunc,
		raw: cc,
		conn: conn,
		pingTicker: time.NewTicker(defaultPingDelay),
	}
	client.state = Offline
	client.stateWait.L = &client.stateMutex

	//sync state
	client.waitGroup.Add(1)
	go func() {
		var err error

		defer func () {
			client.changeState(Die)

			if err != nil {
				client.cancel(err)
			}

			client.waitGroup.Done()
		}()

		for {
			var state grpc.ConnectivityState
			state, err = client.conn.State()
			if err != nil {
				return
			}

			state, err = client.conn.WaitForStateChange(client.ctx, state)
			if err != nil {
				return
			}

			switch state {
			case grpc.Connecting:
			case grpc.Ready:			//connected or reconnected
				client.changeState(Logoff)
			case grpc.TransientFailure:
				client.changeState(Offline)
			case grpc.Shutdown:
				return
			}
		}
	}()

	client.waitGroup.Add(1)
	go client.loop()

	return
}

func (client *AgentClient) Login(ctx context.Context, pass Passport) (err error) {
	client.loginMutex.Lock()
	defer client.loginMutex.Unlock()

	if client.State() == Logon {
		return nil
	}

	req := &agent.HelloRequest{
		Major:versionMajor,
		Minor:versionMinor,
	}

	var reply *agent.HelloReply
	reply, err = client.raw.Hello(ctx, req)
	if err != nil {
		return err
	}

	if reply.AuthMethod == agent.AuthMethod_NoAuth {
		client.session = reply.Session
		client.changeState(Logon)
	} else {
		client.pass = pass

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
		authReply, err = client.raw.Auth(ctx, authReq)
		if err != nil {
			return
		}

		client.session = authReply.Session
		client.changeState(Logon)
	}

	return nil
}

func (client *AgentClient) Ping() error {
	md := metadata.New(map[string]string{
		"session": client.session,
	})
	ctx := metadata.NewContext(client.ctx, md)

	ping := &agent.Ping{
		AppData: time.Now().String(),
	}

	pong, err := client.raw.Heartbeat(ctx, ping)
	if err == gerrSessionInvaild {
		//relogin
		err = client.Login(client.ctx, client.pass)

		if err == nil {
			md := metadata.New(map[string]string{
				"session": client.session,
			})
			ctx := metadata.NewContext(client.ctx, md)

			pong, err = client.raw.Heartbeat(ctx, ping)
		}
	}
	if err != nil {
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
			if client.State() != Logon {
				break
			}

			if err := client.Ping(); err != nil {
				client.cancel(err)
				return
			}
		}
	}
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
	reply, err = client.raw.Connect(ctx, req)
	if err == gerrSessionInvaild {
		//relogin
		err = client.Login(context.Background(), client.pass)

		if err == nil {
			md := metadata.New(map[string]string{
				"session": client.session,
			})
			ctx := metadata.NewContext(client.ctx, md)

			reply, err = client.raw.Connect(ctx, req)
		}
	}
	if err != nil {
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

	return NewStreamPipe(ctx, stream), nil
}

func (client *AgentClient) State() AgentClientState {
	client.stateMutex.Lock()
	defer client.stateMutex.Unlock()

	return client.state
}

func (client *AgentClient) changeState(state AgentClientState) {
	client.stateMutex.Lock()
	defer client.stateMutex.Unlock()

	client.state = state
	client.stateWait.Broadcast()
}

//blocks until the state change, or context is done
func (client *AgentClient) WaitForStateChange(ctx context.Context, sourceState AgentClientState) (state AgentClientState, err error) {
	client.stateMutex.Lock()
	defer client.stateMutex.Unlock()

	if sourceState != client.state {
		return client.state, nil
	}

	done := make(chan struct{})
	go func () {
		select {
		case <- client.ctx.Done():
			err = client.ctx.Err()
		case <- ctx.Done():
			err = ctx.Err()
		case <- done:
			client.stateWait.Broadcast()
		}
	}()

	defer close(done)

	for sourceState == client.state {
		client.stateWait.Wait()
		if err != nil {
			return client.state, err
		}
	}

	return client.state, nil
}

//blocks until the client is logon or ctx is done
func (client *AgentClient) Wait(ctx context.Context) (err error) {
	state := Idle

	for {
		state, err = client.WaitForStateChange(ctx, state)
		if err != nil {
			return err
		}

		switch state {
		case Logon:
			return nil
		case Die:
			return client.Err()
		}
	}
}

func (client *AgentClient) Err() (err error) {
	err = *(*error)(atomic.LoadPointer(&client.err))
	if err != nil {
		return err
	}

	return client.ctx.Err()
}

func (client *AgentClient) cancel(err error) {
	if err == nil {
		panic("AgentClient: internal error: missing cancel error")
	}

	atomic.CompareAndSwapPointer(&client.err, nil, unsafe.Pointer(&err))

	if client.ctx.Err() != nil {
		//always canceled
		return
	}
	client.cancelFunc()
}

func (client *AgentClient) CloseWithError(e error) (err error) {
	if err == nil {
		err = io.EOF
	}

	if state := client.State(); state == Logon {
		md := metadata.New(map[string]string{
			"session": client.session,
		})
		ctx := metadata.NewContext(client.ctx, md)
		req := &agent.Empty{}
		_, e := client.raw.Bye(ctx, req)
		if e != nil {
			err = e
		}
	}

	client.cancel(err)		// Will trigger all the stream object is canceled
	client.waitGroup.Wait()

	return err
}

func (client *AgentClient) Close() (err error) {
	return client.CloseWithError(nil)
}