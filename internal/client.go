package internal

import (
	"github.com/wencan/agents/agent"
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

type Client struct {
	cc         *ClientConn

	ctx        context.Context
	cancelFunc context.CancelFunc

	waitGroup  sync.WaitGroup

	session    string

	stateMutex sync.Mutex
	stateWait  sync.Cond
	state      AgentClientState

	logins     chan interface{}

	err        unsafe.Pointer
}

func NewClient(target string, opts ...grpc.DialOption) (client *Client, err error) {
	cc, err := Dial(target, opts...)
	if err != nil {
		return nil, err
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	client = &Client{
		cc: cc,
		ctx: ctx,
		cancelFunc: cancelFunc,
		logins: make(chan interface{}, 1),
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
			state, err = client.cc.State()
			if err != nil {
				return
			}

			state, err = client.cc.WaitForStateChange(client.ctx, state)
			if err != nil {
				return
			}

			switch state {
			case grpc.Connecting:
			case grpc.Ready:			//connected or reconnected
				client.changeState(Logoff)
				client.logins <- 1
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

func (client *Client) login(ctx context.Context) (err error) {
	if client.State() == Logon {
		return nil
	}

	req := &agent.HelloRequest{
		Major:versionMajor,
		Minor:versionMinor,
	}

	var reply *agent.HelloReply
	reply, err = client.cc.Hello(ctx, req)
	if err != nil {
		return err
	}

	client.session = reply.Session
	client.changeState(Logon)

	return nil
}

func (client *Client) Ping() (err error) {
	err = client.Wait(context.Background())
	if err != nil {
		return err
	}

	md := metadata.New(map[string]string{
		"session": client.session,
	})
	ctx := metadata.NewContext(client.ctx, md)

	ping := &agent.Ping{
		AppData: time.Now().String(),
	}

	pong, err := client.cc.Heartbeat(ctx, ping)
	if err != nil {
		return err
	} else if ping.AppData != pong.AppData {
		return errors.New("pong appData exception")
	}

	return nil
}

func (client *Client) loop() {
	defer client.waitGroup.Done()

	err := client.login(context.Background())
	if err != nil {
		client.cancel(err)
		return
	}

	for {
		select {
		case <-client.ctx.Done():
			return
		case <-client.logins:
			err := client.login(context.Background())
			if err != nil {
				client.cancel(err)
				return
			}
		case <-time.After(defaultPingCheckDelay):
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

func (client *Client) Multiplexing() int32 {
	return client.cc.Ref() - 1
}

func (client *Client) Dial(network, address string) (conn net.Conn, err error) {
	err = client.Wait(context.Background())
	if err != nil {
		return nil, err
	}

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
	reply, err = client.cc.Connect(ctx, req)
	if err != nil {
		return nil, err
	}

	md = metadata.New(map[string]string{
		"session": client.session,
		"channel": reply.Channel,
	})
	ctx = metadata.NewContext(context.Background(), md)
	var stream agent.Agent_ExchangeClient
	if stream, err = client.cc.Exchange(ctx); err != nil {
		return nil, err
	}

	pipe := NewStreamPipe(stream)
	pipe.Attach(client.cc.Fork())
	return pipe, nil
}

func (client *Client) State() AgentClientState {
	client.stateMutex.Lock()
	defer client.stateMutex.Unlock()

	return client.state
}

func (client *Client) changeState(state AgentClientState) {
	client.stateMutex.Lock()
	defer client.stateMutex.Unlock()

	client.state = state
	client.stateWait.Broadcast()
}

//blocks until the state change, or context is done
func (client *Client) WaitForStateChange(ctx context.Context, sourceState AgentClientState) (state AgentClientState, err error) {
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
			client.stateWait.Broadcast()
		case <- ctx.Done():
			err = ctx.Err()
			client.stateWait.Broadcast()
		case <- done:
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
func (client *Client) Wait(ctx context.Context) (err error) {
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

func (client *Client) Err() (err error) {
	p := (*error)(atomic.LoadPointer(&client.err))
	if p != nil {
		return *p
	}

	return client.ctx.Err()
}

func (client *Client) cancel(err error) {
	if err == nil {
		panic("AgentClient: internal error: missing cancel error")
	}

	ok := atomic.CompareAndSwapPointer(&client.err, nil, unsafe.Pointer(&err))
	if !ok {
		//already canceled
		return
	}

	client.cancelFunc()
}

func (client *Client) CloseWithError(err error) error {
	if err == nil {
		err = io.EOF
	}

	client.cancel(err)
	client.waitGroup.Wait()

	return client.cc.Close()
}

func (client *Client) Close() (err error) {
	return client.CloseWithError(nil)
}