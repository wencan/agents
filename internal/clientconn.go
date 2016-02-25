package internal

import (
	"github.com/wencan/agents/agent"
	"google.golang.org/grpc"
	"golang.org/x/net/context"
	"sync/atomic"
)

type ClientConn struct {
	agent.AgentClient
	conn *grpc.ClientConn

	ref *int32	//shared reference counter
}

func Dial(target string, opts ...grpc.DialOption) (cc *ClientConn, err error) {
	var conn *grpc.ClientConn
	conn, err = grpc.Dial(target, opts...)
	if err != nil {
		return nil, err
	}

	raw := agent.NewAgentClient(conn)

	var ref int32 = 1

	cc = &ClientConn{
		AgentClient: raw,
		conn: conn,
		ref: &ref,
	}

	return cc, nil
}

func (cc *ClientConn) Ref() int32 {
	return atomic.LoadInt32(cc.ref)
}

func (cc *ClientConn) Fork() *ClientConn {
	atomic.AddInt32(cc.ref, 1)
	return &ClientConn{
		AgentClient: cc.AgentClient,
		conn: cc.conn,
		ref: cc.ref,
	}
}

func (cc *ClientConn) State() (grpc.ConnectivityState, error) {
	return cc.conn.State()
}

func (cc *ClientConn) WaitForStateChange(ctx context.Context, sourceState grpc.ConnectivityState) (grpc.ConnectivityState, error) {
	return cc.conn.WaitForStateChange(ctx, sourceState)
}

func (cc *ClientConn) Close() (err error) {
	ref := atomic.AddInt32(cc.ref, -1)
	if ref == 0 {
		return cc.conn.Close()
	}

	return nil
}