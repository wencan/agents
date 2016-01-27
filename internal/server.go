package internal

import (
	"../agent"
	"golang.org/x/net/context"
	"github.com/pborman/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"time"
	"sync"
	"net"
	"log"
	"fmt"
)

type Session struct {
	lastKeep time.Time

	proxies  map[string]net.Conn

	done     chan struct{}
}

func newSessionInfo() *Session {
	return &Session{
		lastKeep: time.Now(),
		proxies: make(map[string]net.Conn),
		done: make(chan struct{}),
	}
}

type Server struct {
	slocker    sync.Mutex
	sessions   map[string]*Session

	pingTicker *time.Ticker
}

func NewServer() *Server {
	srv := &Server{
		sessions: make(map[string]*Session, 10),
		pingTicker: time.NewTicker(defaultPingCheckDelay),
	}

	go srv.checkLoop()

	return srv
}

func (srv *Server) ListenAndServe(network, address string, opts ...grpc.ServerOption) (err error) {
	listener, err := net.Listen(network, address)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer(opts...)
	agent.RegisterAgentServer(grpcServer, srv)

	err = grpcServer.Serve(listener)
	if err != nil {
		return err
	}
	return nil
}

func (srv *Server) Hello(ctx context.Context, req *agent.HelloRequest) (reply *agent.HelloReply, err error) {
	if req.Major != versionMajor && req.Minor != versionMinor {
		return nil, ErrVersionNotSupported
	}

	reply = &agent.HelloReply{
		Major: versionMajor,
		Minor: versionMinor,
	}

	session := uuid.New()
	sInfo := newSessionInfo()

	srv.slocker.Lock()
	srv.sessions[session] = sInfo
	srv.slocker.Unlock()

	reply.Session = session

	log.Println("New session:", session)

	return reply, nil
}

func (srv *Server) Connect(ctx context.Context, req *agent.ConnectRequest) (reply *agent.ConnectReply, err error) {
	var session string
	if md, ok := metadata.FromContext(ctx); ok {
		ss := md["session"]
		if len(ss) >= 1 {
			session = ss[0]
		}
	}
	if session == "" {
		return nil, ErrSessionLoss
	}

	srv.slocker.Lock()
	if _, ok := srv.sessions[session]; !ok {
		err = ErrSessionInvaild
	}
	srv.slocker.Unlock()
	if err != nil {
		return nil, err
	}

	log.Println("Connecting:", fmt.Sprintf("%s/%s", req.Remote.Network, req.Remote.Address))

	var conn net.Conn
	if conn, err = net.Dial(req.Remote.Network, req.Remote.Address); err != nil {
		return nil, err
	}

	srv.slocker.Lock()
	defer srv.slocker.Unlock()

	channel := uuid.New()
	if sInfo, ok := srv.sessions[session]; ok {
		sInfo.proxies[channel] = conn
	} else {
		return nil, ErrSessionInvaild
	}

	reply = &agent.ConnectReply{
		Channel: channel,
		Bound: &agent.Address{
			Network: conn.LocalAddr().Network(),
			Address: conn.LocalAddr().String(),
		},
	}

	return reply, nil
}

//bidirection stream procedure
//client must ack
func (srv *Server) Exchange(stream agent.Agent_ExchangeServer) (err error) {
	var session string
	var channel string
	if md, ok := metadata.FromContext(stream.Context()); ok {
		ss := md["session"]
		if len(ss) >= 1 {
			session = ss[0]
		}
		cs := md["channel"]
		if len(cs) >= 1 {
			channel = cs[0]
		}
	}
	if session == "" {
		return ErrSessionLoss
	}
	if channel == "" {
		return ErrChannelLoss
	}

	var done chan struct{}
	var proxy net.Conn

	//get proxy connection
	srv.slocker.Lock()
	if sInfo, ok := srv.sessions[session]; ok {
		done = sInfo.done

		if proxy, ok = sInfo.proxies[channel]; ok {
			delete(sInfo.proxies, channel)
		} else {
			err = ErrChannelInvaild
		}
	} else {
		err = ErrSessionInvaild
	}
	srv.slocker.Unlock()

	if err != nil {
		return err
	}

	pipe := NewStreamPipe(stream)

	//proxy
	//until error(contain eof)
	return IoExchange(pipe, proxy, done)
}

//call procedure
func (srv *Server) Heartbeat(ctx context.Context, ping *agent.Ping) (pong *agent.Pong, err error) {
	var session string
	if md, ok := metadata.FromContext(ctx); ok {
		ss := md["session"]
		if len(ss) >= 1 {
			session = ss[0]
		}
	}
	if session == "" {
		return nil, ErrSessionLoss
	}

	srv.slocker.Lock()
	defer srv.slocker.Unlock()

	if sInfo, ok := srv.sessions[session]; !ok {
		return nil, ErrSessionInvaild
	} else {
		sInfo.lastKeep = time.Now()
	}

	pong = &agent.Pong{
		AppData: ping.AppData,
	}
	return pong, err
}

func (srv *Server) checkAndRemove() {
	srv.slocker.Lock()
	defer srv.slocker.Unlock()

	now := time.Now()

	for session, sInfo := range srv.sessions {
		if now.Sub(sInfo.lastKeep) > defaultPingMaxDelay {
			//kick
			close(sInfo.done)
			delete(srv.sessions, session)
			for _, proxy := range sInfo.proxies {
				proxy.Close()
			}
		}
	}
}

func (srv *Server) checkLoop() {
	for _ = range srv.pingTicker.C {
		srv.checkAndRemove()
	}
}

