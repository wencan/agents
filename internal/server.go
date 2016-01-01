package internal

import (
	"../agent"
	"golang.org/x/net/context"
	"code.google.com/p/go-uuid/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"time"
	"sync"
	"net"
	"log"
	"fmt"
)

const (
	versionMajor uint32 = 1;
	versionMinor uint32 = 0;

	defaultContextTimeout time.Duration = 10 * time.Second

	defaultPingDelay time.Duration = 60 * time.Second
	defaultPingMaxDelay time.Duration = 120 * time.Second
	defaultPingCheckDelay time.Duration = time.Minute
	defaultPacketBytesMaxLen int = 1024 * 1024 * 512
	defaultAckMaxDelay time.Duration = 10 * time.Second
	defaultAckCheckDelay time.Duration = 10 * time.Second
)

var (
	gerrVersionNotSupported = grpc.Errorf(codes.Aborted, "version not supported")
	gerrSessionLoss error = grpc.Errorf(codes.DataLoss, "session loss")
	gerrChannelLoss error = grpc.Errorf(codes.DataLoss, "channel loss")
	gerrSessionInvaild error = grpc.Errorf(codes.PermissionDenied, "session invaild")
	gerrChannelInvaild error = grpc.Errorf(codes.PermissionDenied, "channel invaild")
	gerrUnauthenticated error = grpc.Errorf(codes.PermissionDenied, "unauthenticated or authenticate fail")
	gerrHeartbeatTimeout error = grpc.Errorf(codes.DeadlineExceeded, "heartbeat timeout")
	gerrNetworkNotSupported = grpc.Errorf(codes.Canceled, "network type not supported")
	gerrSessonEnded error = grpc.Errorf(codes.Canceled, "session ended")
	gerrAckTimeout error = grpc.Errorf(codes.Canceled, "ack timeout")
	gerrOther = grpc.Errorf(codes.Canceled, "other...")
)

type Session struct {
	lastKeep time.Time

	proxies  map[string]net.Conn

	done     chan struct{}
}

func NewSessionInfo() *Session {
	return &Session{
		lastKeep: time.Now(),
		proxies: make(map[string]net.Conn),
		done: make(chan struct{}),
	}
}

type AgentServer struct {
	guard      Guard //auth

	slocker    sync.Mutex
	sessions   map[string]*Session

	pingTicker *time.Ticker
}

func NewAgentServer(guard Guard) *AgentServer {
	srv := &AgentServer{
		guard: guard,
		sessions: make(map[string]*Session, 10),
		pingTicker: time.NewTicker(defaultPingCheckDelay),
	}

	go srv.checkLoop()

	return srv
}

func (srv *AgentServer) ListenAndServe(network, address string, opts ...grpc.ServerOption) (err error) {
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

func (srv *AgentServer) Hello(ctx context.Context, req *agent.HelloRequest) (reply *agent.HelloReply, err error) {
	if req.Major != versionMajor && req.Minor != versionMinor {
		return nil, gerrVersionNotSupported
	}

	reply = &agent.HelloReply{
		Major: versionMajor,
		Minor: versionMinor,
	}

	if srv.guard == nil {
		session := uuid.New()
		sInfo := NewSessionInfo()

		srv.slocker.Lock()
		srv.sessions[session] = sInfo
		srv.slocker.Unlock()

		reply.AuthMethod = agent.AuthMethod_NoAuth
		reply.Session = session

		log.Println("New session:", session)
	} else {
		reply.AuthMethod = srv.guard.Type()
	}

	return reply, nil
}

func (srv *AgentServer) Auth(ctx context.Context, req *agent.AuthRequest) (reply *agent.AuthReply, err error) {
	if srv.guard == nil {
		return nil, gerrOther
	}

	ok := false
	ok, err = srv.guard.AuthFromProto(req)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, gerrUnauthenticated
	}

	session := uuid.New()
	sInfo := NewSessionInfo()

	srv.slocker.Lock()
	srv.sessions[session] = sInfo
	srv.slocker.Unlock()

	reply = &agent.AuthReply{
		Session: session,
	}

	return reply, nil
}

func (srv *AgentServer) Bind(ctx context.Context, req *agent.BindRequest) (reply *agent.BindReply, err error) {
	var parent string
	if md, ok := metadata.FromContext(ctx); ok {
		ss := md["session"]
		if len(ss) >= 1 {
			parent = ss[0]
		}
	}
	if parent == "" {
		return nil, gerrSessionLoss
	}

	var session string
	var sInfo *Session

	srv.slocker.Lock()
	defer srv.slocker.Unlock()

	if _, ok := srv.sessions[parent]; !ok {
		return nil, gerrSessionInvaild
	}

	session = uuid.New()
	sInfo = NewSessionInfo()
	srv.sessions[session] = sInfo

	reply = &agent.BindReply{
		Session: session,
	}

	return reply, nil
}

func (srv *AgentServer) Connect(ctx context.Context, req *agent.ConnectRequest) (reply *agent.ConnectReply, err error) {
	var session string
	if md, ok := metadata.FromContext(ctx); ok {
		ss := md["session"]
		if len(ss) >= 1 {
			session = ss[0]
		}
	}
	if session == "" {
		return nil, gerrSessionLoss
	}

	srv.slocker.Lock()
	if _, ok := srv.sessions[session]; !ok {
		err = gerrSessionInvaild
	}
	srv.slocker.Unlock()
	if err != nil {
		return nil, err
	}

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
		return nil, gerrSessionInvaild
	}

	reply = &agent.ConnectReply{
		Channel: channel,
		Bound: &agent.Address{
			Network: conn.LocalAddr().Network(),
			Address: conn.LocalAddr().String(),
		},
	}

	log.Println("New channel:", fmt.Sprintf("%s@%s", channel, session))

	return reply, nil
}

//bidirection stream procedure
//client must ack
func (srv *AgentServer) Exchange(stream agent.Agent_ExchangeServer) (err error) {
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
		return gerrSessionLoss
	}
	if channel == "" {
		return gerrChannelLoss
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
			err = gerrChannelInvaild
		}
	} else {
		err = gerrSessionInvaild
	}
	srv.slocker.Unlock()

	if err != nil {
		return err
	}

	pipe := NewStreamPipe(context.Background(), stream)

	log.Println("New proxy:", fmt.Sprintf("%s@%s", channel, session))

	//proxy
	//until error(contain eof)
	return IoExchange(pipe, proxy, done)
}

//call procedure
func (srv *AgentServer) Heartbeat(ctx context.Context, ping *agent.Ping) (pong *agent.Pong, err error) {
	var session string
	if md, ok := metadata.FromContext(ctx); ok {
		ss := md["session"]
		if len(ss) >= 1 {
			session = ss[0]
		}
	}
	if session == "" {
		return nil, gerrSessionLoss
	}

	srv.slocker.Lock()
	defer srv.slocker.Unlock()

	if sInfo, ok := srv.sessions[session]; !ok {
		return nil, gerrSessionInvaild
	} else {
		sInfo.lastKeep = time.Now()
	}

	pong = &agent.Pong{
		AppData: ping.AppData,
	}
	return pong, err
}

func (srv *AgentServer) Bye(ctx context.Context, req *agent.Empty) (reply *agent.Empty, err error) {
	var session string
	if md, ok := metadata.FromContext(ctx); ok {
		ss := md["session"]
		if len(ss) >= 1 {
			session = ss[0]
		}
	}
	if session == "" {
		return nil, gerrSessionLoss
	}

	srv.slocker.Lock()
	defer srv.slocker.Unlock()

	if sInfo, ok := srv.sessions[session]; !ok {
		return nil, gerrSessionInvaild
	} else {
		close(sInfo.done)
		delete(srv.sessions, session)
	}

	return &agent.Empty{}, err
}

func (srv *AgentServer) checkAndRemove() {
	srv.slocker.Lock()
	defer srv.slocker.Unlock()

	now := time.Now()

	for session, sInfo := range srv.sessions {
		if now.Sub(sInfo.lastKeep) > defaultPingMaxDelay {
			close(sInfo.done)
			delete(srv.sessions, session)
		}
	}
}

func (srv *AgentServer) checkLoop() {
	for _ = range srv.pingTicker.C {
		srv.checkAndRemove()
	}
}

