package agents

import (
	"./agent"
	"golang.org/x/net/context"
	"code.google.com/p/go-uuid/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"time"
	"sync"
	"net"
	"errors"
	"encoding/binary"
	"bytes"
	"io"
)

const (
	version_major uint32 = 1;
	version_minor uint32 = 0;

	defaultContextTimeout time.Duration = 10 *time.Second

	defaultPingDelay time.Duration = 60 * time.Second
	defaultPingMaxDelay time.Duration = 120 * time.Second
	defaultPingCheckDelay time.Duration = time.Minute
	defaultPacketBytesMaxLen int = 1024 * 1024 *512
	defaultAckMaxDelay time.Duration = 10 * time.Second
	defaultAckCheckDelay time.Duration = 10 * time.Second
)

var (
	gerr_version_notsupported = grpc.Errorf(codes.Aborted, "version not supported")
	gerr_session_loss error = grpc.Errorf(codes.DataLoss, "session loss")
	gerr_channel_loss error = grpc.Errorf(codes.DataLoss, "channel loss")
	gerr_session_invaild error = grpc.Errorf(codes.PermissionDenied, "session invaild")
	gerr_channel_invaild error = grpc.Errorf(codes.PermissionDenied, "channel invaild")
	gerr_unauthenticated error = grpc.Errorf(codes.PermissionDenied, "unauthenticated or authenticate fail")
	gerr_heartbeat_timeout error = grpc.Errorf(codes.DeadlineExceeded, "heartbeat timeout")
	gerr_network_notsupported = grpc.Errorf(codes.Canceled, "network type not supported")
	gerr_sesson_ended error = grpc.Errorf(codes.Canceled, "session ended")
	gerr_ack_timeout error = grpc.Errorf(codes.Canceled, "ack timeout")
	gerr_other = grpc.Errorf(codes.Canceled, "other...")
)

type AuthChecker interface {
	Type() agent.AuthMethod
	UsernameAndPassword(username, password string) bool
}

type Session struct {
	lastKeep time.Time
	waitAuth bool

	proxies  map[string]net.Conn

	done     chan error
}

func NewSessionInfo() *Session {
	return &Session{
		lastKeep: time.Now(),
		waitAuth: true,
		proxies: make([string]net.Conn),
		done: make(chan struct{}),
	}
}

func (s *Session) Keep() {
	s.lastKeep = time.Now()
}

func (s *Session) LastKeep() time.Time {
	return s.lastKeep
}

func (s *Session) CloseWithError(e error) error {

}

func (s *Session) Close() error {

}

type AgentServer struct {
	authChecker    AuthChecker

	slocker        sync.Locker
	sessions       map[string]*Session

	pingTicker     *time.Ticker
}

func NewAgentServer(checker AuthChecker) *AgentServer {
	server := &AgentServer{
		authChecker: checker,
		slocker: sync.Mutex{},
		sessions: make(map[string]*Session, 100),
		pingTicker: &time.NewTicker(defaultPingCheckDelay),
	}

	go server.checkLoop()

	return server
}

func (self *AgentServer) Hello(ctx context.Context, req *agent.HelloRequest) (reply *agent.HelloReply, err error) {
	if req.Major != version_major && req.Minor != version_minor {
		return nil, gerr_version_notsupported
	}

	session := uuid.New()
	sInfo := NewSessionInfo()

	reply = &agent.HelloReply{
		Major: version_major,
		Minor: version_minor,
		Session: session,
	}

	if self.authChecker != nil {
		sInfo.waitAuth = true
		reply.AuthMethod = self.authChecker.Type()
	} else {
		sInfo.waitAuth = false
		reply.AuthMethod = agent.AuthMethod_NoAuth
	}

	self.slocker.Lock()
	defer self.slocker.Unlock()

	self.sessions[session] = sInfo

	return reply, nil
}

func (self *AgentServer) Auth(ctx context.Context, req *agent.AuthRequest) (reply *agent.AuthReply, err error) {
	var session string
	if md, ok := metadata.FromContext(ctx); ok {
		ss := md["session"]
		if len(ss) >= 1 {
			session = ss[0]
		}
	}
	if session == "" {
		return nil, gerr_session_loss
	}

	if self.authChecker != nil {
		ok := false
		if up := req.GetUsernameAndPassword(); up != nil {
			ok = self.authChecker.UsernameAndPassword(up.Username, up.Password)
		}

		if !ok {
			return nil, gerr_unauthenticated
		}
	} else {
		return nil, gerr_other
	}

	self.slocker.Lock()
	defer self.slocker.Unlock()

	if sInfo, ok := self.sessions[session]; !ok {
		return nil, gerr_session_invaild
	} else {
		sInfo.waitAuth = false
	}

	reply = &agent.AuthReply{
		Session: session,
	}

	return reply, nil
}

func (self *AgentServer) Bind(ctx context.Context, req *agent.BindRequest) (reply *agent.BindReply, err error) {
	var parent string
	if md, ok := metadata.FromContext(ctx); ok {
		ss := md["session"]
		if len(ss) >= 1 {
			parent = ss[0]
		}
	}
	if parent == "" {
		return nil, gerr_session_loss
	}

	var session string
	var sInfo *Session

	self.slocker.Lock()
	defer self.slocker.Unlock()

	if pSession, ok := self.sessions[parent]; !ok {
		return nil, gerr_session_invaild
	} else if pSession.waitAuth {
		return nil, gerr_unauthenticated
	}

	session = uuid.New()
	sInfo = NewSessionInfo()
	sInfo.waitAuth = false
	self.sessions[session] = sInfo

	reply = agent.BindReply{
		Session: session,
	}

	return reply, nil
}

func (self *AgentServer) Connect(ctx context.Context, req *agent.ConnectRequest) (reply *agent.ConnectReply, err error) {
	var session string
	if md, ok := metadata.FromContext(ctx); ok {
		ss := md["session"]
		if len(ss) >= 1 {
			session = ss[0]
		}
	}
	if session == "" {
		return nil, gerr_session_loss
	}

	self.slocker.Lock()
	if sInfo, ok := self.sessions[session]; !ok {
		err = gerr_session_invaild
	} else if sInfo.waitAuth {
		err = gerr_unauthenticated
	}
	self.slocker.Unlock()
	if err != nil {
		return nil, err
	}

	var conn net.Conn
	if conn, err = net.Dial(req.Remote.Network, req.Remote.Address); err != nil {
		return nil, err
	}

	channel := uuid.New()

	self.slocker.Lock()
	defer self.slocker.Unlock()

	if sInfo, ok := self.sessions[session]; ok {
		sInfo.proxies[channel] = conn
	} else {
		return nil, gerr_session_invaild
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
func (self *AgentServer) Exchange(stream agent.Agent_ExchangeServer) (err error) {
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
		return gerr_session_loss
	}
	if channel == "" {
		return gerr_channel_loss
	}

	var done chan error
	var proxy net.Conn

	//get proxy connection
	self.slocker.Lock()
	if sInfo, ok := self.sessions[session]; ok {
		done = sInfo.done

		if proxy, ok = sInfo.proxies[channel]; ok {
			delete(sInfo.proxies, channel)
		} else {
			err = gerr_channel_invaild
		}
	} else {
		err = gerr_session_invaild
	}
	self.slocker.Unlock()

	if err != nil {
		return err
	}

	pipe := NewStreamPipe(stream)

	return Io_exchange(pipe, proxy, done)
}

//call procedure
func (self *AgentServer) Heartbeat(ctx context.Context, ping *agent.Ping) (pong *agent.Pong, err error) {
	var session string
	if md, ok := metadata.FromContext(ctx); ok {
		ss := md["session"]
		if len(ss) >= 1 {
			session = ss[0]
		}
	}
	if session == "" {
		return nil, gerr_session_loss
	}

	self.slocker.Lock()
	defer self.slocker.Unlock()

	if sInfo, ok := self.sessions[session]; !ok {
		return nil, gerr_session_invaild
	} else {
		sInfo.lastKeep = time.Now()
	}

	pong = &agent.Pong{
		AppData: ping.AppData,
	}
	return pong, err
}

func (self *AgentServer) check() {
	self.slocker.Lock()
	defer self.slocker.Unlock()

	now := time.Now()

	for session, sInfo := range self.sessions {
		if now.Sub(sInfo.lastKeep) > defaultPingMaxDelay {
			sInfo.done <- gerr_heartbeat_timeout
			delete(self.sessions, session)
		}
	}
}

func (self *AgentServer) checkLoop() {
	for _ := range self.pingTicker.C {
		self.check()
	}
}

