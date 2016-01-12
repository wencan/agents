package socks5

import (
	"net"
	"io"
	"log"
	"bytes"
	"errors"
	"strconv"
	"encoding/binary"
	"runtime"
	"golang.org/x/net/proxy"
	"time"
)

//version
const Version = 0x05

//methods
const (
	MethodNoAuthentication = iota
	MethodGSSAPI
	MethodUsernamePassword
	MethodNoAcceptable = 0xff
)

//CMD
const (
	CMDConnect = iota + 0x01
	CMDBind
	CMDUDPAssociate
)

//address type
const (
	AddrTypeIPv4 = iota + 0x01
	AddrTypeDomainName
	AddrTypeIPv6
)

//reply field
const (
	ReplySucceeded = iota
	ReplyGeneralFailure
	ReplyConnectionNotAllowed
	ReplyNetworkUnreachable
	ReplyHostUnreachable
	ReplyConnectionRefused
	ReplyTTLExpired
	ReplyCMDNotSupported
	ReplyAddrTypeNotSupported
)

type Error struct {
	x     byte
	error string
}

func (self Error) Byte() byte {
	return self.x
}

func (self Error) Error() string {
	return self.error
}

type Server struct {
	Dialer proxy.Dialer
}

func (self Server) ListenAndServe(network, address string) (err error) {
	var listener net.Listener
	if listener, err = net.Listen(network, address); err != nil {
		return
	}
	return self.Serve(listener)
}

func (self Server) Serve(listener net.Listener) (err error) {
	for {
		var conn net.Conn
		if conn, err = listener.Accept(); err != nil {
			return
		}

		go self.serve(conn)
	}
	return
}

func (self Server) serve(conn net.Conn) {
	defer func() {
		conn.Close()

		if r := recover(); r != nil {
			log.Println(r)

			buff := make([]byte, 512)
			runtime.Stack(buff, false)
			log.Println(string(buff))
		}
	}()

	var err error
	if err = self.initialize(conn); err != nil {
		log.Println("Socks5 connection initialize failed,", err)
		return
	}

	var proxy net.Conn
	if proxy, err = self.setup(conn); err != nil {
		log.Println("Socks5 connection setup failed,", err)
		return
	}
	defer proxy.Close()

	if err = self.serveloop(conn, proxy); err != nil {
		log.Println("Socks5 loop error,", err)
	}
}

func (self Server) initialize(conn net.Conn) (err error) {
	//read version, methods field
	buff := make([]byte, 2)
	if err = binary.Read(conn, binary.BigEndian, &buff); err != nil {
		return
	}

	ver := buff[0]
	if ver != Version {
		return errors.New("Socks version missmatch")
	}

	//read methods list
	methods := int(buff[1])
	buff = make([]byte, methods)
	if err = binary.Read(conn, binary.BigEndian, &buff); err != nil {
		return
	}
	if ! bytes.Contains(buff, []byte{MethodNoAuthentication}) {
		return errors.New("socks5 methods list invaild")
	}

	//reply
	buff = []byte{Version, MethodNoAuthentication}
	if err = binary.Write(conn, binary.BigEndian, buff); err != nil {
		return
	}

	return
}

func (self Server) readRequest(conn net.Conn) (network, addr string, err error) {
	buff := make([]byte, 4)
	if err = binary.Read(conn, binary.BigEndian, &buff); err != nil {
		return
	}

	//version
	ver := buff[0]
	if ver != Version {
		err = Error{ReplyGeneralFailure, "Socks version missmatch"}
		return
	}

	//cmd
	cmd := buff[1]
	if cmd != CMDConnect {    //Only support CONNECT
		err = Error{ReplyCMDNotSupported, "Socks 5 command not supported"}
		return
	}

	//address type
	atyp := buff[3]

	//read address
	var host string
	switch atyp {
	case 0x01:    //IPv4
		network = "tcp4"

		ip := make(net.IP, 4)
		if err = binary.Read(conn, binary.BigEndian, &ip); err != nil {
			return
		}

		host = ip.String()

	case 0x03:    //domain
		network = "tcp"

		var len byte
		if err = binary.Read(conn, binary.BigEndian, &len); err != nil {
			return
		}

		domain := make([]byte, len)
		if err = binary.Read(conn, binary.BigEndian, &domain); err != nil {
			return
		}

		host = string(domain)

	case 0x04:    //IPv6
		network = "tcp6"

		ip := make(net.IP, 16)
		if err = binary.Read(conn, binary.BigEndian, &ip); err != nil {
			return
		}

		host = ip.String()

	default:
		err = Error{ReplyAddrTypeNotSupported, "socks5 atyp error"}
		return
	}

	var port uint16
	if err = binary.Read(conn, binary.BigEndian, &port); err != err {
		return
	}

	addr = net.JoinHostPort(host, strconv.Itoa(int(port)))

	return
}

func (self Server) writeReply(conn net.Conn, addr net.Addr, e error) (err error) {
	if e != nil {
		if _e, ok := e.(Error); ok {
			return binary.Write(conn, binary.BigEndian, _e.Byte())
		} else {
			return binary.Write(conn, binary.BigEndian, ReplyGeneralFailure)
		}
	}

	var (
		host string
		ip net.IP
		port uint16

		sport string
		atyp byte
		nport int
	)
	host, sport, err = net.SplitHostPort(addr.String())
	if err != nil {
		//dummy
		atyp = 0x03
		host = addr.String()
		port = 0
	} else {
		ip = net.ParseIP(host)

		if ip == nil {
			atyp = 0x03        //domain
		} else {
			if ip = ip.To4(); ip != nil {
				atyp = 0x01        //IPv4
			} else if ip = ip.To16(); ip != nil {
				atyp = 0x04        //IPv6
			} else {
				err = errors.New("local address error")
				return
			}
		}

		if nport, err = net.LookupPort(addr.Network(), sport); err != nil {
			return
		}
		port = uint16(nport)
	}

	//write version, rep, reserved, address type
	head := []byte{
		Version,
		ReplySucceeded,
		0x00,
		atyp,
	}
	if err = binary.Write(conn, binary.BigEndian, head); err != nil {
		return
	}

	//write domain or ip
	if ip != nil {
		if err = binary.Write(conn, binary.BigEndian, ip); err != nil {
			return
		}
	} else {
		data := []byte(host)
		if err = binary.Write(conn, binary.BigEndian, byte(len(data))); err != nil {
			return
		}

		if err = binary.Write(conn, binary.BigEndian, data); err != nil {
			return
		}
	}

	//write port
	return binary.Write(conn, binary.BigEndian, port)
}

func (self Server) setup(conn net.Conn) (proxy net.Conn, err error) {
	var network, addr string

	//read
	if network, addr, err = self.readRequest(conn); err == nil {

		//connect
		if proxy, err = self.Dialer.Dial(network, addr); err != nil {
			err = Error{ReplyConnectionRefused, err.Error()}
		}
		defer func() {
			if err != nil && proxy != nil {
				proxy.Close()
				proxy = nil
			}
		}()
	}

	//reply
	var e error
	var laddr net.Addr
	if proxy != nil {
		laddr = proxy.LocalAddr()
	}
	if e = self.writeReply(conn, laddr, err); err == nil && e != nil {
		err = e
	}

	return
}

func (self Server) serveloop(conn, proxy net.Conn) (err error) {
	ch := make(chan error, 2)

	go self.copyLoop(conn, proxy, ch)

	go self.copyLoop(proxy, conn, ch)

	err = <-ch

	conn.SetReadDeadline(time.Now())
	proxy.SetReadDeadline(time.Now())

	<-ch    //Waiting for another exit
	return
}

func (self Server) copyLoop(conn1, conn2 net.Conn, ch chan error) {
	_, err := io.Copy(conn1, conn2)
	ch <- err
}
