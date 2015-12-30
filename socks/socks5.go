package socks

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
const SOCKS5_VERSION = 0x05

//methods
const (
	SOCKS5_METHOD_NOAUTHENTICATION = iota
	SOCKS5_METHOD_GSSAPI
	SOCKS5_METHOD_USERNAMEPASSWORD
	SOCKS5_METHOD_NOACCEPTABLE = 0xff
)

//CMD
const (
	SOCKS5_CMD_CONNECT = iota + 0x01
	SOCKS5_CMD_BIND
	SOCKS5_CMD_UDP_ASSOCIATE
)

//address type
const (
	SOCKS5_ATYP_IPV4 = iota + 0x01
	SOCKS5_ATYP_DOMAINNAME
	SOCKS5_ATYP_IPV6
)

//reply field
const (
	SOCKS5_REP_SUCCEEDED = iota
	SOCKS5_REP_GENERAL_FAILURE
	SOCKS5_REP_CONNECTION_NOTALLOWED
	SOCKS5_REP_NETWORK_UNREACHABLE
	SOCKS5_REP_HOST_UNREACHABLE
	SOCKS5_REP_CONNECTION_REFUSED
	SOCKS5_REP_TTL_EXPIRED
	SOCKS5_REP_CMD_NOTSUPPORTED
	SOCKS5_REP_ATYP_NOTSUPPORTED
)

type Socks5Server struct {
	Dialer proxy.Dialer
}

func (self Socks5Server) ListenAndServe(network, address string) (err error) {
	var listener net.Listener
	if listener, err = net.Listen(network, address); err != nil {
		return
	}
	return self.Serve(listener)
}

func (self Socks5Server) Serve(listener net.Listener) (err error) {
	for {
		var conn net.Conn
		if conn, err = listener.Accept(); err != nil {
			return
		}

		go self.serve(conn)
	}
	return
}

func (self Socks5Server) serve(conn net.Conn) {
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
	if err = self.serve_initialize(conn); err != nil {
		log.Println("Socks5 connection initialize failed,", err)
		return
	}

	var proxy net.Conn
	if proxy, err = self.serve_setup(conn); err != nil {
		log.Println("Socks5 connection setup failed,", err)
		return
	}
	defer proxy.Close()

	if err = self.serve_loop(conn, proxy); err != nil {
		log.Println("Socks5 loop error,", err)
	}
}

func (self Socks5Server) serve_initialize(conn net.Conn) (err error) {
	//read version, methods field
	buff := make([]byte, 2)
	if err = binary.Read(conn, binary.BigEndian, &buff); err != nil {
		return
	}

	ver := buff[0]
	if ver != SOCKS5_VERSION {
		return errors.New("Socks version missmatch")
	}

	//read methods list
	methods := int(buff[1])
	buff = make([]byte, methods)
	if err = binary.Read(conn, binary.BigEndian, &buff); err != nil {
		return
	}
	if ! bytes.Contains(buff, []byte{SOCKS5_METHOD_NOAUTHENTICATION}) {
		return errors.New("socks5 methods list invaild")
	}

	//reply
	buff = []byte{SOCKS5_VERSION, SOCKS5_METHOD_NOAUTHENTICATION}
	if err = binary.Write(conn, binary.BigEndian, buff); err != nil {
		return
	}

	return
}

func (self Socks5Server) serve_read_request(conn net.Conn) (network, addr string, err error) {
	buff := make([]byte, 4)
	if err = binary.Read(conn, binary.BigEndian, &buff); err != nil {
		return
	}

	//version
	ver := buff[0]
	if ver != SOCKS5_VERSION {
		err = Error{SOCKS5_REP_GENERAL_FAILURE, "Socks version missmatch"}
		return
	}

	//cmd
	cmd := buff[1]
	if cmd != SOCKS5_CMD_CONNECT {    //Only support CONNECT
		err = Error{SOCKS5_REP_CMD_NOTSUPPORTED, "Socks 5 command not supported"}
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
		err = Error{SOCKS5_REP_ATYP_NOTSUPPORTED, "socks5 atyp error"}
		return
	}

	var port uint16
	if err = binary.Read(conn, binary.BigEndian, &port); err != err {
		return
	}

	addr = net.JoinHostPort(host, strconv.Itoa(int(port)))

	return
}

func (self Socks5Server) serve_write_reply(conn net.Conn, addr net.Addr, e error) (err error) {
	if e != nil {
		if _e, ok := e.(Error); ok {
			return binary.Write(conn, binary.BigEndian, _e.Byte())
		} else {
			return binary.Write(conn, binary.BigEndian, SOCKS5_REP_GENERAL_FAILURE)
		}
	}

	var (
		host string
		sport string
	)
	if host, sport, err = net.SplitHostPort(addr.String()); err != nil {
		return
	}

	ip := net.ParseIP(host)

	var atyp byte
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

	var nport int
	if nport, err = net.LookupPort(addr.Network(), sport); err != nil {
		return
	}
	port := uint16(nport)

	//write version, rep, reserved, address type
	head := []byte{
		SOCKS5_VERSION,
		SOCKS5_REP_SUCCEEDED,
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

func (self Socks5Server) serve_setup(conn net.Conn) (proxy net.Conn, err error) {
	var network, addr string

	//read
	if network, addr, err = self.serve_read_request(conn); err == nil {

		//connect
		if proxy, err = self.Dialer.Dial(network, addr); err != nil {
			err = Error{SOCKS5_REP_CONNECTION_REFUSED, err.Error()}
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
	if e = self.serve_write_reply(conn, laddr, err); err == nil && e != nil {
		err = e
	}

	return
}

func (self Socks5Server) serve_loop(conn, proxy net.Conn) (err error) {
	ch := make(chan error, 2)

	go self.serve_copy_loop(conn, proxy, ch)

	go self.serve_copy_loop(proxy, conn, ch)

	err = <-ch

	conn.SetReadDeadline(time.Now())
	proxy.SetReadDeadline(time.Now())

	<-ch    //Waiting for another exit
	return
}

func (self Socks5Server) serve_copy_loop(conn1, conn2 net.Conn, ch chan error) {
	_, err := io.Copy(conn1, conn2)
	ch <- err
}
