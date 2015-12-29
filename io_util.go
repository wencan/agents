package agents

import (
	"./agent"
	"google.golang.org/grpc"
	"sync"
	"bytes"
	"io"
	"cmd/link/internal/ld"
	"time"
	"errors"
	"net"
)

type agentStream interface {
	Send(*agent.DataPacket) error
	Recv() (*agent.DataPacket, error)
}

type serial_number uint32

type unAck struct {
	no serial_number
	t	time.Time
}

func newUnAck(no serial_number) *unAck {
	return &unAck{
		no: no,
		t: time.Now(),
	}
}

// reference: https://golang.org/src/io/self.go
type StreamPipe struct {
	raw       agentStream

	waitGroup sync.WaitGroup

	rLocker   sync.Mutex
	rWait     sync.Cond
	rBuffer   bytes.Buffer

	wLocker   sync.Mutex
	wWait     sync.Cond
	wBuffer   bytes.Buffer

	acks      chan serial_number

	locker    sync.Locker
	err       error
	unacks    []*unAck

	ackChecker	*time.Ticker

	serial    serial_number
}

func NewStreamPipe(stream agentStream) *StreamPipe {
	pipe := &StreamPipe{
		raw: stream,
		ackChecker: time.NewTicker(defaultAckCheckDelay),
	}

	pipe.rWait = sync.Cond{L: pipe.rLocker}
	pipe.rWait = sync.Cond{L: pipe.wLocker}

	pipe.waitGroup.Add(3)
	go pipe.read_loop()
	go pipe.write_loop()
	go pipe.ack_check_loop()

	return pipe
}

func (self *StreamPipe) incrSerial() serial_number {
	self.serial++
	self.serial = self.serial & ^serial_number(0)
	return self.serial
}

func (self *StreamPipe) newPacket() *agent.DataPacket {
	return &agent.DataPacket{
		No: self.incrSerial(),
	}
}

func (self *StreamPipe) read_once() (err error) {
	packet , err := self.raw.Recv()
	if err != nil {
		return err
	}

	self.rLocker.Lock()
	defer self.rLocker.Unlock()

	_, err = self.rBuffer.Write(packet.Buff)
	if err != nil {
		return err
	}

	for _, ack := range packet.Acks {
		no, ok := self.pop_unack()
		if !ok || ack!=no {
			return errors.New("ack invaild")
		}
	}

	self.acks <- packet.No

	self.rWait.Signal()
	self.wWait.Signal()

	return nil
}

func (self *StreamPipe) read_loop() {
	defer self.waitGroup.Done()

	for {
		if err := self.read_once(); err != nil {
			self.setErr(err)
			return
		}
	}
}

func (self *StreamPipe) write_loop() {
	defer self.waitGroup.Done()

	self.wLocker.Lock()
	defer self.wLocker.Unlock()

	for {
		if self.getErr() != nil {
			return
		}

		if self.wBuffer.Len() == 0 && len(self.acks) == 0 {
			self.wWait.Wait()
			continue
		}

		buff := make([]byte, self.wBuffer.Len())
		_, err := self.wBuffer.Read(buff)
		if err != nil {
			self.setErr(err)
			return
		}

		packet := self.newPacket()
		packet.Buff = buff

		for {
			if len(self.acks) == 0 {
				break
			}
			packet.Acks = append(packet.Acks, <- self.acks)
		}

		err = self.raw.Send(packet)
		if err != nil {
			self.setErr(err)
			return
		}
		self.push_unack(packet.No)
	}
}

func (self *StreamPipe) push_unack(no serial_number) {
	self.locker.Lock()
	defer self.locker.Unlock()

	self.unacks = append(self.unacks, newUnAck(no))
}

func (self *StreamPipe) pop_unack() (no serial_number, ok bool) {
	self.locker.Lock()
	defer self.locker.Unlock()

	if len(self.unacks) == 0 {
		return 0, false
	}

	unack := self.unacks[0]
	self.unacks = self.unacks[1:]
	return unack.no, true
}

func (self *StreamPipe) ack_check() error {
	self.locker.Lock()
	defer self.locker.Unlock()

	now := time.Now()

	for _, unack := range self.unacks {
		if now.Sub(unack.t) > defaultAckMaxDelay {
			return errors.New("ack timeout")
		}
	}
	return nil
}

func (self *StreamPipe) ack_check_loop() {
	defer self.waitGroup.Done()

	for _ := range self.ackChecker.C {
		if err := self.ack_check(); err != nil {
			self.setErr(err)
			return
		}
	}
}

func (self *StreamPipe) getErr() error {
	self.locker.Lock()
	defer self.locker.Unlock()

	return self.err
}

func (self*StreamPipe) setErr(err error) {
	self.locker.Lock()
	defer self.locker.Unlock()

	if self.err == nil {
		self.err = err
	}
}

func (self *StreamPipe) Read(buff []byte) (n int, err error) {
	self.rLocker.Lock()
	defer self.rLocker.Unlock()

	for {
		if err = self.getErr(); err != nil {
			return
		}

		if self.rBuffer.Len() == 0 {
			self.rWait.Wait()
			continue
		}
	}

	return self.rBuffer.Read(buff)
}

func (self *StreamPipe) Write(buff []byte) (n int, err error) {
	self.wLocker.Lock()
	defer self.wLocker.Unlock()

	err = self.getErr()
	if err != nil {
		return 0, nil
	}

	_, err = self.wBuffer.Write(buff)
	if err != nil {
		return 0, err
	}

	self.wWait.Signal()

	return
}

func (self *StreamPipe) CloseWithError(e error) (err error) {
	self.locker.Lock()
	defer self.locker.Unlock()

	if e == nil {
		e = io.EOF
	}
	self.setErr(e)

	self.rWait.Signal()
	self.wWait.Signal()

	if s, ok := self.raw.(grpc.ClientStream); ok {
		err = s.CloseSend()
	} else {
		//waiting peer close
		for {
			_, err = self.raw.Recv()
			if err != nil {
				break
			}
		}
	}
	if err != nil {
		return err
	}

	self.waitGroup.Wait()
	return nil
}

func (self *StreamPipe) Close() (err error) {
	return self.CloseWithError(nil)
}

func (self *StreamPipe) LocalAddr() net.Addr {
	return &streamPipeAddr(0)
}

func (self *StreamPipe) RemoteAddr() net.Addr {
	return &streamPipeAddr(0)
}

func (self *StreamPipe) SetDeadline(t time.Time) error {
	return errors.New("deadLine not supported")
}

func (self *StreamPipe) SetReadDeadline(t time.Time) error {
	return errors.New("deadLine not supported")
}

func (self *StreamPipe) SetWriteDeadline(t time.Time) error {
	return errors.New("deadLine not supported")
}

type streamPipeAddr int

func (streamPipeAddr) Network() string {
	return "StreamPipe"
}

func (streamPipeAddr) String() string {
	return "StreamPipe"
}

func Io_exchange(pipe *StreamPipe, proxy net.Conn, done chan struct{}) (err error) {
	ch := make(chan error, 2)

	go io_copy_until_error(pipe, proxy, ch)
	go io_copy_until_error(proxy, pipe, ch)

	select {
	case err = <- ch:	//io error
		proxy.Close()
		pipe.CloseWithError(err)
		<- ch
	case <- done:	//session done
		proxy.Close()
		pipe.Close()
		<- ch
		<- ch
	}

	return err
}

func io_copy_until_error(a, b io.ReadWriteCloser, ch chan  struct{}) {
	_, err := io.Copy(a, b)
	ch <- err
}