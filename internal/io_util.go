package internal

import (
	"../agent"
	"google.golang.org/grpc"
	"golang.org/x/net/context"
	"sync"
	"bytes"
	"io"
	"time"
	"errors"
	"net"
)


type agentStream interface {
	Send(*agent.DataPacket) error
	Recv() (*agent.DataPacket, error)
}

type unAck struct {
	no uint32
	t  time.Time
}

func newUnAck(no uint32) *unAck {
	return &unAck{
		no: no,
		t: time.Now(),
	}
}

// reference: https://golang.org/src/io/pipe.go
type StreamPipe struct {
	ctx        context.Context

	raw        agentStream

	waitGroup  sync.WaitGroup

	rLocker    sync.Mutex
	rWait      sync.Cond
	rBuffer    bytes.Buffer

	wLocker    sync.Mutex
	wWait      sync.Cond
	wBuffer    bytes.Buffer

	acks       chan uint32

	locker     sync.Locker
	err        error
	unacks     []*unAck

	ackChecker *time.Ticker

	serial     uint32
}

func NewStreamPipe(ctx context.Context, stream agentStream) *StreamPipe {
	pipe := &StreamPipe{
		ctx: ctx,
		raw: stream,
		ackChecker: time.NewTicker(defaultAckCheckDelay),
	}

	pipe.rWait = sync.Cond{L: &pipe.rLocker}
	pipe.rWait = sync.Cond{L: &pipe.wLocker}

	pipe.waitGroup.Add(3)
	go pipe.readLoop()
	go pipe.writeLoop()
	go pipe.loop()

	return pipe
}

func (pipe *StreamPipe) incrSerial() uint32 {
	pipe.serial++
	pipe.serial = pipe.serial & ^uint32(0)
	return pipe.serial
}

func (pipe *StreamPipe) newPacket() *agent.DataPacket {
	return &agent.DataPacket{
		No: pipe.incrSerial(),
	}
}

func (pipe *StreamPipe) readOnce() (err error) {
	packet, err := pipe.raw.Recv()
	if err != nil {
		return err
	}

	pipe.rLocker.Lock()
	defer pipe.rLocker.Unlock()

	_, err = pipe.rBuffer.Write(packet.Buff)
	if err != nil {
		return err
	}

	for _, ack := range packet.Acks {
		no, ok := pipe.popUnack()
		if !ok || ack != no {
			return errors.New("ack invaild")
		}
	}

	pipe.acks <- packet.No

	pipe.rWait.Signal()
	pipe.wWait.Signal()

	return nil
}

func (pipe *StreamPipe) readLoop() {
	defer pipe.waitGroup.Done()

	for {
		if err := pipe.readOnce(); err != nil {
			pipe.setErr(err)
			return
		}
	}
}

func (pipe *StreamPipe) writeLoop() {
	defer pipe.waitGroup.Done()

	pipe.wLocker.Lock()
	defer pipe.wLocker.Unlock()

	for {
		if pipe.Err() != nil {
			return
		}

		if pipe.wBuffer.Len() == 0 && len(pipe.acks) == 0 {
			pipe.wWait.Wait()
			continue
		}

		buff := make([]byte, pipe.wBuffer.Len())
		_, err := pipe.wBuffer.Read(buff)
		if err != nil {
			pipe.setErr(err)
			return
		}

		packet := pipe.newPacket()
		packet.Buff = buff

		for {
			if len(pipe.acks) == 0 {
				break
			}
			packet.Acks = append(packet.Acks, <-pipe.acks)
		}

		err = pipe.raw.Send(packet)
		if err != nil {
			pipe.setErr(err)
			return
		}
		pipe.pushUnack(packet.No)
	}
}

func (pipe *StreamPipe) pushUnack(no uint32) {
	pipe.locker.Lock()
	defer pipe.locker.Unlock()

	pipe.unacks = append(pipe.unacks, newUnAck(no))
}

func (pipe *StreamPipe) popUnack() (no uint32, ok bool) {
	pipe.locker.Lock()
	defer pipe.locker.Unlock()

	if len(pipe.unacks) == 0 {
		return 0, false
	}

	unack := pipe.unacks[0]
	pipe.unacks = pipe.unacks[1:]
	return unack.no, true
}

func (pipe *StreamPipe) ackCheck() error {
	pipe.locker.Lock()
	defer pipe.locker.Unlock()

	if len(pipe.unacks) == 0 {
		return nil
	}

	if time.Now().Sub(pipe.unacks[0].t) > defaultAckCheckDelay {
		return errors.New("ack timeout")
	}

	return nil
}

func (pipe *StreamPipe) loop() {
	defer pipe.waitGroup.Done()

	for {
		select {
		case <-pipe.ackChecker.C:
			if err := pipe.ackCheck(); err != nil {
				pipe.setErr(err)
				return
			}
		case <-pipe.ctx.Done():
			pipe.setErr(pipe.ctx.Err())
			return
		}
	}
}

func (pipe *StreamPipe) Err() error {
	pipe.locker.Lock()
	defer pipe.locker.Unlock()

	return pipe.err
}

func (pipe*StreamPipe) setErr(err error) {
	pipe.locker.Lock()
	defer pipe.locker.Unlock()

	if pipe.err == nil {
		pipe.err = err

		pipe.rWait.Signal()
		pipe.wWait.Signal()
	}
}

func (pipe *StreamPipe) Read(buff []byte) (n int, err error) {
	pipe.rLocker.Lock()
	defer pipe.rLocker.Unlock()

	for {
		if err = pipe.Err(); err != nil {
			return
		}

		if pipe.rBuffer.Len() == 0 {
			pipe.rWait.Wait()
			continue
		}
	}

	return pipe.rBuffer.Read(buff)
}

func (pipe *StreamPipe) Write(buff []byte) (n int, err error) {
	pipe.wLocker.Lock()
	defer pipe.wLocker.Unlock()

	err = pipe.Err()
	if err != nil {
		return 0, nil
	}

	_, err = pipe.wBuffer.Write(buff)
	if err != nil {
		return 0, err
	}

	pipe.wWait.Signal()

	return
}

func (pipe *StreamPipe) CloseWithError(e error) (err error) {
	pipe.locker.Lock()
	defer pipe.locker.Unlock()

	if pipe.Err() == nil {
		if e == nil {
			e = io.EOF
		}
		pipe.setErr(e)
	}

	if s, ok := pipe.raw.(grpc.ClientStream); ok {
		//client actively close the stream
		if err = s.CloseSend(); err != nil {
			return err
		}
	} else {
		// server wait for the peer to close the stream
		for {
			_, err = pipe.raw.Recv()
			if err != nil {
				break
			}
		}
	}

	pipe.waitGroup.Wait()
	return nil
}

func (pipe *StreamPipe) Close() (err error) {
	return pipe.CloseWithError(nil)
}

func (pipe *StreamPipe) LocalAddr() net.Addr {
	return streamPipeAddr(0)
}

func (pipe *StreamPipe) RemoteAddr() net.Addr {
	return streamPipeAddr(0)
}

func (pipe *StreamPipe) SetDeadline(t time.Time) error {
	return errors.New("deadLine not supported")
}

func (pipe *StreamPipe) SetReadDeadline(t time.Time) error {
	return errors.New("deadLine not supported")
}

func (pipe *StreamPipe) SetWriteDeadline(t time.Time) error {
	return errors.New("deadLine not supported")
}

type streamPipeAddr int

func (streamPipeAddr) Network() string {
	return "StreamPipe"
}

func (streamPipeAddr) String() string {
	return "StreamPipe"
}

func IoExchange(a, b io.ReadWriteCloser, done chan struct{}) (err error) {
	ch := make(chan error, 2)

	go ioCopyUntilError(a, b, ch)
	go ioCopyUntilError(b, a, ch)

	select {
	case err = <-ch:
	//io error
		if pipe, ok := a.(*StreamPipe); ok {
			pipe.CloseWithError(err)
		} else {
			a.Close()
		}
		if pipe, ok := b.(*StreamPipe); ok {
			pipe.CloseWithError(err)
		} else {
			b.Close()
		}
		<-ch
	case <-done:
	//session done
		a.Close()
		b.Close()
		<-ch
		<-ch
	}

	return err
}

func ioCopyUntilError(a, b io.ReadWriteCloser, ch chan error) {
	_, err := io.Copy(a, b)
	ch <- err
}