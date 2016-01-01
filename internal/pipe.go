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
	"reflect"
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

//reference: https://golang.org/src/io/pipe.go
//wrap grpc stream as net.Conn
//very inefficient
type StreamPipe struct {
	ctx        context.Context
	cancelFunc context.CancelFunc

	raw        agentStream

	waitGroup  sync.WaitGroup

	rLocker    sync.Mutex
	rWait      sync.Cond
	rBuffer    bytes.Buffer

	wLocker    sync.Mutex
	wWait      sync.Cond
	wBuffer    bytes.Buffer
	acks       []uint32

	locker     sync.Mutex
	err        error
	unacks     []*unAck

	ackChecker *time.Ticker

	serial     uint32
}

func NewStreamPipe(ctx context.Context, stream agentStream) *StreamPipe {
	var cancelFunc context.CancelFunc
	ctx, cancelFunc = context.WithCancel(ctx)
	pipe := &StreamPipe{
		ctx: ctx,
		cancelFunc:cancelFunc,
		raw: stream,
		ackChecker: time.NewTicker(defaultAckCheckDelay),
	}

	pipe.rWait = sync.Cond{L: &pipe.rLocker}
	pipe.wWait = sync.Cond{L: &pipe.wLocker}

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
	pipe.rWait.Signal()

	if len(packet.Buff) > 0 {
		pipe.wLocker.Lock()
		pipe.acks  = append(pipe.acks, packet.No)
		pipe.wLocker.Unlock()
		pipe.wWait.Signal()
	}

	if len(packet.Acks) > 0 {
		acksNo, ok := pipe.popUnacks(len(packet.Acks))
		if ok {
			ok = reflect.DeepEqual(packet.Acks, acksNo)
		}
		if !ok {
			return errors.New("ack error")
		}
	}

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

func (pipe *StreamPipe) waitWrite() (packet *agent.DataPacket, err error) {
	pipe.wLocker.Lock()
	defer pipe.wLocker.Unlock()

	for {
		err = pipe.Err()
		if err != nil {
			return nil, err
		}

		if pipe.wBuffer.Len() == 0 && len(pipe.acks) == 0 {
			pipe.wWait.Wait()
			continue
		}

		buff := make([]byte, pipe.wBuffer.Len())
		_, err = pipe.wBuffer.Read(buff)
		if err != nil {
			return nil, err
		}

		packet := pipe.newPacket()
		packet.Buff = buff

		packet.Acks = pipe.acks
		pipe.acks = []uint32{}

		return packet, nil
	}
}

func (pipe *StreamPipe) writeLoop() {
	defer pipe.waitGroup.Done()

	for {
		packet, err := pipe.waitWrite()
		if err != nil {
			pipe.setErr(err)
			return
		}

		if len(packet.Buff) > 0 {
			pipe.pushUnack(packet.No)
		}

		err = pipe.raw.Send(packet)
		if err != nil {
			pipe.setErr(err)
			return
		}
	}
}

func (pipe *StreamPipe) pushUnack(no uint32) {
	pipe.locker.Lock()
	defer pipe.locker.Unlock()

	pipe.unacks = append(pipe.unacks, newUnAck(no))
}

func (pipe *StreamPipe) popUnacks(n int) (acksNo []uint32, ok bool) {
	pipe.locker.Lock()
	defer pipe.locker.Unlock()

	if len(pipe.unacks) < n {
		return nil, false
	}

	unacks := pipe.unacks[0:n]
	pipe.unacks = pipe.unacks[n:]

	acksNo = make([]uint32, n)
	for i, unack := range unacks {
		acksNo[i] = unack.no
	}

	return acksNo, true
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
		} else {
			break
		}
	}

	return pipe.rBuffer.Read(buff)
}

func (pipe *StreamPipe) Write(buff []byte) (n int, err error) {
	pipe.wLocker.Lock()
	defer pipe.wLocker.Unlock()

	err = pipe.Err()
	if err != nil {
		return 0, err
	}

	n, err = pipe.wBuffer.Write(buff)
	if err != nil {
		return 0, err
	}

	pipe.wWait.Signal()

	return
}

func (pipe *StreamPipe) CloseWithError(e error) (err error) {
	if pipe.Err() == nil {
		if e == nil {
			e = io.EOF
		}
		pipe.setErr(e)
	}

	pipe.cancelFunc()

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