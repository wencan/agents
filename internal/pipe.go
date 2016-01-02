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

func intMin(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

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

//wrap grpc stream as net.Conn
type StreamPipe struct {
	ctx        context.Context
	cancelFunc context.CancelFunc

	raw        agentStream

	waitGroup  sync.WaitGroup

	inPackets chan *agent.DataPacket
	outPackets chan *agent.DataPacket
	reads	chan []byte
	writes	chan []byte

	acks	[]uint32
	unAcks	[]*unAck

	wBuffer bytes.Buffer
	rBuffer bytes.Buffer

	locker     sync.Mutex
	err        error

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
		inPackets: make(chan *agent.DataPacket, 10),
		outPackets: make(chan *agent.DataPacket, 10),
		reads: make(chan []byte, 10),
		writes: make(chan []byte, 10),
		ackChecker: time.NewTicker(defaultAckCheckDelay),
	}

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


func (pipe *StreamPipe) readLoop() {
	defer pipe.waitGroup.Done()

	for {
		packet, err := pipe.raw.Recv()
		if err != nil {
			pipe.setErr(err)
			return
		}

		pipe.inPackets <- packet
	}
}

func (pipe *StreamPipe) writeLoop() {
	defer pipe.waitGroup.Done()

	for {
		select {
		case packet := <- pipe.outPackets:
			err := pipe.raw.Send(packet)
			if err != nil {
				pipe.setErr(err)
				return
			}
		case <- pipe.ctx.Done():
			return
		}
	}
}

func (pipe *StreamPipe) preparePacket(first []byte) (packet *agent.DataPacket, err error) {
	packet = pipe.newPacket()

	if first != nil {
		packet.Buff = first
	}

	cap := defaultPacketMaxBytes
	for {
		if cap == 0 {
			break
		}

		if pipe.wBuffer.Len() > 0 {
			len := intMin(cap, pipe.wBuffer.Len())
			buff := make([]byte, len)

			nr := 0
			nr, err = pipe.wBuffer.Read(buff)
			if err != nil {
				return nil, err
			}

			packet.Buff = append(packet.Buff, buff[:nr]...)
			cap -= nr

			continue
		}

		if len(pipe.writes) == 0 {
			break
		}

		buff := <- pipe.writes
		len := intMin(len(buff), cap)
		packet.Buff = append(packet.Buff, buff[:len]...)
		cap -= len

		pipe.wBuffer = *bytes.NewBuffer(buff[len:])
	}

	packet.Acks = pipe.acks
	pipe.acks = []uint32{}

	if len(packet.Buff) > 0 {
		unack := newUnAck(packet.No)
		pipe.unAcks = append(pipe.unAcks, unack)
	}

	return packet, nil
}

func (pipe *StreamPipe) handleInPacket(packet *agent.DataPacket) (err error) {
	if len(packet.Buff) > 0 {
		pipe.reads <- packet.Buff

		//need ack
		pipe.acks = append(pipe.acks, packet.No)
	}

	//ack sended packet
	if len(pipe.unAcks) < len(packet.Acks) {
		return errors.New("ack error")
	}
	unacks := pipe.unAcks[:len(packet.Acks)]
	pipe.unAcks = pipe.unAcks[len(packet.Acks):]
	for idx, unack := range unacks {
		if packet.Acks[idx] != unack.no {
			return errors.New("ack missmatch")
		}
	}

	//send ack
	if len(pipe.acks) > 0 {
		packet, err = pipe.preparePacket(nil)
		if err != nil {
			return err
		}

		pipe.outPackets <- packet
	}

	return nil
}

func (pipe *StreamPipe) handleWrite(buff []byte) (err error) {
	var packet *agent.DataPacket
	packet, err = pipe.preparePacket(buff)
	if err != nil {
		return err
	}

	pipe.outPackets <- packet
	return nil
}

func (pipe *StreamPipe) ackCheck() error {
	if len(pipe.unAcks) == 0 {
		return nil
	}

	t := pipe.unAcks[0].t
	if time.Now().Sub(t) > defaultAckMaxDelay {
		return errors.New("ack timeout")
	}

	return nil
}

func (pipe *StreamPipe) loop() {
	defer pipe.waitGroup.Done()

	for {
		select {
		case packet := <- pipe.inPackets:
			err := pipe.handleInPacket(packet)
			if err != nil {
				pipe.setErr(err)
				return
			}
		case buff := <- pipe.writes:
			err := pipe.handleWrite(buff)
			if err != nil {
				pipe.setErr(err)
				return
			}
		case <-pipe.ackChecker.C:
			err := pipe.ackCheck()
			if err != nil {
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
	if pipe.err == nil {
		pipe.err = err
	}
	pipe.locker.Unlock()

	pipe.cancelFunc()
}

func (pipe *StreamPipe) Read(buff []byte) (n int, err error) {
	for {
		err = pipe.Err()
		if err != nil {
			return n, err
		}

		if pipe.rBuffer.Len() == 0 {
			if len(pipe.reads) > 0 || n == 0 {
				var bs []byte
				select {
				case bs = <- pipe.reads:
				case <- pipe.ctx.Done():
					return n, pipe.Err()
				}

				if bs == nil {
					return n, io.EOF
				}

				pipe.rBuffer = *bytes.NewBuffer(bs)
				if err != nil {
					return n, err
				}
			} else {
				break
			}
		}

		var nr int
		nr, err = pipe.rBuffer.Read(buff)
		n += nr
		if err != nil {
			return n, err
		}

		if len(buff) == nr {
			break
		}

		buff = buff[nr:]
	}

	return n, err
}

func (pipe *StreamPipe) Write(buff []byte) (n int, err error) {
	err = pipe.Err()
	if err != nil {
		return n, err
	}

	pipe.writes <- buff
	return len(buff), nil
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
