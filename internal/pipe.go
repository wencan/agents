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
	"log"
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

	inPackets  chan *agent.DataPacket
	outPackets chan *agent.DataPacket

	readCache  [][]byte
	outCache   []*agent.DataPacket

	reads      chan []byte
	writes     chan []byte

	acks       []uint32
	unAcks     []*unAck

	wBuffer    bytes.Buffer
	rBuffer    bytes.Buffer

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
		//util error(contain eof)
		packet, err := pipe.raw.Recv()
		if err != nil {
			pipe.cancel(err)
			return
		}

		pipe.inPackets <- packet
	}
}

func (pipe *StreamPipe) writeLoop() {
	defer pipe.waitGroup.Done()

	discard := false

	FIRST:
	for {
		select {
		case packet := <-pipe.outPackets:
			err := pipe.raw.Send(packet)
			if err != nil {
				pipe.cancel(err)
				discard = true
			}
		case <-pipe.ctx.Done():
			break FIRST
		}
	}

	//handle remain packet
	for {
		select {
		case packet := <-pipe.outPackets:
			if discard {
				log.Println("discard packet, No:", packet.No)
				break
			}

			err := pipe.raw.Send(packet)
			if err != nil {
				pipe.cancel(err)
				discard = true
			}
		default:
			return
		}
	}
}

func (pipe *StreamPipe) preparePacket(first []byte) (packet *agent.DataPacket, err error) {
	if first != nil {
		_, err = pipe.wBuffer.Write(first)
		if err != nil {
			return nil, err
		}
	}

	packet = pipe.newPacket()

	cap := defaultPacketMaxBytes
	DONE:
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

		select {
		case buff := <-pipe.writes:
			len := intMin(len(buff), cap)
			packet.Buff = append(packet.Buff, buff[:len]...)
			cap -= len

			pipe.wBuffer = *bytes.NewBuffer(buff[len:])
		default:
			break DONE
		}
	}

	packet.Acks = pipe.acks
	pipe.acks = []uint32{}

	if len(packet.Buff) > 0 {
		unack := newUnAck(packet.No)
		pipe.unAcks = append(pipe.unAcks, unack)
	}

	if len(packet.Buff) == 0 && len(packet.Acks) == 0 {
		return nil, nil
	}

	return packet, nil
}

func (pipe *StreamPipe) handleInPacket(packet *agent.DataPacket) (err error) {
	if len(packet.Buff) > 0 {
		if len(pipe.readCache) > 0 {
			pipe.readCache = append(pipe.readCache, packet.Buff)
		} else {
			//must non-block
			select {
			case pipe.reads <- packet.Buff:
			default:
				pipe.readCache = append(pipe.readCache, packet.Buff)
				break
			}
		}

		//need ack
		pipe.acks = append(pipe.acks, packet.No)
	}

	//ack sended packet
	if len(pipe.unAcks) < len(packet.Acks) {
		err = errors.New("ack error")
		return
	}
	unacks := pipe.unAcks[:len(packet.Acks)]
	pipe.unAcks = pipe.unAcks[len(packet.Acks):]
	for idx, unack := range unacks {
		if packet.Acks[idx] != unack.no {
			err = errors.New("ack missmatch")
			return
		}
	}

	//send ack
	if len(pipe.acks) > 0 {
		err = pipe.handleWrite(nil)
		if err != nil {
			return err
		}
	}

	return
}

//allow buff == nil
func (pipe *StreamPipe) handleWrite(buff []byte) (err error) {
	var packet *agent.DataPacket
	for {
		packet, err = pipe.preparePacket(buff)
		buff = nil
		if err != nil {
			return
		}
		if packet == nil {
			return
		}

		if len(pipe.outCache) > 0 {
			pipe.outCache = append(pipe.outCache, packet)
		} else {
			//must non-block
			select {
			case pipe.outPackets <- packet:
			default:
				pipe.outCache = append(pipe.outCache, packet)
				break
			}
		}
	}
	return
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

func (pipe *StreamPipe) handleCache() {
	FIRST:
	for {
		if len(pipe.readCache) == 0 {
			break
		}

		buff := pipe.readCache[0]
		select {
		case pipe.reads <- buff:
			pipe.readCache = pipe.readCache[1:]
		default:
			break FIRST
		}
	}

	SECOND:
	for {
		if len(pipe.outCache) == 0 {
			break
		}

		packet := pipe.outCache[0]
		select {
		case pipe.outPackets <- packet:
			pipe.outCache = pipe.outCache[1:]
		default:
			break SECOND
		}
	}
}

func (pipe *StreamPipe) loop() {
	defer pipe.waitGroup.Done()

	FIRST:
	for {
		pipe.handleCache()

		select {
		case packet := <-pipe.inPackets:
			err := pipe.handleInPacket(packet)
			if err != nil {
				pipe.cancel(err)
				return
			}
		case buff := <-pipe.writes:
			err := pipe.handleWrite(buff)
			if err != nil {
				pipe.cancel(err)
				return
			}
		case <-pipe.ackChecker.C:
			err := pipe.ackCheck()
			if err != nil {
				pipe.cancel(err)
				return
			}
		case <-pipe.ctx.Done():
			break FIRST
		}
	}

	for {
		pipe.handleCache()

		select {
		case packet := <-pipe.inPackets:
			err := pipe.handleInPacket(packet)
			if err != nil {
				pipe.cancel(err)
				return
			}
		case buff := <-pipe.writes:
			err := pipe.handleWrite(buff)
			if err != nil {
				pipe.cancel(err)
				return
			}
		default:
			return
		}
	}
}

func (pipe *StreamPipe) Err() error {
	pipe.locker.Lock()
	defer pipe.locker.Unlock()

	return pipe.err
}

func (pipe*StreamPipe) cancel(err error) {
	pipe.locker.Lock()
	defer pipe.locker.Unlock()

	if pipe.err != nil {
		return
	}

	pipe.err = err

	pipe.cancelFunc()
}

//unsafe
func (pipe *StreamPipe) Read(buff []byte) (n int, err error) {
	for {
		if pipe.rBuffer.Len() > 0 {
			n, err = pipe.rBuffer.Read(buff)

			//full
			if n == len(buff) {
				return n, err
			}

			//non-block
			select {
			case bs := <-pipe.reads:
				_, err = pipe.rBuffer.Write(bs)
				if err != nil {
					return n, err
				}
			default:
			//not more data
				return n, err
			}

			//reread
			continue
		}

		//block
		select {
		case bs := <-pipe.reads:
			_, err = pipe.rBuffer.Write(bs)
			if err != nil {
				return n, err
			}
		case <-pipe.ctx.Done():
			return n, pipe.Err()
		}
	}

	return n, err
}

//unsafe
func (pipe *StreamPipe) Write(buff []byte) (n int, err error) {
	//check pipe status
	select {
	case <-pipe.ctx.Done():
		return 0, pipe.Err()
	default:
	}

	select {
	case pipe.writes <- buff:
		return len(buff), nil
	case <-pipe.ctx.Done():
		return 0, pipe.Err()
	}

	return 0, nil
}

func (pipe *StreamPipe) CloseWithError(e error) (err error) {
	if pipe.Err() == nil {
		if e == nil {
			e = io.EOF
		}
		pipe.cancel(e)
	}

	//client actively close the stream
	// server wait for the peer to close the stream
	if s, ok := pipe.raw.(grpc.ClientStream); ok {
		if err = s.CloseSend(); err != nil {
			return err
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
