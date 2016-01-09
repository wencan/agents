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
	"unsafe"
	"sync/atomic"
)

const (
	selectInPackets = iota
	selectReads
	selectOutPackets
	selectWrites
	selectAckChecker
	selectContext
	selectDefault
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

	err        unsafe.Pointer

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

	//init pipe.err as nil
	var right error
	atomic.StorePointer(&pipe.err, unsafe.Pointer(&right))

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
			break
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
	SECOND:
	for {
		select {
		case packet := <-pipe.outPackets:
			if discard {
				//log.Println("discard packet, No:", packet.No)
				break
			}

			err := pipe.raw.Send(packet)
			if err != nil {
				pipe.cancel(err)
				discard = true
			}
		default:
			break SECOND
		}
	}

	if !discard {
		//client actively close the stream
		//server wait for the peer to close the stream
		if s, ok := pipe.raw.(grpc.ClientStream); ok {
			if err := s.CloseSend(); err != nil {
				pipe.cancel(err)
			}
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
	for cap >= 0 {
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
		pipe.readCache = append(pipe.readCache, packet.Buff)

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

		pipe.outCache = append(pipe.outCache, packet)
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

func (pipe *StreamPipe) selectCases(block bool) (label int, err error) {
	var cases []reflect.SelectCase
	var labels []int

	//pipe.inPackets
	cases = append(cases, reflect.SelectCase{
		Dir: reflect.SelectRecv,
		Chan: reflect.ValueOf(pipe.inPackets),
	})
	labels = append(labels, selectInPackets)

	//pipe.writes
	cases = append(cases, reflect.SelectCase{
		Dir: reflect.SelectRecv,
		Chan: reflect.ValueOf(pipe.writes),
	})
	labels = append(labels, selectWrites)

	//pipe.ackChecker
	cases = append(cases, reflect.SelectCase{
		Dir: reflect.SelectRecv,
		Chan: reflect.ValueOf(pipe.ackChecker.C),
	})
	labels = append(labels, selectAckChecker)

	//pipe.reads
	if len(pipe.readCache) > 0 {
		cases = append(cases, reflect.SelectCase{
			Dir: reflect.SelectSend,
			Chan: reflect.ValueOf(pipe.reads),
			Send: reflect.ValueOf(pipe.readCache[0]),
		})
		labels = append(labels, selectReads)
	}

	//send pipe.outPackets
	if len(pipe.outCache) > 0 {
		cases = append(cases, reflect.SelectCase{
			Dir: reflect.SelectSend,
			Chan: reflect.ValueOf(pipe.outPackets),
			Send: reflect.ValueOf(pipe.outCache[0]),
		})
		labels = append(labels, selectOutPackets)
	}

	if block {
		//pipe.ctx.Done()
		cases = append(cases, reflect.SelectCase{
			Dir: reflect.SelectRecv,
			Chan: reflect.ValueOf(pipe.ctx.Done()),
		})
		labels = append(labels, selectContext)
	} else {
		cases = append(cases, reflect.SelectCase{
			Dir: reflect.SelectDefault,
		})
		labels = append(labels, selectDefault)
	}

	chosen, recv, recvOk := reflect.Select(cases)
	label = labels[chosen]
	switch label {
	case selectInPackets:
		if !recvOk {
			break
		}
		packet := recv.Interface().(*agent.DataPacket)
		err = pipe.handleInPacket(packet)
	case selectWrites:
		if !recvOk {
			break
		}
		buff := recv.Interface().([]byte)
		err = pipe.handleWrite(buff)
	case selectAckChecker:
		err = pipe.ackCheck()
	case selectReads:
		pipe.readCache = pipe.readCache[1:]
	case selectOutPackets:
		pipe.outCache = pipe.outCache[1:]
	case selectDefault:
		//default
	}

	return
}

func (pipe *StreamPipe) loop() {
	defer pipe.waitGroup.Done()

	for {
		chosen, err := pipe.selectCases(true)
		if err != nil {
			pipe.cancel(err)
			break
		}
		if chosen == selectContext {
			break
		}
	}

	for {
		chosen, err := pipe.selectCases(false)
		if err != nil {
			pipe.cancel(err)
			break
		}
		if chosen == selectDefault {
			break
		}
	}
}

func (pipe *StreamPipe) Err() (err error) {
	err = *(*error)(atomic.LoadPointer(&pipe.err))
	if err != nil {
		return err
	}

	return pipe.ctx.Err()
}

func (pipe*StreamPipe) cancel(err error) {
	if err == nil {
		panic("context: internal error: missing cancel error")
	}

	atomic.CompareAndSwapPointer(&pipe.err, nil, unsafe.Pointer(&err))

	if pipe.ctx.Err() != nil {
		//already canceled
		return
	}
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
			if n > 0 {
				return n, nil
			} else {
				return n, pipe.Err()
			}
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
	if e == nil {
		e = io.EOF
	}
	pipe.cancel(e)

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
