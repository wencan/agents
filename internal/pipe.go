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
	"unsafe"
	"sync/atomic"
)

const (
	PipeChannelBuffSize int = 10
	PipeAcksMaxSize int = 100
)

var bufPool *BufPool = NewBufPool(1024 * 1024 * 1024)

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

const (
	cmdPushAck = iota
	cmdPullAck
	cmdPopUnack
	cmdPushUnack
)

type command struct {
	cmd     int
	inAcks  []uint32
	outAcks chan uint32
}

//wrap grpc stream as net.Conn
type StreamPipe struct {
	ctx         context.Context
	cancelFunc  context.CancelFunc

	raw         agentStream
	cc          *ClientConn //may is nil

	waitGroup   sync.WaitGroup
	ioComplete  chan int

	reads       chan []byte
	writes      chan []byte
	writeable   chan int
	writeFlush  chan int

	cmds        chan *command

	acks        []uint32
	unAcks      []*unAck

	wBuffer     bytes.Buffer
	rCache      bytes.Buffer

	err         unsafe.Pointer

	acksChecker *time.Ticker

	serial      uint32
}

func NewStreamPipe(stream agentStream) *StreamPipe {
	ctx, cancelFunc := context.WithCancel(context.Background())
	pipe := &StreamPipe{
		ctx: ctx,
		cancelFunc:cancelFunc,
		raw: stream,
		ioComplete: make(chan int, 2),
		reads: make(chan []byte, PipeChannelBuffSize),
		writes: make(chan []byte, PipeChannelBuffSize),
		writeable: make(chan int, PipeChannelBuffSize),
		writeFlush: make(chan int, 1),
		cmds: make(chan *command, PipeChannelBuffSize),
		acksChecker: time.NewTicker(defaultAckCheckDelay),
	}

	pipe.waitGroup.Add(3)
	pipe.ioComplete <- 1
	pipe.ioComplete <- 1
	go pipe.readLoop()
	go pipe.writeLoop()
	go pipe.loop()

	return pipe
}

func (pipe *StreamPipe) Attach(cc *ClientConn) {
	pipe.cc = cc
}

func (pipe *StreamPipe) incrSerial() uint32 {
	pipe.serial++
	pipe.serial = pipe.serial & ^uint32(0)
	return pipe.serial
}

func (pipe *StreamPipe) newPacket() (packet *agent.DataPacket) {
	packet = &agent.DataPacket{
		No: pipe.incrSerial(),
	}
	return
}

func (pipe *StreamPipe) readLoop() {
	defer func() {
		pipe.waitGroup.Done()
		close(pipe.reads)
		<-pipe.ioComplete
	}()

	for {
		//util error(contain eof)
		packet, err := pipe.raw.Recv()
		if err != nil {
			pipe.cancel(err)
			break
		}

		//pop unack
		if len(packet.Acks) > 0 {
			cmd := &command{
				cmd: cmdPopUnack,
				inAcks: packet.Acks,
			}
			pipe.cmds <- cmd
		}

		//push ack and buf
		if len(packet.Buf) > 0 {
			cmd := &command{
				cmd: cmdPushAck,
				inAcks: []uint32{packet.No},
			}
			pipe.cmds <- cmd

			pipe.reads <- packet.Buf
		}
	}
}

//allow first == nil
func (pipe *StreamPipe) handleWrite(first []byte) (err error) {
	//until no need write
	FIRST:
	for {
		storage := bufPool.Get(defaultPacketMaxBytes)
		pos := 0

		//loop a times
		//SECOND:
		for true {
			if pipe.wBuffer.Len() > 0 {
				nr, err := pipe.wBuffer.Read(storage)
				if err != nil {
					return err
				}

				pos += nr
				if pos == len(storage) {    //full
					if first != nil && len(first) == 0 {
						_, err = pipe.wBuffer.Write(first)
						if err != nil {
							return err
						}
					}
					break    //SECOND
				}
			}

			//now, pipe.wBuffer is empty

			if first != nil && len(first) > 0 {
				nc := copy(storage[pos:], first)
				if len(first) > nc {
					pipe.wBuffer = *bytes.NewBuffer(first[nc:])
				} else {
					bufPool.Put(first)
				}
				first = nil

				pos += nc
				if pos == len(storage) {    //full
					break    //SECOND
				}
			}

			THIRD:
			for {
				//non-block
				select {
				case buf, ok := <-pipe.writes:
					if !ok {
						//pipe.writes is closed
						break THIRD
					}
					<-pipe.writeable

					nc := copy(storage[pos:], buf)

					if len(buf) > nc {
						pipe.wBuffer = *bytes.NewBuffer(buf[nc:])
					} else {
						bufPool.Put(buf)
					}

					pos += nc
					if pos == len(storage) {    //full
						break THIRD
					}
				default:
					break THIRD
				}
			}

			break    //SECOND
		}

		//request ack
		cmd := &command{
			cmd: cmdPullAck,
			outAcks: make(chan uint32),
		}
		pipe.cmds <- cmd

		acks := []uint32{}
		for ack := range cmd.outAcks {
			acks = append(acks, ack)
		}

		if pos == 0 && len(acks) == 0 {
			//no need write
			bufPool.Put(storage)
			break FIRST
		}

		packet := pipe.newPacket()
		packet.Buf = storage[:pos]
		packet.Acks = acks

		if pos > 0 {
			//need ack
			cmd = &command{
				cmd: cmdPushUnack,
				inAcks: []uint32{packet.No},
			}
			pipe.cmds <- cmd
		}

		err = pipe.raw.Send(packet)
		bufPool.Put(storage)
		if err != nil {
			return err
		}
	}

	return nil
}

func (pipe *StreamPipe) writeLoop() {
	defer func() {
		pipe.waitGroup.Done()
		<-pipe.ioComplete
	}()

	var err error = nil

	//util pipe.writes is closed
	FIRST:
	for {
		select {
		case buf, ok := <-pipe.writes:
			if !ok {
				//pipe.writes is closed
				break FIRST
			}

			<-pipe.writeable
			err = pipe.handleWrite(buf)
			if err != nil {
				pipe.cancel(err)
				break FIRST
			}
		case <-pipe.writeFlush:
			err = pipe.handleWrite(nil)
			if err != nil {
				pipe.cancel(err)
				break FIRST
			}
		}
	}

	if err != nil {
		//discard packet
		for _ = range pipe.writes {}
	} else {
		//client actively close the stream
		//server wait for the peer to close the stream
		if s, ok := pipe.raw.(grpc.ClientStream); ok {
			if err := s.CloseSend(); err != nil {
				pipe.cancel(err)
			}
		}
	}
}

func (pipe *StreamPipe) ackCheck() error {
	if len(pipe.unAcks) == 0 {
		return nil
	}

	t := pipe.unAcks[0].t
	if time.Now().Sub(t) > defaultAckMaxDelay {
		return ErrAckTimeout
	}

	return nil
}

func (pipe *StreamPipe) handleCommand(cmd *command) (err error) {
	switch cmd.cmd {
	case cmdPushAck:
		pipe.acks = append(pipe.acks, cmd.inAcks...)
		if len(pipe.writes) == 0 {
			//Do not repeat flush
			select {
			case pipe.writeFlush <- 1:
			default:
			}
		}
	case cmdPullAck:
		uplimit := intMin(len(pipe.acks), PipeAcksMaxSize)
		acks := pipe.acks[:uplimit]
		pipe.acks = pipe.acks[uplimit:]

		for _, ack := range acks {
			cmd.outAcks <- ack
		}
		close(cmd.outAcks)
	case cmdPushUnack:
		for _, unack := range cmd.inAcks {
			unAck := newUnAck(unack)
			pipe.unAcks = append(pipe.unAcks, unAck)
		}
	case cmdPopUnack:
		lenght := len(cmd.inAcks)
		if lenght > len(pipe.unAcks) {
			return errors.New("ack number error")
		}
		unAcks := pipe.unAcks[:lenght]
		pipe.unAcks = pipe.unAcks[lenght:]

		for i, unack := range cmd.inAcks {
			if unack != unAcks[i].no {
				return errors.New("ack error")
			}
		}
	}
	return nil
}

//main loop
func (pipe *StreamPipe) loop() {
	defer pipe.waitGroup.Done()

	var err error = nil

	FIRST:
	for {
		select {
		case cmd := <-pipe.cmds:
			err = pipe.handleCommand(cmd)
		case <-pipe.acksChecker.C:
			err = pipe.ackCheck()
		case <-pipe.ctx.Done():
			break FIRST
		}

		if err != nil {
			pipe.cancel(err)
			break
		}
	}

	//util writerLoop and readLoop exit
	counting := cap(pipe.ioComplete)
	SECOND:
	for {
		select {
		case cmd := <-pipe.cmds:
			if cmd.outAcks != nil {
				close(cmd.outAcks)
			}
		case pipe.ioComplete <- 1:
			counting--
			if counting == 0 {
				//io completed
				break SECOND
			}
		}
	}
}

func (pipe *StreamPipe) Err() (err error) {
	p := (*error)(atomic.LoadPointer(&pipe.err))
	if p != nil {
		return *p
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
	close(pipe.writes)
}

//unsafe
func (pipe *StreamPipe) Read(buf []byte) (n int, err error) {
	if pipe.rCache.Len() > 0 {
		n, err = pipe.rCache.Read(buf)
		if err != nil {
			return n, err
		}

		//full or error
		if n == len(buf) || err != nil {
			return n, err
		}
	}

	//non-block
	FIRST:
	for {
		select {
		case bs, ok := <-pipe.reads:
			if !ok {
				//pipe.reads is closed
				if n > 0 {
					return n, nil
				} else {
					return n, pipe.Err()
				}
			}

			var nc int
			nc = copy(buf[n:], bs)
			n += nc

			if nc < len(bs) {    //buf must is full
				pipe.rCache = *bytes.NewBuffer(bs[nc:])
			}

		//full or error
			if n == len(buf) || err != nil {
				return n, err
			}
		default:
		//non-block
			break FIRST
		}
	}

	if n > 0 {
		return n, err
	}

	//wait data coming
	for {
		select {
		case bs, ok := <-pipe.reads:
			if !ok {
				//pipe.reads is closed
				return n, pipe.Err()        //n == 0
			}

			for {
				var nc int
				nc = copy(buf[n:], bs)
				n += nc

				if nc < len(bs) {    //buf must is full
					pipe.rCache = *bytes.NewBuffer(bs[nc:])
				}

				//full or error
				if n == len(buf) || err != nil {
					return n, err
				}

				select {
				case bs, ok = <-pipe.reads:
					if !ok {
						//pipe.reads is closed
						if n > 0 {
							return n, nil
						} else {
							return n, pipe.Err()
						}
					}
				default:
				//no more data
					return n, nil
				}
			}
		case <-pipe.ctx.Done():
			return n, pipe.Err()    //n == 0
		}
	}

	return n, err
}

//unsafe
func (pipe *StreamPipe) Write(buf []byte) (n int, err error) {
	//safe write
	select {
	case <-pipe.ctx.Done():
		return 0, pipe.Err()
	case pipe.writeable <- 1:
			select {
			case <-pipe.ctx.Done():
				return 0, pipe.Err()
			default:
				mem := bufPool.Get(len(buf))
				nc := copy(mem, buf)
				pipe.writes <- mem
				return nc, nil
			}
	}

	return 0, nil
}

func (pipe *StreamPipe) CloseWithError(e error) (err error) {
	if e == nil {
		e = io.EOF
	}
	pipe.cancel(e)

	pipe.acksChecker.Stop()

	pipe.waitGroup.Wait()

	if pipe.cc != nil {
		return pipe.cc.Close()
	}

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
	go func() {
		ctx, _ := context.WithDeadline(pipe.ctx, t)
		<-ctx.Done()
		pipe.cancel(ctx.Err())
	}()
	return nil
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
