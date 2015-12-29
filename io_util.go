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

// reference: https://golang.org/src/io/pipe.go
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
	p := &StreamPipe{
		raw: stream,
		ackChecker: time.NewTicker(defaultAckCheckDelay),
	}

	p.rWait = sync.Cond{L: p.rLocker}
	p.rWait = sync.Cond{L: p.wLocker}

	p.waitGroup.Add(3)
	go p.read_loop()
	go p.write_loop()
	go p.ack_check_loop()

	return p
}

func (p *StreamPipe) incrSerial() serial_number {
	p.serial++
	p.serial = p.serial & ^serial_number(0)
	return p.serial
}

func (p *StreamPipe) newPacket() *agent.DataPacket {
	return &agent.DataPacket{
		No: p.incrSerial(),
	}
}

func (p *StreamPipe) read_once() (err error) {
	packet , err := p.raw.Recv()
	if err != nil {
		return err
	}

	p.rLocker.Lock()
	defer p.rLocker.Unlock()

	_, err = p.rBuffer.Write(packet.Buff)
	if err != nil {
		return err
	}

	for _, ack := range packet.Acks {
		no, ok := p.pop_unack()
		if !ok || ack!=no {
			return errors.New("ack invaild")
		}
	}

	p.acks <- packet.No

	p.rWait.Signal()
	p.wWait.Signal()

	return nil
}

func (p *StreamPipe) read_loop() {
	defer p.waitGroup.Done()

	for {
		if err := p.read_once(); err != nil {
			p.setErr(err)
			return
		}
	}
}

func (p *StreamPipe) write_loop() {
	defer p.waitGroup.Done()

	p.wLocker.Lock()
	defer p.wLocker.Unlock()

	for {
		if p.getErr() != nil {
			return
		}

		if p.wBuffer.Len() == 0 && len(p.acks) == 0 {
			p.wWait.Wait()
			continue
		}

		buff := make([]byte, p.wBuffer.Len())
		_, err := p.wBuffer.Read(buff)
		if err != nil {
			p.setErr(err)
			return
		}

		packet := p.newPacket()
		packet.Buff = buff

		for {
			if len(p.acks) == 0 {
				break
			}
			packet.Acks = append(packet.Acks, <- p.acks)
		}

		err = p.raw.Send(packet)
		if err != nil {
			p.setErr(err)
			return
		}
		p.push_unack(packet.No)
	}
}

func (p *StreamPipe) push_unack(no serial_number) {
	p.locker.Lock()
	defer p.locker.Unlock()

	p.unacks = append(p.unacks, newUnAck(no))
}

func (p *StreamPipe) pop_unack() (no serial_number, ok bool) {
	p.locker.Lock()
	defer p.locker.Unlock()

	if len(p.unacks) == 0 {
		return 0, false
	}

	unack := p.unacks[0]
	p.unacks = p.unacks[1:]
	return unack.no, true
}

func (p *StreamPipe) ack_check() error {
	p.locker.Lock()
	defer p.locker.Unlock()

	now := time.Now()

	for _, unack := range p.unacks {
		if now.Sub(unack.t) > defaultAckMaxDelay {
			return errors.New("ack timeout")
		}
	}
	return nil
}

func (p *StreamPipe) ack_check_loop() {
	defer p.waitGroup.Done()

	for _ := range p.ackChecker.C {
		if err := p.ack_check(); err != nil {
			p.setErr(err)
			return
		}
	}
}

func (p *StreamPipe) getErr() error {
	p.locker.Lock()
	defer p.locker.Unlock()

	return p.err
}

func (p*StreamPipe) setErr(err error) {
	p.locker.Lock()
	defer p.locker.Unlock()

	if p.err == nil {
		p.err = err
	}
}

func (p *StreamPipe) Read(buff []byte) (n int, err error) {
	p.rLocker.Lock()
	defer p.rLocker.Unlock()

	for {
		if err = p.getErr(); err != nil {
			return
		}

		if p.rBuffer.Len() == 0 {
			p.rWait.Wait()
			continue
		}
	}

	return p.rBuffer.Read(buff)
}

func (p *StreamPipe) Write(buff []byte) (n int, err error) {
	p.wLocker.Lock()
	defer p.wLocker.Unlock()

	err = p.getErr()
	if err != nil {
		return 0, nil
	}

	_, err = p.wBuffer.Write(buff)
	if err != nil {
		return 0, err
	}

	p.wWait.Signal()

	return
}

func (p *StreamPipe) CloseWithError(e error) (err error) {
	p.locker.Lock()
	defer p.locker.Unlock()

	if e == nil {
		e = io.EOF
	}
	p.setErr(e)

	p.rWait.Signal()
	p.wWait.Signal()

	if s, ok := p.raw.(grpc.ClientStream); ok {
		err = s.CloseSend()
	} else {
		//wait peer close
		for {
			_, err = p.raw.Recv()
			if err != nil {
				break
			}
		}
	}
	if err != nil {
		return err
	}

	p.waitGroup.Wait()
	return nil
}

func (p *StreamPipe) Close() (err error) {
	return p.CloseWithError(nil)
}