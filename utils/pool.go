package utils

import (
	"sync"
	"errors"
)

func defaultDelete (x interface{}) {}

type Pool struct {
	new         func() interface{}
	delete      func(interface{})

	gets        chan interface{}
	puts        chan interface{}
	done        chan struct{}

	storage     []interface{}
	storageSize int

	waitGroup   sync.WaitGroup
}

func NewPool(new func() interface{}, delete func(interface{}), size int) (p *Pool) {
	if size <= 0 {
		panic(errors.New("pool size error"))
	}

	if delete == nil {
		delete = defaultDelete
	}

	p = &Pool{
		new: new,
		delete: delete,
		gets: make(chan interface{}),
		puts: make(chan interface{}),
		done: make(chan struct{}),
		storageSize: size - 1,
	}

	p.waitGroup.Add(1)
	go p.loop()

	return p
}

func (p *Pool) get() (x interface{}) {
	if len(p.storage) > 0 {
		x = p.storage[0]
		p.storage = p.storage[1:]

		return x
	}

	return p.new()
}

func (p *Pool) put(x interface{}) {
	p.storage = append(p.storage, x)

	for len(p.storage) > p.storageSize {
		x = p.storage[0]
		p.storage = p.storage[1:]

		p.delete(x)
	}
}

func (p *Pool) loop() {
	defer p.waitGroup.Done()

	x := p.get()
	defer func() {
		p.put(x)
	}()

	FIRST:
	for {
		select {
		case p.gets <- x:
			 x = p.get()
		case old := <- p.puts:
			p.put(old)
		case <- p.done:
			break FIRST
		}
	}
}

func (p *Pool) Get() (x interface{}) {
	return <- p.gets
}

func (p *Pool) Put(x interface{}) {
	p.puts <- x
}

func (p *Pool) Destory() {
	close(p.done)
	p.waitGroup.Wait()

	close(p.gets)
	close(p.puts)

	for _, x := range p.storage {
		p.delete(x)
	}
}