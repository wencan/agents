package utils

import (
	"errors"
	"sync"
	"time"
)

func defaultDelete (x interface{}) {}

const poolCleanupDelay time.Duration = time.Second * 10

var (
	allPoolsMutex sync.Mutex
	allPools []*Pool
)

func cleanupLoop() {
	for {
		<-time.After(poolCleanupDelay)

		allPoolsMutex.Lock()
		pools := make([]*Pool, len(allPools))
		copy(pools, allPools)
		allPoolsMutex.Unlock()

		for _, pool := range pools {
			pool.Cleanup()
		}
	}
}

func init() {
	go cleanupLoop()
}

type Pool struct {
	new         func() interface{}
	delete      func(interface{})

	storage     chan interface{}
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
		storage: make(chan interface{}, size),
	}

	allPoolsMutex.Lock()
	allPools = append(allPools, p)
	allPoolsMutex.Unlock()

	return p
}

func (p *Pool) Get() (x interface{}) {
	select {
	case x = <- p.storage:
		return x
	default:
	}

	return p.new()
}

func (p *Pool) Put(x interface{}) {
	select {
	case p.storage <- x:
	default:
		p.delete(x)
	}
}

func (p *Pool) Cleanup() {
	for x := range p.storage {
		p.delete(x)
	}
}