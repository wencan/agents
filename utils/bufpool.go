package utils

import "sync"

var MegaBufPool *BufPool

func init() {
	MegaBufPool = NewBufPool(1024 * 1024)
}

type BufPool struct {
	Uplimit	int

	mutex sync.Mutex

	chunk map[int]*sync.Pool
	mutexs map[int]sync.Mutex
}

func NewBufPool(uplimit int) *BufPool {
	return &BufPool{
		Uplimit: uplimit,
		chunk: make(map[int]*sync.Pool),
		mutexs: make(map[int]sync.Mutex),
	}
}

func (p *BufPool) index(length int) int {
	length = length -1
	n := 1

	for length > 0 {
		length >>= 1
		n <<= 1
	}
	return n
}

func (p *BufPool) allocator(bucket int) func() interface{} {
	return func()interface{}{return make([]byte, bucket)}
}

func (p *BufPool) Get(length int) []byte {
	if length > p.Uplimit {
		return make([]byte, length)
	}

	idx := p.index(length)

	mutex := p.mutexs[idx]
	mutex.Lock()

	c, ok := p.chunk[idx]
	if !ok {
		c = &sync.Pool{New: p.allocator(idx)}
		p.chunk[idx] = c
	}

	mutex.Unlock()

	x := c.Get()
	buf := x.([]byte)
	return buf[:length]		//slice
}

func (p *BufPool) Put(buf []byte) {
	length := len(buf)
	if length > p.Uplimit {
		//free
		return
	}

	idx := p.index(length)

	mutex := p.mutexs[idx]
	mutex.Lock()

	c, ok := p.chunk[idx]
	if !ok {
		c = &sync.Pool{New: p.allocator(idx)}
		p.chunk[idx] = c
	}

	mutex.Unlock()

	buf = buf[:cap(buf)]	//reset
	c.Put(buf)
}