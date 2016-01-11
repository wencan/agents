package agents

import (
	"testing"
	"sync/atomic"
	"log"
	"math/rand"
	"time"
)

var poolObjNumber int32 = 0

type poolObj struct {
	number int32
}

func newPoolObj() *poolObj {
	n := atomic.AddInt32(&poolObjNumber, 1)

	log.Println("New poolObj:", n)

	return &poolObj{
		number: n,
	}
}

func (o *poolObj) Close() {
	log.Println("Delete poolObj:", o.number)
}

func _new() interface{} {
	return newPoolObj()
}

func _delete(x interface{}) {
	o := x.(*poolObj)
	o.Close()
}

func TestPool(t *testing.T) {
	p := NewPool(_new, _delete, 5)
	defer p.Destory()

	objs := []*poolObj{}

	rand.Seed(int64(time.Now().Nanosecond()))
	for {
		n := rand.Intn(100)
		if n == 99 {
			for _, obj := range objs {
				p.Put(obj)
			}
			objs = []*poolObj{}

			break
		}

		if n % 2 == 0 {
			x := p.Get()
			obj := x.(*poolObj)

			objs = append(objs, obj)
		} else if len(objs) > 0 {
			obj := objs[0]
			objs = objs[1:]

			p.Put(obj)
		}
	}
}