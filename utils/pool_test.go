package utils

import (
	"testing"
	"sync/atomic"
	"math/rand"
	"time"
)

var poolObjNumber int32 = 0

type poolObj struct {
	number int32
}

func newPoolObj() *poolObj {
	n := atomic.AddInt32(&poolObjNumber, 1)

	return &poolObj{
		number: n,
	}
}

func (o *poolObj) Close() {
}

func _new() interface{} {
	return newPoolObj()
}

func _delete(x interface{}) {
	o := x.(*poolObj)
	o.Close()
}

func TestPool(t *testing.T) {
	p := NewPool(_new, _delete, 100)

	objs := []*poolObj{}
	total := 0
	after := time.After(time.Minute)

	rand.Seed(int64(time.Now().Nanosecond()))
	for {
		n := rand.Intn(2)

		if n % 2 == 0 || len(objs) == 0 {
			x := p.Get()
			obj := x.(*poolObj)

			objs = append(objs, obj)

			total++
		} else {
			obj := objs[0]
			objs = objs[1:]

			p.Put(obj)
		}

		select {
		case <- after:
			t.Log("total:", total)
			t.Log("objects:", poolObjNumber)
			return
		default:
		}
	}
}