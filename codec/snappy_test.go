package codec

import (
	"testing"
	"crypto/rand"
	"io"
	"bytes"
)

func TestSnappy(t *testing.T) {
	src := make([]byte, 512)

	_, err := io.ReadFull(rand.Reader, src)
	if err != nil {
		t.Error(err)
		return
	}

	c, _ := _new_snappy()

	var dst []byte
	dst, err = c.Encode(nil, src)
	if err != nil {
		t.Error(err)
		return
	}

	dst, err = c.Decode(nil, dst)
	if err != nil {
		t.Error(err)
		return
	}

	if bytes.Compare(src, dst) != 0 {
		t.Fail()
	}
}
