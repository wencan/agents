package codec

import "github.com/golang/snappy"

type Snappy struct {}

func (Snappy) Encode(dst, src []byte) ([]byte, error) {
	return snappy.Encode(dst, src), nil
}

func (Snappy) Decode(dst, src []byte) ([]byte, error) {
	return snappy.Decode(dst, src)
}

func init() {
	register("snappy", &Snappy{})
}