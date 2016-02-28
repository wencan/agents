package codec

import (
	"google.golang.org/grpc"
	"github.com/golang/protobuf/proto"
	"errors"
	"fmt"
)

type Codec interface {
	Encode(dst, src []byte) ([]byte, error)
	Decode(dst, src []byte) ([]byte, error)
}

type Factory func (...interface{}) (Codec, error)

var codecs map[string]Factory = make(map[string]Factory)

func register(name string, f Factory) error {
	codecs[name] = f
	return nil
}

func New(name string, args ...interface{}) (Codec, error) {
	f, ok := codecs[name]
	if !ok {
		return nil, errors.New(fmt.Sprintf("codec \"%s\" not found", name))
	}
	return f(args)
}

type Chan struct {
	Codecs []Codec
}

func (c *Chan) Encode(dst, src []byte) ([]byte, error) {
	data := src
	var err error
	for _, codec := range c.Codecs {
		data, err = codec.Encode(nil, data)
		if err != nil {
			return data, err
		}
	}
	return data, nil
}

func (c *Chan) Decode(dst, src []byte) ([]byte, error) {
	data := src
	var err error
	for i := len(c.Codecs) - 1; i >= 0; i-- {
		codec := c.Codecs[i]
		data, err = codec.Decode(nil, data)
		if err != nil {
			return data, err
		}
	}
	return data, nil
}

func NewChan(names []string) (*Chan, error) {
	c := &Chan{}
	for _, name := range names {
		codec, err := New(name)
		if err != nil {
			return nil, err
		}
		c.Codecs = append(c.Codecs, codec)
	}
	return c, nil
}

type withProto struct {
	c Codec
}

func (c *withProto) Marshal(v interface{}) ([]byte, error) {
	data, err := proto.Marshal(v.(proto.Message))
	if err != nil {
		return nil, err
	}

	return c.c.Encode(nil, data)
}

func (c *withProto) Unmarshal(data []byte, v interface{}) error {
	data, err := c.c.Decode(nil, data)
	if err != nil {
		return err
	}

	return proto.Unmarshal(data, v.(proto.Message))
}

func (c *withProto) String() string {
	return "withProto"
}

func WithProto(c Codec) (grpc.Codec, error) {
	return &withProto{c}, nil
}