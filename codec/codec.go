package codec

import (
	"google.golang.org/grpc"
	"github.com/golang/protobuf/proto"
	"errors"
	"fmt"
)

type Codec interface {
	Encode(dst,src []byte) ([]byte, error)
	Decode(dst,src []byte) ([]byte, error)
}

var codecs map[string]Codec = make(map[string]Codec)

func register(name string, c Codec) error {
	codecs[name] = c
	return nil
}

func New(name string) (Codec, error) {
	c, ok := codecs[name]
	if !ok {
		return nil, errors.New(fmt.Sprintf("\"%s\" not found", name))
	}
	return c, nil
}

type CodecChan struct {
	codecs []Codec
}

func (c *CodecChan) Encode(dst,src []byte) ([]byte, error) {
	data := src
	var err error
	for _, codec := range c.codecs {
		data, err = codec.Encode(nil, data)
		if err != nil {
			return data, err
		}
	}
	return data, nil
}

func (c *CodecChan) Decode(dst,src []byte) ([]byte, error) {
	data := src
	var err error
	for i := len(c.codecs)-1; i>=0; i-- {
		codec := c.codecs[i]
		data, err = codec.Decode(nil, data)
		if err != nil {
			return data, err
		}
	}
	return data, nil
}

func NewCodecChan(names []string) (*CodecChan, error) {
	codecChan := &CodecChan{}
	for _, name := range names {
		codec , err := New(name)
		if err != nil {
			return nil, err
		}
		codecChan.codecs = append(codecChan.codecs, codec)
	}
	return codecChan, nil
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