package codec

import (
	"github.com/wencan/agents/agent"
	"testing"
	"github.com/pborman/uuid"
)

func TestProto(t *testing.T) {
	uid := uuid.New()
	t.Log("uuid:", uid)

	ping := &agent.Ping{AppData: uid}

	c, _ := New("snappy")
	codec, _ := WithProto(c)

	data, err := codec.Marshal(ping)
	if err != nil {
		t.Error(err)
		return
	}

	ping = &agent.Ping{}
	err = codec.Unmarshal(data, ping)
	if err != nil {
		t.Error(err)
		return
	}

	t.Log("new uuid:", ping.AppData)
	if ping.AppData != uid {
		t.Fail()
	}
}
