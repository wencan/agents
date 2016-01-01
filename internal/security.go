package internal

import (
"../agent"
)

//for server
type Guard interface {
	Type() agent.AuthMethod
	AuthFromProto(*agent.AuthRequest) (bool, error)
}

//for client
type Passport interface {
	Type() agent.AuthMethod
	ToProto() (*agent.AuthRequest, error)
}