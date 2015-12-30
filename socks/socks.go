package socks

type Error struct {
	x     byte
	error string
}

func (self Error) Byte() byte {
	return self.x
}

func (self Error) Error() string {
	return self.error
}