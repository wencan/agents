package internal

import "time"

const (
	versionMajor uint32 = 1;
	versionMinor uint32 = 0;

	defaultContextTimeout time.Duration = 60 * time.Second

	defaultPingDelay time.Duration = 60 * time.Second
	defaultPingMaxDelay time.Duration = 120 * time.Second
	defaultPingCheckDelay time.Duration = time.Minute
	defaultPacketMaxBytes int = 1024 * 512
	defaultAckMaxDelay time.Duration = 10 * time.Second
	defaultAckCheckDelay time.Duration = 10 * time.Second
)