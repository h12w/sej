package wire

import "h12.me/sej/shard"

type (
	Request struct {
		ID       int
		Type     RequestType
		ClientID string
		Shard    shard.Shard
	}
	RequestType uint8
	Response    struct {
		ID  int
		Err string
	}
)
