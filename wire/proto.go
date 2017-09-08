//go:generate colf -b .. go proto.colf
//go:generate mv Colfer.go proto_auto.go
package wire

type (
	// Request struct {
	// 	ID       int
	// 	Type     RequestType
	// 	ClientID string
	// 	Shard    shard.Shard
	// 	Offset   uint64
	// }
	RequestType uint8
	// Response    struct {
	// 	ID  int
	// 	Err string
	// }
)

const (
	GET RequestType = iota + 1
	PUT
)
