package shard

import (
	"fmt"
	"path"
)
 
	// Shard contains the shard info for opening
type Shard struct {
		Prefix string // root directory
		Bit     uint8  // number of bits that the shard index contains
		Index   int    // shard index
}

func (s Shard) Dir(rootDir string) string {
	if s.Bit == 0 {
		return path.Join(rootDir, s.Prefix)
	}
	return path.Join(rootDir, s.Prefix+fmt.Sprintf(".%x.%03x", s.Bit, s.Index))
} 