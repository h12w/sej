package shard

import (
	"fmt"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// Shard contains the shard info for opening
type Shard struct {
	Prefix string // root directory
	Bit    uint8  // number of bits that the shard index contains
	Index  int    // shard index
}

func (s Shard) Dir(rootDir string) string {
	prefix := s.Prefix
	if prefix != "" && s.Bit != 0 {
		prefix += "."
	}
	if s.Bit == 0 {
		return path.Join(rootDir, prefix)
	}
	return path.Join(rootDir, prefix+fmt.Sprintf("%x.%03x", s.Bit, s.Index))
}

func parseShardDir(rootDir, shardDir string) (Shard, error) {
	dir := strings.TrimPrefix(shardDir, rootDir)
	dir = strings.Trim(dir, string(filepath.Separator))
	parts := strings.Split(dir, ".")
	prefix := ""
	bitStr, indexStr := "0", "0"
	switch len(parts) {
	case 1:
		prefix = parts[0]
	case 2:
		bitStr, indexStr = parts[0], parts[1]
	case 3:
		prefix = parts[0]
		bitStr, indexStr = parts[1], parts[2]
	default:
		return Shard{}, errors.New("fail to parse shard dir " + dir)
	}

	shardBit, err := strconv.ParseUint(bitStr, 16, 8)
	if err != nil {
		return Shard{}, errors.Wrap(err, "fail to parse shard bit")
	}
	shardIndex, err := strconv.ParseUint(indexStr, 16, 16)
	if err != nil {
		return Shard{}, errors.Wrap(err, "fail to parse shard index")
	}
	return Shard{
		Prefix: prefix,
		Bit:    uint8(shardBit),
		Index:  int(shardIndex),
	}, nil

}
