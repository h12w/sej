package shard

import (
	"fmt"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// shard contains the shard info for opening
type (
	Path struct {
		Root     string
		Prefix   string
		ShardBit uint8
	}
	shard struct {
		Path
		Index int
	}
)

var rxPrefix = regexp.MustCompile(`[a-zA-Z0-9_\-]*`)

func (p *Path) check() error {
	if !rxPrefix.MatchString(p.Prefix) {
		return errors.New("invalid prefix " + p.Prefix)
	}
	if p.ShardBit > 10 {
		return errors.New("shardBit should be no more than 10")
	}
	return nil
}

func (p *Path) shardCount() int {
	return 1 << p.ShardBit
}

func (p *Path) shardMask() uint16 {
	return 1<<p.ShardBit - 1
}

func (p *Path) dir(shardIndex int) string {
	return shard{Path: *p, Index: shardIndex}.Dir()
}

func (s shard) Dir() string {
	if s.ShardBit == 0 {
		return path.Join(s.Root, s.Prefix)
	}
	prefix := s.Prefix
	if s.Prefix != "" {
		prefix += "."
	}
	return path.Join(s.Root, prefix+fmt.Sprintf("%x.%03x", s.ShardBit, s.Index))
}

func parseShardDir(rootDir, dir string) (shard, error) {
	dir = strings.TrimPrefix(dir, rootDir)
	dir = strings.TrimPrefix(dir, string(filepath.Separator))
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
		return shard{}, errors.New("fail to parse shard dir " + dir)
	}

	shardBit, err := strconv.ParseUint(bitStr, 16, 8)
	if err != nil {
		return shard{}, errors.Wrap(err, "fail to parse shard bit")
	}
	shardIndex, err := strconv.ParseUint(indexStr, 16, 16)
	if err != nil {
		return shard{}, errors.Wrap(err, "fail to parse shard index")
	}
	s := shard{
		Path: Path{
			Root:     rootDir,
			Prefix:   prefix,
			ShardBit: uint8(shardBit),
		},
		Index: int(shardIndex),
	}
	return s, s.check()
}
