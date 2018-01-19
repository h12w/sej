package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"time"

	"gopkg.in/mgo.v2/bson"
	"gopkg.in/vmihailenco/msgpack.v2"
	"gopkg.in/yaml.v2"
	"h12.me/errors"
	"h12.me/sej"
	"h12.me/sej/sejutil"
	"h12.me/uuid/hexid"
)

type FileCommand struct {
	JournalFileConfig `positional-args:"yes"  required:"yes"`
}

func (c *FileCommand) Execute(args []string) error {
	jf, err := sej.ParseJournalFileName(path.Split(c.JournalFile))
	if err != nil {
		return err
	}
	firstMsg, err := jf.FirstMessage()
	if err != nil {
		return err
	}
	fmt.Println("first:")
	fmt.Println("    offset:", firstMsg.Offset)
	fmt.Println("    timestamp:", firstMsg.Timestamp)
	lastMsg, err := jf.LastMessage()
	if err != nil {
		return err
	}
	fmt.Println("last:")
	fmt.Println("    offset:", lastMsg.Offset)
	fmt.Println("    timestamp:", lastMsg.Timestamp)
	return nil
}

type TimestampCommand struct {
	JournalDirConfig `positional-args:"yes"  required:"yes"`
	Offset           string `
		long:"offset"
		description:"the offset"`
}

func (d *TimestampCommand) Execute(args []string) error {
	ofsFilename := path.Join(sej.OffsetDirPath(d.Dir), d.Offset) + ".ofs"
	ofsFile, err := os.Open(ofsFilename)
	if err != nil {
		return err
	}
	defer ofsFile.Close()
	offset, err := sej.ReadOffset(ofsFile)
	if err != nil {
		return err
	}

	s, err := sej.NewScanner(d.Dir, offset)
	if err != nil {
		return err
	}
	if s.Offset() != offset {
		return fmt.Errorf("fail to scan to offset %d in %s", offset, d.Dir)
	}
	fmt.Println("offset:", s.Offset())
	fmt.Println("timestamp:", s.Message().Timestamp)
	return nil
}

type ScanCommand struct {
	JournalDirConfig `positional-args:"yes"  required:"yes"`
	Start            Timestamp `
	long:"start"
	description:"start time"`
	End Timestamp `
	long:"end"
	description:"start time"`
	Type byte `
	long:"type"
	description:"message type"`
	Format string `
		long:"format"
		default:"bson"
		description:"encoding format of the message"`
	Count bool `
		long:"count"
		description:"count or print"
	`
}

type CountCommand ScanCommand

func (c *CountCommand) Execute(args []string) error {
	scanCmd := ScanCommand(*c)
	scanCmd.Count = true
	return scanCmd.Execute(args)
}

func (c *ScanCommand) Execute(args []string) error {
	s, err := sejutil.NewScannerFrom(c.Dir, c.Start.Time)
	if err != nil {
		return err
	}
	defer s.Close()
	s.Timeout = time.Second
	cnt := 0
	overCount := 0
scan:
	for s.Scan() {
		msg := s.Message()
		if msg.Timestamp.Before(c.End.Time) {
			if c.Type == 0 || msg.Type == c.Type {
				if !c.Count {
					line, err := DefaultFormatter.Sprint(msg)
					if err != nil {
						fmt.Println(err)
						break scan
					}
					fmt.Println(line)
				}
				cnt++
			}
		} else {
			overCount++
		}
		if overCount > 5000 {
			break
		}
	}
	if c.Count {
		fmt.Println(cnt)
	}
	return nil
}

type ResetCommand struct {
	JournalDirConfig `positional-args:"yes"  required:"yes"`
	Start            Timestamp `
	long:"start"
	description:"start time"`
	Offset string `
		long:"offset"
		description:"the offset"`
	Reset bool `
		long:"reset"
		description:"reset or just print"
	`
}

func (c *ResetCommand) Execute(args []string) error {
	if c.Offset == "" {
		return errors.New("empty offset")
	}
	offset, err := sej.NewOffset(c.Dir, c.Offset, sej.FirstOffset)
	if err != nil {
		return err
	}
	defer offset.Close()
	s, err := sejutil.NewScannerFrom(c.Dir, c.Start.Time)
	if err != nil {
		return err
	}
	defer s.Close()
	msg := s.Message()
	fmt.Println("current-offset:", offset.Value())
	fmt.Println("reset-time:", msg.Timestamp)
	fmt.Println("reset-offset:", msg.Offset)
	fmt.Println("reset", c.Reset)

	if c.Reset {
		return offset.Commit(msg.Offset)
	}
	return nil
}

type DumpCommand struct {
	JournalFileConfig `positional-args:"yes"  required:"yes"`
}

type JournalFileConfig struct {
	JournalFile string
}

func (d *DumpCommand) Execute(args []string) error {
	var msg sej.Message
	for {
		file, err := os.Open(d.JournalFile)
		if err != nil {
			return errors.Wrap(err)
		}
		defer file.Close()
		if _, err := msg.ReadFrom(file); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		fmt.Println("offset:", msg.Offset)
		fmt.Printf("message: %x (%s)\n", msg.Value, string(msg.Value))
	}
	return nil
}

type OffsetCommand struct {
	JournalDirConfig `positional-args:"yes"  required:"yes"`
}

func (c *OffsetCommand) Execute(args []string) error {
	dir, err := sej.OpenJournalDir(sej.JournalDirPath(c.Dir))
	if err != nil {
		return err
	}
	firstOffset := dir.First().FirstOffset
	lastOffset, err := dir.Last().LastReadableOffset()
	if err != nil {
		return err
	}
	fmt.Println(path.Join(c.Dir, "first:"), firstOffset)
	fmt.Println(path.Join(c.Dir, "last:"), lastOffset)
	offsets, err := readOffsets(c.Dir)
	if err != nil {
		return err
	}
	buf, _ := yaml.Marshal(offsets)
	fmt.Println(string(buf))
	return nil
}

type JournalDirConfig struct {
	Dir string
}

type TailCommand struct {
	Count int `
		long:"count"
		description:"the number of tailing messages to print"
		default:"10"`
	Format string `
		long:"format"
		default:"bson"
		description:"encoding format of the message"`
	JournalDirConfig `positional-args:"yes"  required:"yes"`
}

func (c *TailCommand) Execute(args []string) error {
	dir, err := sej.OpenJournalDir(sej.JournalDirPath(c.Dir))
	if err != nil {
		return err
	}
	earlist := dir.First().FirstOffset
	latest, err := dir.Last().LastOffset()
	if err != nil {
		return err
	}
	offset := int(latest) - c.Count
	if offset < int(earlist) {
		offset = int(earlist)
	}
	scanner, err := sej.NewScanner(c.Dir, uint64(offset))
	if err != nil {
		return err
	}
	scanner.Timeout = time.Second
	if scanner.Err() != nil {
		return scanner.Err()
	}
	cnt := 0
scan:
	for scanner.Scan() {
		line, err := DefaultFormatter.Sprint(scanner.Message())
		if err != nil {
			fmt.Println(err)
			break scan
		}
		fmt.Println(line)
		cnt++
		if cnt >= int(c.Count) {
			break
		}
	}
	return nil
}

type Format string

func (format Format) Sprint(msg *sej.Message) (string, error) {
	value := msg.Value
	m := make(map[string]interface{})
	switch format {
	case "msgpack":
		if err := msgpack.Unmarshal(value, &m); err != nil {
			return "", err
		}
	case "bson":
		if err := bson.Unmarshal(value, &m); err != nil {
			return "", err
		}
	default:
		return string(value), nil
	}
	hexid.Restore(m)
	m = map[string]interface{}{
		"key":       string(msg.Key),
		"timestamp": msg.Timestamp,
		"type":      msg.Type,
		"value":     m,
	}
	buf, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("fail to marshal %#v: %s", m, err.Error())
	}
	return string(buf), nil
}

type OldCommand struct {
	Days int `
		long:"days"
		default:"7"
		description:"max number of days of journal files kept after cleanning"`
	JournalDirConfig `positional-args:"yes"  required:"yes"`
}

func (c *OldCommand) Execute(args []string) error {
	if c.Days < 1 {
		return errors.New("days must be at least 1")
	}
	dir, err := sej.OpenJournalDir(sej.JournalDirPath(c.Dir))
	if err != nil {
		return errors.Wrap(err)
	}

	lastOffset, err := dir.Last().LastReadableOffset()
	if err != nil {
		return errors.Wrap(err)
	}
	slowestReader := ""
	slowestOffset := lastOffset
	offsets, err := readOffsets(c.Dir)
	if err != nil {
		return errors.Wrap(err)
	}
	for ofsFile, offset := range offsets {
		if offset < slowestOffset {
			slowestReader = ofsFile
			slowestOffset = offset
		}
	}

	daysAgo := time.Now().Add(-time.Duration(c.Days) * time.Hour * 24)
	for _, journalFile := range dir.Files[:len(dir.Files)-1] {
		lastMessage, err := journalFile.LastMessage()
		if err != nil {
			return errors.Wrap(err)
		}
		if slowestOffset <= lastMessage.Offset {
			log.Printf("cannot clean %s (%d-%d) because of slow reader %s\n", journalFile.FileName, journalFile.FirstOffset, lastMessage.Offset, slowestReader)
			break
		}
		if !lastMessage.Timestamp.Before(daysAgo) {
			break
		}
		fmt.Println(journalFile.FileName)
	}
	return nil
}

type Timestamp struct {
	time.Time
}

const timeFormat = "2006-01-02T15:04:05"

func (t *Timestamp) UnmarshalFlag(value string) error {
	tm, err := time.Parse(timeFormat, value)
	if err != nil {
		return fmt.Errorf("error parsing %s: %s", value, err.Error())
	}
	t.Time = tm
	return nil
}

func readOffsets(dir string) (map[string]uint64, error) {
	offsets := make(map[string]uint64)
	ofsFiles, err := filepath.Glob(path.Join(sej.OffsetDirPath(dir), "*.ofs"))
	if err != nil {
		return nil, errors.Wrap(err)
	}
	for _, ofsFile := range ofsFiles {
		f, err := os.Open(ofsFile)
		if err != nil {
			return nil, errors.Wrap(err)
		}
		offset, err := sej.ReadOffset(f)
		if err != nil && err != io.EOF {
			f.Close()
			return nil, errors.Wrap(err)
		}
		f.Close()
		offsets[ofsFile] = offset
	}
	return offsets, nil
}
