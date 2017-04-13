package main

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
	"h12.me/errors"
	"h12.me/sej"
	"h12.me/uuid/hexid"
)

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

type CountCommand struct {
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
}

func (c *CountCommand) Execute(args []string) error {
	//fmt.Println("couting from", c.Start, c.End, "for type", c.Type)
	dir, err := sej.OpenJournalDir(sej.JournalDirPath(c.Dir))
	if err != nil {
		return err
	}
	startOffset := dir.First().FirstOffset
	for _, file := range dir.Files {
		f, err := os.Open(file.FileName)
		if err != nil {
			return err
		}
		var msg sej.Message
		if _, err := msg.ReadFrom(f); err != nil && err != io.EOF {
			f.Close()
			return err
		}
		f.Close()
		if msg.Timestamp.After(c.Start.Time) {
			break
		}
		startOffset = file.FirstOffset
	}
	if int(startOffset)-5000 > 0 {
		startOffset -= 5000
	}
	s, err := sej.NewScanner(c.Dir, startOffset)
	if err != nil {
		return err
	}
	s.Timeout = time.Second
	cnt := 0
	overCount := 0
	for s.Scan() {
		msg := s.Message()
		if !msg.Timestamp.Before(c.Start.Time) {
			if msg.Timestamp.Before(c.End.Time) {
				if msg.Type == c.Type {
					cnt++
				}
			} else {
				overCount++
			}
		}
		if overCount > 5000 {
			break
		}
	}
	fmt.Println(cnt)
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

type LastOffsetCommand struct {
	JournalDirConfig `positional-args:"yes"  required:"yes"`
	Offset           string `
		long:"offset"
		description:"the offset"`
}

func (c *LastOffsetCommand) Execute(args []string) error {
	dir, err := sej.OpenJournalDir(sej.JournalDirPath(c.Dir))
	if err != nil {
		return err
	}
	offset, err := dir.Last().LastReadableOffset()
	if err != nil {
		return err
	}
	fmt.Println(offset)
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
		default:"msgpack"
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
	cnt := 0
	for scanner.Scan() {
		switch c.Format {
		case "json", "msgpack", "bson":
			line, _ := Format(c.Format).Sprint(scanner.Message())
			fmt.Println(line)
		}
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

type CleanCommand struct {
	Max int `
		long:"max"
		default:"2"
		description:"max number of journal files kept after cleanning"`
	JournalDirConfig `positional-args:"yes"  required:"yes"`
}

func (c *CleanCommand) Execute(args []string) error {
	if c.Max < 1 {
		return errors.New("max must be at least 1")
	}
	dir, err := sej.OpenJournalDir(sej.JournalDirPath(c.Dir))
	if err != nil {
		return errors.Wrap(err)
	}
	// make sure len(dir.Files) > c.Max
	if len(dir.Files) <= c.Max {
		return nil
	}

	latest, err := dir.Last().LastOffset()
	if err != nil {
		return errors.Wrap(err)
	}
	slowestReader := ""
	slowestOffset := latest
	ofsFiles, err := filepath.Glob(path.Join(sej.OffsetDirPath(c.Dir), "*.ofs"))
	if err != nil {
		return errors.Wrap(err)
	}
	for _, ofsFile := range ofsFiles {
		f, err := os.Open(ofsFile)
		if err != nil {
			return errors.Wrap(err)
		}
		offset, err := sej.ReadOffset(f)
		if err != nil {
			f.Close()
			return errors.Wrap(err)
		}
		f.Close()
		if offset < slowestOffset {
			slowestReader = ofsFile
			slowestOffset = offset
		}
	}

	for _, journalFile := range dir.Files[:len(dir.Files)-c.Max] {
		lastOffset, err := journalFile.LastOffset()
		if err != nil {
			return errors.Wrap(err)
		}
		if slowestOffset <= lastOffset {
			log.Printf("stop cleaning %s (%d-%d) because of slow reader %s\n", journalFile.FileName, journalFile.FirstOffset, lastOffset, slowestReader)
			break
		}
		if err := os.Remove(journalFile.FileName); err != nil {
			return errors.Wrap(err)
		}
		log.Printf("%s removed", journalFile.FileName)
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
