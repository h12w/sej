package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path"

	"path/filepath"

	"gopkg.in/vmihailenco/msgpack.v2"
	"h12.me/errors"
	"h12.me/sej"
	"h12.me/uuid/hexid"
)

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
	JournalFileConfig `positional-args:"yes"  required:"yes"`
}

func (c *LastOffsetCommand) Execute(args []string) error {
	jf, err := sej.ParseJournalFileName(".", os.Args[2])
	if err != nil {
		return errors.Wrap(err)
	}
	fmt.Println(jf.LastOffset())
	return nil
}

type JournalDirConfig struct {
	Dir string
}

type TailCommand struct {
	Dir string `
		long:"dir"
		description:"directory of the segemented journal"`
	Count uint64 `
		long:"count"
		description:"the number of tailing messages to print"
		default:"10"`
	Format string `
		long:"format"
		default:"msgpack"
		description:"encoding format of the message"`
}

func (c *TailCommand) Execute(args []string) error {
	dir, err := sej.OpenJournalDir(c.Dir)
	if err != nil {
		return err
	}
	earlist := dir.First().FirstOffset
	latest, err := dir.Last().LastOffset()
	if err != nil {
		return err
	}
	offset := latest - c.Count
	if offset < earlist {
		offset = earlist
	}
	reader, err := sej.NewReader(c.Dir, offset)
	if err != nil {
		return err
	}
	for i := 0; i < int(c.Count); i++ {
		msg, err := reader.Read()
		if err != nil {
			return err
		}
		switch c.Format {
		case "json", "msgpack":
			line, _ := Format(c.Format).Sprint(msg.Value)
			fmt.Println(line)
		}
	}
	return nil
}

type Format string

func (format Format) Sprint(value []byte) (string, error) {
	switch format {
	case "msgpack":
		m := make(map[string]interface{})
		if err := msgpack.Unmarshal(value, &m); err != nil {
			break
		}
		buf, err := json.Marshal(hexid.Restore(m))
		if err != nil {
			return "", fmt.Errorf("fail to marshal %#v: %s", m, err.Error())
		}
		return string(buf), nil
	}
	return string(value), nil
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
	dir, err := sej.OpenJournalDir(c.Dir)
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
