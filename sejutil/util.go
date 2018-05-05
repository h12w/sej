package sejutil

import (
	"io"
	"os"
	"time"

	"h12.io/sej"
)

func NewScannerFrom(journalDir string, from time.Time) (*sej.Scanner, error) {
	dir, err := sej.OpenJournalDir(sej.JournalDirPath(journalDir))
	if err != nil {
		return nil, err
	}
	startOffset := dir.First().FirstOffset
	for _, file := range dir.Files {
		f, err := os.Open(file.FileName)
		if err != nil {
			return nil, err
		}
		var msg sej.Message
		if _, err := msg.ReadFrom(f); err != nil && err != io.EOF {
			f.Close()
			return nil, err
		}
		f.Close()
		if msg.Timestamp.After(from) {
			break
		}
		startOffset = file.FirstOffset
	}
	if int(startOffset)-5000 > 0 {
		startOffset -= 5000
	}
	s, err := sej.NewScanner(journalDir, startOffset)
	if err != nil {
		return nil, err
	}
	s.Timeout = time.Second
	for s.Scan() {
		msg := s.Message()
		if !msg.Timestamp.Before(from) {
			startOffset = msg.Offset
			break
		}
	}
	return sej.NewScanner(journalDir, startOffset)
}
