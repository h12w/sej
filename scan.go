package sej

import (
	"io"
	"time"
)

var (
	// NotifyTimeout is the timeout value in rare cases that the OS notification fails
	// to capture the file/directory change events
	NotifyTimeout = time.Hour
)

// Scanner implements reading of messages from segmented journal files
type Scanner struct {
	offset      uint64
	journalDir  *watchedJournalDir
	journalFile *JournalFile
	file        watchedReadSeekCloser
	message     Message
	err         error

	Timeout time.Duration // read timeout when no data arrived, default 0
}
type watchedReadSeekCloser interface {
	readSeekCloser
	Watch() chan bool
	Name() string
}

// NewScanner creates a scanner for reading dir/jnl starting from offset
// Default Timeout is 1 second
func NewScanner(dir string, offset uint64) (*Scanner, error) {
	dir = JournalDirPath(dir)
	r := Scanner{Timeout: time.Second}
	journalDir, err := openWatchedJournalDir(dir)
	if err != nil {
		return nil, err
	}
	journalFile, err := journalDir.Find(offset)
	if err != nil {
		return nil, err
	}
	if journalDir.IsLast(journalFile) {
		r.file, err = openWatchedFile(journalFile.FileName)
	} else {
		r.file, err = openDummyWatchedFile(journalFile.FileName)
	}
	if err != nil {
		return nil, err
	}
	r.offset = journalFile.FirstOffset
	r.journalFile = journalFile
	r.journalDir = journalDir
	for r.offset < offset && r.Scan() {
	}
	// ignore r.Err(), which will be detected by the caller later anyway
	// ignore the difference between r.offset and offset, in case the journal has been truncated
	// TODO: add a testcase
	return &r, nil
}

// Scan scans the next message and increment the offset
func (r *Scanner) Scan() bool {
	if r.err != nil && r.err != ErrTimeout {
		return false
	}
	for {
		fileChanged, dirChanged := r.file.Watch(), r.journalDir.Watch()
		var n int64
		n, r.err = r.message.ReadFrom(r.file)
		if r.err != nil {
			// rollback the reader
			if _, seekErr := r.file.Seek(-n, io.SeekCurrent); seekErr != nil {
				return false
			}

			// unexpected io error
			switch r.err {
			case io.EOF, io.ErrUnexpectedEOF:
			default:
				return false
			}

			// not the last file, open the next journal file
			if !r.journalDir.IsLast(r.journalFile) {
				if r.err = r.reopenFile(); r.err != nil {
					return false
				}
				continue
			}

			// the last file, wait for any changes
			var timeoutChan <-chan time.Time
			if r.Timeout != 0 {
				timeoutChan = time.After(r.Timeout)
			}
			select {
			case <-dirChanged:
				if r.err = r.reopenFile(); r.err != nil {
					return false
				}
			case <-fileChanged:
			case <-timeoutChan:
				r.err = ErrTimeout
				return false
			case <-time.After(NotifyTimeout):
			}
			continue
		}
		break
	}

	// check offset
	if r.message.Offset != r.offset {
		r.err = &ScanOffsetError{
			File:           r.file.Name(),
			Offset:         r.message.Offset,
			Timestamp:      r.message.Timestamp,
			ExpectedOffset: r.offset,
		}
	}
	if r.err != nil {
		return false
	}

	r.offset = r.message.Offset + 1
	return true
}

func (r *Scanner) Message() *Message {
	return &r.message
}

func (r *Scanner) Err() error {
	return r.err
}

func (r *Scanner) reopenFile() error {
	journalFile, err := r.journalDir.Find(r.offset)
	if err != nil {
		return err
	}
	var newFile watchedReadSeekCloser
	if r.journalDir.IsLast(journalFile) {
		newFile, err = openWatchedFile(journalFile.FileName)
	} else {
		newFile, err = openDummyWatchedFile(journalFile.FileName)
	}
	if err != nil {
		return err
	}
	if err := r.file.Close(); err != nil {
		return err
	}
	r.file = newFile
	r.journalFile = journalFile
	return nil
}

// Offset returns the current offset of the reader, i.e. last_message.offset + 1
func (r *Scanner) Offset() uint64 {
	return r.offset
}

// Close closes the reader
func (r *Scanner) Close() error {
	err1 := r.journalDir.Close()
	err2 := r.file.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

type dummyWatchedFile struct {
	*fileReader
}

func openDummyWatchedFile(file string) (*dummyWatchedFile, error) {
	f, err := openFileReader(file)
	if err != nil {
		return nil, err
	}
	return &dummyWatchedFile{fileReader: f}, nil
}

func (f *dummyWatchedFile) Watch() chan bool { return nil }
