package sej

import (
	"encoding/base64"
	"errors"
	"fmt"
	"time"
)

var (
	// ErrCRC is returned when the CRC of an message value does not match the stored CRC
	ErrCRC = errors.New("CRC mismatch")
	// ErrTimeout is returned when no message can be obtained within Reader.Timeout
	ErrTimeout = errors.New("read timeout")
)

// internal errors
var (
	// errOffsetTooSmall is returned when the journal file containing the offset has been cleaned up
	errOffsetTooSmall = errors.New("offset is too small")
)

// CorruptionError is returned when the last message of a segmented journal file is corrupted
type CorruptionError struct {
	File      string
	Offset    uint64
	Timestamp time.Time
	Message   []byte
	Err       error
	FixErr    error
}

func (e *CorruptionError) Error() string {
	if e.FixErr != nil {
		return fmt.Sprintf("file %s is corrupted at (%d, %v) but failed to fix it: %s", e.File, e.Offset, e.Timestamp, e.FixErr.Error())
	}
	return fmt.Sprintf("file %s is corrupted at %d, %v but has been fixed, base64 of the bad message is %s", e.File, e.Offset, e.Timestamp, base64.StdEncoding.EncodeToString(e.Message))
}

type ScanOffsetError struct {
	File           string
	Offset         uint64
	Timestamp      time.Time
	ExpectedOffset uint64
}

func (e *ScanOffsetError) Error() string {
	return fmt.Sprintf("offset out of order when scanning %s at %d, %v", e.File, e.Offset, e.Timestamp)
}

type ScanTruncatedError struct {
	File       string
	Size       int64
	FileOffset int64
}

func (e *ScanTruncatedError) Error() string {
	return fmt.Sprintf("file truncation detected in %s (size: %d) from offset %d", e.File, e.Size, e.FileOffset)
}
