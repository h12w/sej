package sej

import (
	"encoding/base64"
	"errors"
	"fmt"
)

var (
	// ErrCRC is returned when the CRC of an message value does not match the stored CRC
	ErrCRC = errors.New("CRC mismatch")
)

// CorruptionError is returned when the last message of a segmented journal file is corrupted
type CorruptionError struct {
	File    string
	Message []byte
	Err     error
	FixErr  error
}

func (e *CorruptionError) Error() string {
	if e.FixErr != nil {
		return fmt.Sprintf("file %s is corrupted but failed to fix it: %s", e.File, e.FixErr.Error())
	}
	return fmt.Sprintf("file %s is corrupted but has been fixed, base64 of the bad message is %s", e.File, base64.StdEncoding.EncodeToString(e.Message))
}
