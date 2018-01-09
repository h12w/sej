package cli

import (
	"h12.me/config"
	"h12.me/sej"
)

// Command is the top-level command
type Command struct {
	Dump DumpCommand `
                command:"dump"
                description:"dump all messages from a journal file"`

	Scan ScanCommand `
                command:"scan"
                description:"scan and print messages in range"`

	Count CountCommand `
                command:"count"
                description:"count messages in range"`

	Offset OffsetCommand `
                command:"offset"
                description:"print first, last offset and all consumer offsets of a journal directory"`

	Tail TailCommand `
                command:"tail"
                description:"print the tailing messages of a segmented journal directory"`

	Old OldCommand `
                command:"old"
                description:"print old journal files according to rules"`

	File FileCommand `
                command:"file"
                description:"print info about a journal file"`

	Timestamp TimestampCommand `
                command:"timestamp"
                description:"show timestamp of an offset in a journal directory"`

	Formatter Formatter
}

type Formatter interface {
	Sprint(msg *sej.Message) (string, error)
}

var DefaultFormatter Formatter = Format("json")

func Run() {
	config.ExecuteCommand(&Command{})
}
