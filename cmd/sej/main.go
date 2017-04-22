package main

import "h12.me/config"

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

	Timestamp TimestampCommand `
                command:"timestamp"
                description:"show timestamp of an offset in a journal directory"`
}

func main() {
	config.ExecuteCommand(&Command{})
}
