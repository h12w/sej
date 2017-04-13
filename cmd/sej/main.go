package main

import "h12.me/config"

// Command is the top-level command
type Command struct {
	Dump DumpCommand `
                command:"dump"
                description:"dump all messages from a journal file"`

	Count CountCommand `
                command:"count"
                description:"count messages in range"`

	LastOffset LastOffsetCommand `
                command:"last-offset"
                description:"print the last offset of a journal file"`

	Tail TailCommand `
                command:"tail"
                description:"print the tailing messages of a segmented journal directory"`

	Clean CleanCommand `
                command:"clean"
                description:"clean journal files according to cleaning rules"`

	Timestamp TimestampCommand `
                command:"timestamp"
                description:"show timestamp of an offset ina journal"`
}

func main() {
	config.ExecuteCommand(&Command{})
}
