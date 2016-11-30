package main

import "h12.me/config"

// Command is the top-level command
type Command struct {
	Dump DumpCommand `
        command:"dump"
        description:"dump all messages from a journal file"`
	LastOffset LastOffsetCommand `
        command:"last-offset"
        description:"print the last offset of a journal file"`
	Tail TailCommand `
        command:"tail"
        description:"print the tailing messages of a segmented journal directory"`
}

func main() {
	config.ExecuteCommand(&Command{})
}
