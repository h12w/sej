package hub

import (
	"encoding/gob"

	"h12.me/sej"
)

type (
	Request struct {
		ClientID string
		Command  Command
	}
	Command interface {
		isCommand()
	}

	Messages []sej.Message

	Put struct {
		JournalDir string
		Messages   Messages
	}

	Get struct {
		JournalDir string
		Offset     uint64
	}
	GetResponse struct {
		Messages Messages
	}

	Quit struct {
		JournalDir string
	}

	Response struct {
		Err string
	}
)

func init() {
	gob.Register(&Put{})
	gob.Register(&Get{})
	gob.Register(&Quit{})
}

func (Put) isCommand()  {}
func (Get) isCommand()  {}
func (Quit) isCommand() {}
