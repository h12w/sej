package proto

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"h12.me/sej"
)

func TestMarshal(t *testing.T) {
	req := Request{
		Title: RequestTitle{
			Verb:     uint8(PUT),
			ClientID: "b",
		},
		Header: &Put{
			JournalDir: "c.3.4",
		},
		Messages: []sej.Message{
			{
				Timestamp: time.Now().UTC().Truncate(time.Millisecond),
				Value:     []byte("a"),
			},
			{
				Timestamp: time.Now().UTC().Truncate(time.Millisecond),
				Value:     []byte("b"),
			},
		},
	}
	w := new(bytes.Buffer)
	if _, err := req.WriteTo(w); err != nil {
		t.Fatal(err)
	}
	var res Request
	if _, err := res.ReadFrom(bytes.NewReader(w.Bytes())); err != nil {
		t.Fatal(err)
	}
	if expect, actual := js(req), js(res); expect != actual {
		t.Fatalf("expect\n%v\ngot\n%v\n", expect, actual)
	}
}

func js(v interface{}) string {
	buf, _ := json.MarshalIndent(v, "", "    ")
	return string(buf)
}
