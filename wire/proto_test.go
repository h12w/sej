package wire

import (
	"bytes"
	"reflect"
	"testing"
)

func TestMarshal(t *testing.T) {
	req := Request{
		ID:         1,
		Type:       2,
		ClientID:   "b",
		JournalDir: "c.3.4",
	}
	w := new(bytes.Buffer)
	if _, err := req.WriteTo(w); err != nil {
		t.Fatal(err)
	}
	var res Request
	if _, err := res.ReadFrom(bytes.NewReader(w.Bytes())); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(req, res) {
		t.Fatalf("expect\n%v\ngot\n%v\n", req, res)
	}
}
