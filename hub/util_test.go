package hub

import (
	"testing"

	"github.com/pkg/errors"
	"h12.me/sej"
)

type stackTracer interface {
	StackTrace() errors.StackTrace
}

func TestMain(m *testing.M) {
	sej.Test{}.Main(m)
}
