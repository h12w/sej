package hub

import (
	"testing"

	"github.com/pkg/errors"
	"h12.me/sej/sejtest"
)

type stackTracer interface {
	StackTrace() errors.StackTrace
}

func TestMain(m *testing.M) {
	sejtest.TestMain(m)
}
