package sej

import (
	"flag"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"
)

const testPrefix = "sej-test-"

func TestMain(m *testing.M) {
	flag.Parse()
	ret := m.Run()
	files, _ := ioutil.ReadDir(".")
	for _, file := range files {
		if file.IsDir() && strings.HasPrefix(path.Base(file.Name()), testPrefix) {
			os.RemoveAll(file.Name())
		}
	}
	os.Exit(ret)
}
