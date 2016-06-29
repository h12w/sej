package sej

import (
	"flag"
	"fmt"
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
			if err := os.RemoveAll(file.Name()); err != nil {
				fmt.Println(err)
			}
		}
	}
	os.Exit(ret)
}
