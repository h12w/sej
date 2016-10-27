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

const testFilePrefix = "sej-test-"

func TestMain(m *testing.M) {
	flag.Parse()
	ret := m.Run()
	removeTestFiles()
	os.Exit(ret)
}

func removeTestFiles() {
	files, _ := ioutil.ReadDir(".")
	for _, file := range files {
		if file.IsDir() && strings.HasPrefix(path.Base(file.Name()), testFilePrefix) {
			if err := os.RemoveAll(file.Name()); err != nil {
				fmt.Println(err)
			}
		}
	}

}
