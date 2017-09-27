package sejtest

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"
)

const DirPrefix = "sej-test-"

// TestMain should be called to clear test directories
func TestMain(m *testing.M) {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())
	ret := m.Run()
	removeTestFiles()
	os.Exit(ret)
}

func removeTestFiles() {
	files, _ := ioutil.ReadDir(".")
	for _, file := range files {
		if file.IsDir() && strings.HasPrefix(path.Base(file.Name()), DirPrefix) {
			if err := os.RemoveAll(file.Name()); err != nil {
				fmt.Println(err)
			}
		}
	}

}

// NewPath creates a new test directory
// the path will be deleted automatically after the tests
func NewDir(t testing.TB) string {
	dir := DirPrefix + strconv.Itoa(rand.Int())
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatal(err)
	}
	return dir
}
