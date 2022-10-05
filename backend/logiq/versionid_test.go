package logiq

import (
	"strings"
	"testing"

	gofakes3 "github.com/logiqai/s32http"
)

func TestVersionID(t *testing.T) {
	vid := newVersionGenerator(0, 32)

	var last gofakes3.VersionID
	for i := 0; i < 1000; i++ {
		next, _ := vid.Next(nil)
		if strings.Compare(string(last), string(next)) > 0 {
			t.Fatal("failed at index", i, "-", next, "<", last)
		}
		last = next
	}
}
