//go:build linux

package arj

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReaderExtractAllWithStrictOptionsUnix(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "strict-unix.arj")
	writeExtractArchive(t, archivePath, []extractEntry{
		{
			header:  buildExtractHeader("docs/readme.txt", 0o600, time.Date(2024, time.July, 1, 1, 2, 3, 0, time.UTC)),
			payload: []byte("strict-ok"),
		},
	})

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	out := filepath.Join(tmp, "out")
	if err := r.ExtractAllWithOptions(out, StrictExtractOptions()); err != nil {
		t.Fatalf("ExtractAllWithOptions(strict): %v", err)
	}

	data, err := os.ReadFile(filepath.Join(out, "docs", "readme.txt"))
	if err != nil {
		t.Fatalf("ReadFile(docs/readme.txt): %v", err)
	}
	if string(data) != "strict-ok" {
		t.Fatalf("payload = %q, want %q", data, "strict-ok")
	}
}
