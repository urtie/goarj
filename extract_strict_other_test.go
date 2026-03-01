//go:build !unix

package arj

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReaderExtractAllWithStrictOptionsUnsupportedOnNonUnix(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "strict-other.arj")
	writeExtractArchive(t, archivePath, []extractEntry{
		{
			header:  buildExtractHeader("file.txt", 0o600, time.Date(2024, time.July, 1, 1, 2, 3, 0, time.UTC)),
			payload: []byte("strict-fail-closed"),
		},
	})

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	out := filepath.Join(tmp, "out")
	err = r.ExtractAllWithOptions(out, StrictExtractOptions())
	if !errors.Is(err, ErrStrictModeUnsupported) {
		t.Fatalf("ExtractAllWithOptions(strict) error = %v, want %v", err, ErrStrictModeUnsupported)
	}

	var pathErr *fs.PathError
	if !errors.As(err, &pathErr) {
		t.Fatalf("error type = %T, want *fs.PathError", err)
	}
	if pathErr.Op != "extract" {
		t.Fatalf("path error op = %q, want %q", pathErr.Op, "extract")
	}
	if pathErr.Path != out {
		t.Fatalf("path error path = %q, want %q", pathErr.Path, out)
	}

	if _, statErr := os.Stat(out); !errors.Is(statErr, fs.ErrNotExist) {
		t.Fatalf("output root exists or stat failed: %v", statErr)
	}
}
