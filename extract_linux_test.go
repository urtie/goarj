//go:build linux

package arj

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReaderExtractAllLinuxDoesNotUsePathBasedMetadataHelpers(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "linux-metadata-path-usage.arj")
	writeExtractArchive(t, archivePath, []extractEntry{
		{
			header:  buildExtractHeader("dir/", 0o755, time.Date(2024, time.August, 1, 1, 2, 3, 0, time.UTC)),
			payload: nil,
		},
		{
			header:  buildExtractHeader("dir/file.txt", 0o640, time.Date(2024, time.August, 1, 2, 3, 4, 0, time.UTC)),
			payload: []byte("linux-path-metadata-guard"),
		},
	})

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	var pathMetadataCalls int
	prevHook := extractTestHookPathMetadataApply
	extractTestHookPathMetadataApply = func(path, entryName string) {
		pathMetadataCalls++
		t.Fatalf("path-based metadata helper invoked for entry %q at path %q", entryName, path)
	}
	t.Cleanup(func() {
		extractTestHookPathMetadataApply = prevHook
	})

	out := filepath.Join(tmp, "out")
	if err := r.ExtractAll(out); err != nil {
		t.Fatalf("ExtractAll: %v", err)
	}
	if pathMetadataCalls != 0 {
		t.Fatalf("path-based metadata helper calls = %d, want 0", pathMetadataCalls)
	}

	got, err := os.ReadFile(filepath.Join(out, "dir", "file.txt"))
	if err != nil {
		t.Fatalf("ReadFile(extracted): %v", err)
	}
	if string(got) != "linux-path-metadata-guard" {
		t.Fatalf("extracted payload = %q, want %q", got, "linux-path-metadata-guard")
	}
}
