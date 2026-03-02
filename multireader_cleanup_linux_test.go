//go:build linux

package arj

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOpenMultiReaderClosesOpenedVolumesWhenLaterVolumeParseFails(t *testing.T) {
	tmp := t.TempDir()
	base := filepath.Join(tmp, "cleanup")
	firstPath := base + ".arj"

	writeVolumeArchive(t, firstPath, []volumeEntry{
		{name: "ok.txt", payload: []byte("ok")},
	})
	// Keep the continuation present but malformed so first volume opens and
	// second-volume parse fails.
	mustWriteFile(t, base+".a01", []byte{arjHeaderID1, arjHeaderID2, 0x01})

	before := countLinuxFDRefsToPath(t, firstPath)
	_, err := OpenMultiReader(firstPath)
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("OpenMultiReader error = %v, want %v", err, ErrFormat)
	}
	after := countLinuxFDRefsToPath(t, firstPath)
	if after != before {
		t.Fatalf("open file descriptor leak for %s: before=%d after=%d", firstPath, before, after)
	}
}

func countLinuxFDRefsToPath(t *testing.T, path string) int {
	t.Helper()

	absPath, err := filepath.Abs(path)
	if err != nil {
		t.Fatalf("filepath.Abs(%s): %v", path, err)
	}
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skipf("cannot inspect /proc/self/fd: %v", err)
	}

	count := 0
	for _, entry := range entries {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if err != nil {
			continue
		}
		target = strings.TrimSuffix(target, " (deleted)")
		if target == absPath {
			count++
		}
	}
	return count
}
