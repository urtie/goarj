//go:build linux

package arj

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestExtractAllStreamRejectsSymlinkSwapRace(t *testing.T) {
	entryName := "docs/readme.txt"
	archive := buildStreamArchive(t, []streamTestEntry{
		{
			header: FileHeader{Name: "docs/", Method: Store, fileType: arjFileTypeDirectory},
		},
		{
			header:  buildExtractHeader(entryName, 0o644, time.Date(2024, time.September, 1, 2, 3, 4, 0, time.UTC)),
			payload: []byte("race-resistant"),
		},
	})

	tmp := t.TempDir()
	out := filepath.Join(tmp, "out")
	if err := os.MkdirAll(filepath.Join(out, "docs"), 0o755); err != nil {
		t.Fatalf("MkdirAll(out/docs): %v", err)
	}
	outside := filepath.Join(tmp, "outside")
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatalf("MkdirAll(outside): %v", err)
	}

	prevHook := extractTestHookBeforeCreate
	extractTestHookBeforeCreate = func(name string) {
		if name != entryName {
			return
		}
		if err := os.Remove(filepath.Join(out, "docs")); err != nil {
			t.Fatalf("Remove(out/docs): %v", err)
		}
		symlinkOrSkip(t, outside, filepath.Join(out, "docs"))
	}
	t.Cleanup(func() {
		extractTestHookBeforeCreate = prevHook
	})

	err := ExtractAllStream(bytes.NewReader(archive), out)
	assertExtractInsecurePathError(t, err, entryName)

	if _, statErr := os.Stat(filepath.Join(outside, "readme.txt")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("outside file exists or stat failed: %v", statErr)
	}
}

func TestStreamReaderExtractAllRejectsSwappedStagedTempBeforeCommit(t *testing.T) {
	entryName := "docs/readme.txt"
	archive := buildStreamArchive(t, []streamTestEntry{
		{
			header: FileHeader{Name: "docs/", Method: Store, fileType: arjFileTypeDirectory},
		},
		{
			header:  buildExtractHeader(entryName, 0o644, time.Date(2024, time.September, 2, 2, 3, 4, 0, time.UTC)),
			payload: []byte("race-resistant"),
		},
	})

	sr, err := NewStreamReader(bytes.NewReader(archive))
	if err != nil {
		t.Fatalf("NewStreamReader: %v", err)
	}

	tmp := t.TempDir()
	out := filepath.Join(tmp, "out")
	if err := os.MkdirAll(filepath.Join(out, "docs"), 0o755); err != nil {
		t.Fatalf("MkdirAll(out/docs): %v", err)
	}

	prevHook := extractTestHookBeforeCommit
	extractTestHookBeforeCommit = func(name string, staged *os.File) error {
		if name != entryName {
			return nil
		}
		matches, globErr := filepath.Glob(filepath.Join(out, "docs", ".arj-extract-*"))
		if globErr != nil {
			t.Fatalf("Glob(staged temp): %v", globErr)
		}
		if len(matches) != 1 {
			t.Fatalf("staged temp matches = %v, want one match", matches)
		}
		stagedPath := matches[0]
		movedPath := stagedPath + ".moved"
		if err := os.Rename(stagedPath, movedPath); err != nil {
			t.Fatalf("Rename(staged -> moved): %v", err)
		}
		if err := os.WriteFile(stagedPath, []byte("attacker"), 0o600); err != nil {
			t.Fatalf("WriteFile(replacement staged): %v", err)
		}
		t.Cleanup(func() {
			_ = os.Remove(stagedPath)
			_ = os.Remove(movedPath)
		})
		return nil
	}
	t.Cleanup(func() {
		extractTestHookBeforeCommit = prevHook
	})

	err = sr.ExtractAll(out)
	assertExtractInsecurePathError(t, err, entryName)

	if _, statErr := os.Stat(filepath.Join(out, filepath.FromSlash(entryName))); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("destination file exists or stat failed: %v", statErr)
	}
}
