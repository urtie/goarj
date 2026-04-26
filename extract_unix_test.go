//go:build linux

package arj

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type closeCountingReadCloser struct {
	io.Reader
	closeCount *int
}

func (r *closeCountingReadCloser) Close() error {
	if r.closeCount != nil {
		(*r.closeCount)++
	}
	return nil
}

func TestReaderExtractAllSymlinkSwapRace(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "symlink-swap-race.arj")
	entryName := "docs/readme.txt"
	writeExtractArchive(t, archivePath, []extractEntry{
		{
			header:  buildExtractHeader(entryName, 0o644, time.Date(2024, time.June, 2, 3, 4, 5, 0, time.UTC)),
			payload: []byte("race-resistant"),
		},
	})

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

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

	err = r.ExtractAll(out)
	assertExtractInsecurePathError(t, err, entryName)

	outsideWrite := filepath.Join(outside, "readme.txt")
	if _, statErr := os.Stat(outsideWrite); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("outside file %s exists or stat failed: %v", outsideWrite, statErr)
	}
}

func TestReaderExtractAllRejectsSwappedStagedTempBeforeCommit(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "staged-temp-swap-race.arj")
	entryName := "docs/readme.txt"
	writeExtractArchive(t, archivePath, []extractEntry{
		{
			header:  buildExtractHeader(entryName, 0o644, time.Date(2024, time.June, 3, 3, 4, 5, 0, time.UTC)),
			payload: []byte("race-resistant"),
		},
	})

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

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

	err = r.ExtractAll(out)
	assertExtractInsecurePathError(t, err, entryName)

	if _, statErr := os.Stat(filepath.Join(out, filepath.FromSlash(entryName))); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("destination file exists or stat failed: %v", statErr)
	}
}

func TestReaderExtractAllClosesOpenedEntryOnPreCopyFailure(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "close-on-precopy-failure.arj")
	entryName := "docs/readme.txt"
	writeExtractArchive(t, archivePath, []extractEntry{
		{
			header:  buildExtractHeader(entryName, 0o644, time.Date(2024, time.June, 4, 3, 4, 5, 0, time.UTC)),
			payload: []byte("race-resistant"),
		},
	})

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	closeCount := 0
	r.RegisterDecompressor(Store, func(in io.Reader) io.ReadCloser {
		return &closeCountingReadCloser{Reader: in, closeCount: &closeCount}
	})

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

	err = r.ExtractAll(out)
	assertExtractInsecurePathError(t, err, entryName)

	if got, want := closeCount, 1; got != want {
		t.Fatalf("close count = %d, want %d", got, want)
	}
}
