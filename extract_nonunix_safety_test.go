//go:build !unix

package arj

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestNonUnixEnsureNoSymlinkComponentsRejectsSymlinkRootParentIntermediate(t *testing.T) {
	t.Run("root", func(t *testing.T) {
		tmp := t.TempDir()
		rootTarget := filepath.Join(tmp, "root-target")
		if err := os.MkdirAll(rootTarget, 0o755); err != nil {
			t.Fatalf("MkdirAll(root-target): %v", err)
		}
		rootLink := filepath.Join(tmp, "root-link")
		nonUnixSymlinkOrSkip(t, rootTarget, rootLink)
		t.Cleanup(func() {
			_ = os.Remove(rootLink)
		})

		target := filepath.Join(rootLink, "file.txt")
		err := ensureNoSymlinkComponents(rootLink, target, "file.txt", false)
		if !errors.Is(err, ErrInsecurePath) {
			t.Fatalf("ensureNoSymlinkComponents(root) error = %v, want %v", err, ErrInsecurePath)
		}
	})

	t.Run("parent", func(t *testing.T) {
		tmp := t.TempDir()
		root := filepath.Join(tmp, "root")
		if err := os.MkdirAll(root, 0o755); err != nil {
			t.Fatalf("MkdirAll(root): %v", err)
		}
		outside := filepath.Join(tmp, "outside")
		if err := os.MkdirAll(outside, 0o755); err != nil {
			t.Fatalf("MkdirAll(outside): %v", err)
		}
		linkPath := filepath.Join(root, "docs")
		nonUnixSymlinkOrSkip(t, outside, linkPath)
		t.Cleanup(func() {
			_ = os.Remove(linkPath)
		})

		target := filepath.Join(root, "docs", "readme.txt")
		err := ensureNoSymlinkComponents(root, target, "docs/readme.txt", false)
		if !errors.Is(err, ErrInsecurePath) {
			t.Fatalf("ensureNoSymlinkComponents(parent) error = %v, want %v", err, ErrInsecurePath)
		}
	})

	t.Run("intermediate", func(t *testing.T) {
		tmp := t.TempDir()
		root := filepath.Join(tmp, "root")
		if err := os.MkdirAll(filepath.Join(root, "nested"), 0o755); err != nil {
			t.Fatalf("MkdirAll(root/nested): %v", err)
		}
		outside := filepath.Join(tmp, "outside")
		if err := os.MkdirAll(outside, 0o755); err != nil {
			t.Fatalf("MkdirAll(outside): %v", err)
		}
		linkPath := filepath.Join(root, "nested", "link")
		nonUnixSymlinkOrSkip(t, outside, linkPath)
		t.Cleanup(func() {
			_ = os.Remove(linkPath)
		})

		target := filepath.Join(root, "nested", "link", "sub", "file.txt")
		err := ensureNoSymlinkComponents(root, target, "nested/link/sub/file.txt", false)
		if !errors.Is(err, ErrInsecurePath) {
			t.Fatalf("ensureNoSymlinkComponents(intermediate) error = %v, want %v", err, ErrInsecurePath)
		}
	})
}

func TestNonUnixExtractAllCreatesMissingParentDirectory(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "missing-parent-created.arj")
	entryName := "docs/readme.txt"
	nonUnixWriteExtractArchive(t, archivePath, []nonUnixExtractEntry{
		{
			header:  nonUnixBuildExtractHeader(entryName, 0o640, time.Date(2024, time.May, 12, 1, 2, 3, 0, time.UTC)),
			payload: []byte("payload"),
		},
	})

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	out := filepath.Join(tmp, "out")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatalf("MkdirAll(out): %v", err)
	}

	if err := r.ExtractAll(out); err != nil {
		t.Fatalf("ExtractAll: %v", err)
	}

	parentPath := filepath.Join(out, "docs")
	parentInfo, statErr := os.Stat(parentPath)
	if statErr != nil {
		t.Fatalf("Stat(docs): %v", statErr)
	}
	if !parentInfo.IsDir() {
		t.Fatalf("docs is not a directory: mode=%v", parentInfo.Mode())
	}

	got, readErr := os.ReadFile(filepath.Join(out, filepath.FromSlash(entryName)))
	if readErr != nil {
		t.Fatalf("ReadFile(%s): %v", entryName, readErr)
	}
	if string(got) != "payload" {
		t.Fatalf("payload = %q, want %q", got, "payload")
	}
}

func TestNonUnixExtractAllCreatesMissingArchiveDirectory(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "missing-dir-entry-created.arj")
	entryName := "docs/"
	nonUnixWriteExtractArchive(t, archivePath, []nonUnixExtractEntry{
		{
			header: nonUnixBuildExtractHeader(entryName, fs.ModeDir|0o750, time.Date(2024, time.May, 13, 1, 2, 3, 0, time.UTC)),
		},
	})

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	out := filepath.Join(tmp, "out")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatalf("MkdirAll(out): %v", err)
	}

	if err := r.ExtractAll(out); err != nil {
		t.Fatalf("ExtractAll: %v", err)
	}

	info, statErr := os.Stat(filepath.Join(out, "docs"))
	if statErr != nil {
		t.Fatalf("Stat(docs): %v", statErr)
	}
	if !info.IsDir() {
		t.Fatalf("docs is not a directory: mode=%v", info.Mode())
	}
}

func TestNonUnixExtractAllCanonicalRootIgnoresCWDChanges(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "canonical-root-cwd-swap.arj")
	entryName := "note.txt"
	nonUnixWriteExtractArchive(t, archivePath, []nonUnixExtractEntry{
		{
			header:  nonUnixBuildExtractHeader(entryName, 0o640, time.Date(2024, time.May, 13, 4, 5, 6, 0, time.UTC)),
			payload: []byte("payload"),
		},
	})

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	baseCWD := filepath.Join(tmp, "cwd-base")
	if err := os.MkdirAll(baseCWD, 0o755); err != nil {
		t.Fatalf("MkdirAll(baseCWD): %v", err)
	}
	otherCWD := filepath.Join(tmp, "cwd-other")
	if err := os.MkdirAll(otherCWD, 0o755); err != nil {
		t.Fatalf("MkdirAll(otherCWD): %v", err)
	}

	originalCWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	if err := os.Chdir(baseCWD); err != nil {
		t.Fatalf("Chdir(baseCWD): %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(originalCWD)
	})

	prevHook := extractTestHookBeforeCreate
	extractTestHookBeforeCreate = func(name string) {
		if name != entryName {
			return
		}
		if err := os.Chdir(otherCWD); err != nil {
			t.Fatalf("Chdir(otherCWD): %v", err)
		}
	}
	t.Cleanup(func() {
		extractTestHookBeforeCreate = prevHook
	})

	relRoot := "out"
	if err := r.ExtractAll(relRoot); err != nil {
		t.Fatalf("ExtractAll: %v", err)
	}

	anchoredTarget := filepath.Join(baseCWD, relRoot, entryName)
	got, readErr := os.ReadFile(anchoredTarget)
	if readErr != nil {
		t.Fatalf("ReadFile(anchored target): %v", readErr)
	}
	if string(got) != "payload" {
		t.Fatalf("anchored payload = %q, want %q", got, "payload")
	}

	shiftedTarget := filepath.Join(otherCWD, relRoot, entryName)
	if _, statErr := os.Stat(shiftedTarget); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("shifted target exists or stat failed: %v", statErr)
	}
}

func TestNonUnixExtractAllRenameFallbackReplacesExistingDestination(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "rename-fallback-overwrite.arj")
	entryName := "note.txt"
	nonUnixWriteExtractArchive(t, archivePath, []nonUnixExtractEntry{
		{
			header:  nonUnixBuildExtractHeader(entryName, 0o640, time.Date(2024, time.May, 14, 1, 2, 3, 0, time.UTC)),
			payload: []byte("replacement"),
		},
	})

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	out := filepath.Join(tmp, "out")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatalf("MkdirAll(out): %v", err)
	}
	target := filepath.Join(out, entryName)
	if err := os.WriteFile(target, []byte("original"), 0o600); err != nil {
		t.Fatalf("WriteFile(target): %v", err)
	}

	if err := r.ExtractAll(out); err != nil {
		t.Fatalf("ExtractAll: %v", err)
	}

	got, readErr := os.ReadFile(target)
	if readErr != nil {
		t.Fatalf("ReadFile(target): %v", readErr)
	}
	if string(got) != "replacement" {
		t.Fatalf("target payload = %q, want %q", got, "replacement")
	}
}

func TestNonUnixExtractAllRenameFallbackRejectsSymlinkDestination(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "rename-fallback-symlink-destination.arj")
	entryName := "note.txt"
	nonUnixWriteExtractArchive(t, archivePath, []nonUnixExtractEntry{
		{
			header:  nonUnixBuildExtractHeader(entryName, 0o640, time.Date(2024, time.May, 15, 1, 2, 3, 0, time.UTC)),
			payload: []byte("replacement"),
		},
	})

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	out := filepath.Join(tmp, "out")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatalf("MkdirAll(out): %v", err)
	}
	outside := filepath.Join(tmp, "outside")
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatalf("MkdirAll(outside): %v", err)
	}
	outsideTarget := filepath.Join(outside, entryName)
	originalOutside := []byte("outside-original")
	if err := os.WriteFile(outsideTarget, originalOutside, 0o600); err != nil {
		t.Fatalf("WriteFile(outside target): %v", err)
	}
	linkPath := filepath.Join(out, entryName)
	nonUnixSymlinkOrSkip(t, outsideTarget, linkPath)
	t.Cleanup(func() {
		_ = os.Remove(linkPath)
	})

	err = r.ExtractAll(out)
	assertNonUnixExtractInsecurePathError(t, err, entryName)

	gotOutside, readErr := os.ReadFile(outsideTarget)
	if readErr != nil {
		t.Fatalf("ReadFile(outside target): %v", readErr)
	}
	if string(gotOutside) != string(originalOutside) {
		t.Fatalf("outside payload = %q, want %q", gotOutside, originalOutside)
	}

	linkInfo, lstatErr := os.Lstat(linkPath)
	if lstatErr != nil {
		t.Fatalf("Lstat(linkPath): %v", lstatErr)
	}
	if linkInfo.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("destination was replaced; mode=%v", linkInfo.Mode())
	}
}

func TestNonUnixExtractAllRejectsSymlinkSwapBeforeTempCreate(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "precreate-parent-swap-race.arj")
	entryName := "docs/readme.txt"
	nonUnixWriteExtractArchive(t, archivePath, []nonUnixExtractEntry{
		{
			header:  nonUnixBuildExtractHeader(entryName, 0o640, time.Date(2024, time.May, 15, 2, 2, 3, 0, time.UTC)),
			payload: []byte("replacement"),
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
		docsPath := filepath.Join(out, "docs")
		if err := os.Remove(docsPath); err != nil {
			t.Fatalf("Remove(out/docs): %v", err)
		}
		nonUnixSymlinkOrSkip(t, outside, docsPath)
		t.Cleanup(func() {
			_ = os.Remove(docsPath)
		})
	}
	t.Cleanup(func() {
		extractTestHookBeforeCreate = prevHook
	})

	err = r.ExtractAll(out)
	assertNonUnixExtractInsecurePathError(t, err, entryName)

	outsideWrite := filepath.Join(outside, "readme.txt")
	if _, statErr := os.Stat(outsideWrite); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("outside file %s exists or stat failed: %v", outsideWrite, statErr)
	}
}

func TestNonUnixExtractAllRejectsSwappedStagedTempBeforeCommit(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "staged-temp-swap-race.arj")
	entryName := "note.txt"
	nonUnixWriteExtractArchive(t, archivePath, []nonUnixExtractEntry{
		{
			header:  nonUnixBuildExtractHeader(entryName, 0o640, time.Date(2024, time.May, 16, 1, 2, 3, 0, time.UTC)),
			payload: []byte("replacement"),
		},
	})

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	out := filepath.Join(tmp, "out")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatalf("MkdirAll(out): %v", err)
	}

	prevHook := extractTestHookBeforeCommit
	extractTestHookBeforeCommit = func(name string, staged *os.File) error {
		if name != entryName {
			return nil
		}
		stagedPath := staged.Name()
		movedPath := stagedPath + ".moved"
		if err := os.Rename(stagedPath, movedPath); err != nil {
			if errors.Is(err, fs.ErrPermission) || errors.Is(err, os.ErrPermission) {
				t.Skipf("cannot swap open staged temp in this environment: %v", err)
			}
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
	assertNonUnixExtractInsecurePathError(t, err, entryName)

	if _, statErr := os.Stat(filepath.Join(out, entryName)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("destination file exists or stat failed: %v", statErr)
	}
}

func TestNonUnixExtractAllRejectsSwappedDestinationAfterDirectRename(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "post-rename-direct-identity-check.arj")
	entryName := "note.txt"
	nonUnixWriteExtractArchive(t, archivePath, []nonUnixExtractEntry{
		{
			header:  nonUnixBuildExtractHeader(entryName, 0o640, time.Date(2024, time.May, 16, 2, 2, 3, 0, time.UTC)),
			payload: []byte("replacement"),
		},
	})

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	out := filepath.Join(tmp, "out")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatalf("MkdirAll(out): %v", err)
	}

	prevHook := extractTestHookAfterNonUnixCommitRename
	extractTestHookAfterNonUnixCommitRename = func(name, destination string) error {
		if name != entryName {
			return nil
		}
		movedPath := destination + ".moved"
		if err := os.Rename(destination, movedPath); err != nil {
			t.Fatalf("Rename(destination -> moved): %v", err)
		}
		if err := os.WriteFile(destination, []byte("attacker"), 0o600); err != nil {
			t.Fatalf("WriteFile(attacker destination): %v", err)
		}
		t.Cleanup(func() {
			_ = os.Remove(destination)
			_ = os.Remove(movedPath)
		})
		return nil
	}
	t.Cleanup(func() {
		extractTestHookAfterNonUnixCommitRename = prevHook
	})

	err = r.ExtractAll(out)
	assertNonUnixExtractInsecurePathError(t, err, entryName)

	if _, statErr := os.Stat(filepath.Join(out, entryName)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("destination file exists or stat failed: %v", statErr)
	}
}

func TestNonUnixExtractAllRenameFallbackLateFailureRestoresDestination(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "rename-fallback-late-failure-restore.arj")
	entryName := "note.txt"
	nonUnixWriteExtractArchive(t, archivePath, []nonUnixExtractEntry{
		{
			header:  nonUnixBuildExtractHeader(entryName, 0o640, time.Date(2024, time.May, 17, 1, 2, 3, 0, time.UTC)),
			payload: []byte("replacement"),
		},
	})

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	out := filepath.Join(tmp, "out")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatalf("MkdirAll(out): %v", err)
	}
	target := filepath.Join(out, entryName)
	originalPayload := []byte("original")
	if err := os.WriteFile(target, originalPayload, 0o600); err != nil {
		t.Fatalf("WriteFile(target): %v", err)
	}

	prevHook := extractTestHookBeforeNonUnixFallbackCommit
	extractTestHookBeforeNonUnixFallbackCommit = func(name, destination, stagedPath, backupPath string) error {
		if name != entryName {
			return nil
		}
		movedPath := stagedPath + ".late-failure"
		if err := os.Rename(stagedPath, movedPath); err != nil {
			t.Fatalf("Rename(staged -> moved): %v", err)
		}
		t.Cleanup(func() {
			_ = os.Remove(movedPath)
		})
		return nil
	}
	t.Cleanup(func() {
		extractTestHookBeforeNonUnixFallbackCommit = prevHook
	})

	err = r.ExtractAll(out)
	if err == nil {
		t.Fatalf("ExtractAll error = nil, want non-nil")
	}

	gotPayload, readErr := os.ReadFile(target)
	if readErr != nil {
		t.Fatalf("ReadFile(target): %v", readErr)
	}
	if string(gotPayload) != string(originalPayload) {
		t.Fatalf("target payload = %q, want %q", gotPayload, originalPayload)
	}
}

func TestNonUnixExtractAllRenameFallbackIdentityMismatchRestoresDestination(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "rename-fallback-identity-mismatch-restore.arj")
	entryName := "note.txt"
	nonUnixWriteExtractArchive(t, archivePath, []nonUnixExtractEntry{
		{
			header:  nonUnixBuildExtractHeader(entryName, 0o640, time.Date(2024, time.May, 17, 2, 2, 3, 0, time.UTC)),
			payload: []byte("replacement"),
		},
	})

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	out := filepath.Join(tmp, "out")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatalf("MkdirAll(out): %v", err)
	}
	target := filepath.Join(out, entryName)
	originalPayload := []byte("original")
	if err := os.WriteFile(target, originalPayload, 0o600); err != nil {
		t.Fatalf("WriteFile(target): %v", err)
	}

	prevHook := extractTestHookAfterNonUnixCommitRename
	extractTestHookAfterNonUnixCommitRename = func(name, destination string) error {
		if name != entryName {
			return nil
		}
		movedPath := destination + ".moved"
		if err := os.Rename(destination, movedPath); err != nil {
			t.Fatalf("Rename(destination -> moved): %v", err)
		}
		if err := os.WriteFile(destination, []byte("attacker"), 0o600); err != nil {
			t.Fatalf("WriteFile(attacker destination): %v", err)
		}
		t.Cleanup(func() {
			_ = os.Remove(destination)
			_ = os.Remove(movedPath)
		})
		return nil
	}
	t.Cleanup(func() {
		extractTestHookAfterNonUnixCommitRename = prevHook
	})

	err = r.ExtractAll(out)
	assertNonUnixExtractInsecurePathError(t, err, entryName)

	gotPayload, readErr := os.ReadFile(target)
	if readErr != nil {
		t.Fatalf("ReadFile(target): %v", readErr)
	}
	if string(gotPayload) != string(originalPayload) {
		t.Fatalf("target payload = %q, want %q", gotPayload, originalPayload)
	}
}

func TestNonUnixExtractAllRejectsSwappedTempPathBeforeTimestamp(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "temp-metadata-path-swap-race.arj")
	entryName := "note.txt"
	metadataTime := time.Date(2024, time.May, 18, 1, 2, 3, 0, time.UTC)
	nonUnixWriteExtractArchive(t, archivePath, []nonUnixExtractEntry{
		{
			header:  nonUnixBuildExtractHeader(entryName, 0o640, metadataTime),
			payload: []byte("replacement"),
		},
	})

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	out := filepath.Join(tmp, "out")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatalf("MkdirAll(out): %v", err)
	}
	outside := filepath.Join(tmp, "outside")
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatalf("MkdirAll(outside): %v", err)
	}
	outsideTarget := filepath.Join(outside, "outside.txt")
	outsideTime := time.Date(2024, time.May, 19, 1, 2, 3, 0, time.UTC)
	if err := os.WriteFile(outsideTarget, []byte("outside"), 0o600); err != nil {
		t.Fatalf("WriteFile(outside target): %v", err)
	}
	if err := os.Chtimes(outsideTarget, outsideTime, outsideTime); err != nil {
		t.Fatalf("Chtimes(outside target): %v", err)
	}

	prevHook := extractTestHookBeforeTempPathMetadataTimestamp
	extractTestHookBeforeTempPathMetadataTimestamp = func(path, name string, staged *os.File) error {
		if name != entryName {
			return nil
		}
		movedPath := path + ".moved"
		if err := os.Rename(path, movedPath); err != nil {
			if errors.Is(err, fs.ErrPermission) || errors.Is(err, os.ErrPermission) {
				t.Skipf("cannot swap temp metadata path in this environment: %v", err)
			}
			t.Fatalf("Rename(staged -> moved): %v", err)
		}
		nonUnixSymlinkOrSkip(t, outsideTarget, path)
		t.Cleanup(func() {
			_ = os.Remove(path)
			_ = os.Remove(movedPath)
		})
		return nil
	}
	t.Cleanup(func() {
		extractTestHookBeforeTempPathMetadataTimestamp = prevHook
	})

	err = r.ExtractAll(out)
	assertNonUnixExtractInsecurePathError(t, err, entryName)

	if _, statErr := os.Stat(filepath.Join(out, entryName)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("destination file exists or stat failed: %v", statErr)
	}

	outsideInfo, statErr := os.Stat(outsideTarget)
	if statErr != nil {
		t.Fatalf("Stat(outside target): %v", statErr)
	}
	if !nonUnixModTimeClose(outsideInfo.ModTime(), outsideTime) {
		t.Fatalf("outside modtime = %s, want %s", outsideInfo.ModTime().UTC(), outsideTime.UTC())
	}
}

func TestNonUnixExtractAllRejectsTempPathSwapDuringTimestampCall(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "temp-metadata-path-swap-during-chtime-race.arj")
	entryName := "note.txt"
	metadataTime := time.Date(2024, time.May, 18, 7, 8, 9, 0, time.UTC)
	nonUnixWriteExtractArchive(t, archivePath, []nonUnixExtractEntry{
		{
			header:  nonUnixBuildExtractHeader(entryName, 0o640, metadataTime),
			payload: []byte("replacement"),
		},
	})

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	out := filepath.Join(tmp, "out")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatalf("MkdirAll(out): %v", err)
	}
	outside := filepath.Join(tmp, "outside")
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatalf("MkdirAll(outside): %v", err)
	}
	outsideTarget := filepath.Join(outside, "outside.txt")
	if err := os.WriteFile(outsideTarget, []byte("outside"), 0o600); err != nil {
		t.Fatalf("WriteFile(outside target): %v", err)
	}

	prevBeforeCall := extractTestHookBeforeTempPathMetadataTimestampCall
	prevAfterCall := extractTestHookAfterTempPathMetadataTimestampCall
	var movedPath string
	extractTestHookBeforeTempPathMetadataTimestampCall = func(path, name string, staged *os.File) error {
		if name != entryName {
			return nil
		}
		movedPath = path + ".moved"
		if err := os.Rename(path, movedPath); err != nil {
			if errors.Is(err, fs.ErrPermission) || errors.Is(err, os.ErrPermission) {
				t.Skipf("cannot swap temp metadata path in this environment: %v", err)
			}
			t.Fatalf("Rename(staged -> moved): %v", err)
		}
		nonUnixSymlinkOrSkip(t, outsideTarget, path)
		return nil
	}
	extractTestHookAfterTempPathMetadataTimestampCall = func(path, name string, staged *os.File) error {
		if name != entryName {
			return nil
		}
		if err := os.Remove(path); err != nil {
			t.Fatalf("Remove(swapped staged path): %v", err)
		}
		if err := os.Rename(movedPath, path); err != nil {
			t.Fatalf("Rename(moved -> staged): %v", err)
		}
		return nil
	}
	t.Cleanup(func() {
		extractTestHookBeforeTempPathMetadataTimestampCall = prevBeforeCall
		extractTestHookAfterTempPathMetadataTimestampCall = prevAfterCall
		if movedPath != "" {
			_ = os.Remove(movedPath)
		}
	})

	err = r.ExtractAll(out)
	assertNonUnixExtractInsecurePathError(t, err, entryName)

	if _, statErr := os.Stat(filepath.Join(out, entryName)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("destination file exists or stat failed: %v", statErr)
	}
}

func TestNonUnixApplyExtractMetadataRejectsSymlinkPath(t *testing.T) {
	tmp := t.TempDir()
	target := filepath.Join(tmp, "target.txt")
	if err := os.WriteFile(target, []byte("payload"), 0o600); err != nil {
		t.Fatalf("WriteFile(target): %v", err)
	}
	originalTime := time.Date(2024, time.May, 10, 1, 2, 3, 0, time.UTC)
	if err := os.Chtimes(target, originalTime, originalTime); err != nil {
		t.Fatalf("Chtimes(target): %v", err)
	}

	link := filepath.Join(tmp, "target-link")
	nonUnixSymlinkOrSkip(t, target, link)

	newTime := time.Date(2024, time.May, 11, 1, 2, 3, 0, time.UTC)
	err := applyExtractMetadata(link, 0o777, newTime)
	if !errors.Is(err, ErrInsecurePath) {
		t.Fatalf("applyExtractMetadata error = %v, want %v", err, ErrInsecurePath)
	}
	var pathErr *fs.PathError
	if !errors.As(err, &pathErr) {
		t.Fatalf("applyExtractMetadata error type = %T, want *fs.PathError", err)
	}
	if pathErr.Op != "extract" {
		t.Fatalf("path error op = %q, want %q", pathErr.Op, "extract")
	}
	if pathErr.Path != link {
		t.Fatalf("path error path = %q, want %q", pathErr.Path, link)
	}

	info, statErr := os.Stat(target)
	if statErr != nil {
		t.Fatalf("Stat(target): %v", statErr)
	}
	if !nonUnixModTimeClose(info.ModTime(), originalTime) {
		t.Fatalf("target modtime = %s, want %s", info.ModTime().UTC(), originalTime.UTC())
	}
}

func TestNonUnixApplyExtractMetadataRejectsSwappedPathBeforeTimestamp(t *testing.T) {
	tmp := t.TempDir()
	target := filepath.Join(tmp, "target.txt")
	if err := os.WriteFile(target, []byte("payload"), 0o600); err != nil {
		t.Fatalf("WriteFile(target): %v", err)
	}
	originalTargetTime := time.Date(2024, time.May, 20, 1, 2, 3, 0, time.UTC)
	if err := os.Chtimes(target, originalTargetTime, originalTargetTime); err != nil {
		t.Fatalf("Chtimes(target): %v", err)
	}

	outside := filepath.Join(tmp, "outside.txt")
	originalOutsideTime := time.Date(2024, time.May, 21, 1, 2, 3, 0, time.UTC)
	if err := os.WriteFile(outside, []byte("outside"), 0o600); err != nil {
		t.Fatalf("WriteFile(outside): %v", err)
	}
	if err := os.Chtimes(outside, originalOutsideTime, originalOutsideTime); err != nil {
		t.Fatalf("Chtimes(outside): %v", err)
	}

	prevHook := extractTestHookBeforePathMetadataTimestamp
	extractTestHookBeforePathMetadataTimestamp = func(path, entryName string) error {
		if path != target || entryName != target {
			return nil
		}
		movedPath := target + ".moved"
		if err := os.Rename(target, movedPath); err != nil {
			if errors.Is(err, fs.ErrPermission) || errors.Is(err, os.ErrPermission) {
				t.Skipf("cannot swap metadata path in this environment: %v", err)
			}
			t.Fatalf("Rename(target -> moved): %v", err)
		}
		nonUnixSymlinkOrSkip(t, outside, target)
		t.Cleanup(func() {
			_ = os.Remove(target)
			_ = os.Remove(movedPath)
		})
		return nil
	}
	t.Cleanup(func() {
		extractTestHookBeforePathMetadataTimestamp = prevHook
	})

	newTime := time.Date(2024, time.May, 22, 1, 2, 3, 0, time.UTC)
	err := applyExtractMetadata(target, 0o777, newTime)
	if !errors.Is(err, ErrInsecurePath) {
		t.Fatalf("applyExtractMetadata error = %v, want %v", err, ErrInsecurePath)
	}
	var pathErr *fs.PathError
	if !errors.As(err, &pathErr) {
		t.Fatalf("applyExtractMetadata error type = %T, want *fs.PathError", err)
	}
	if pathErr.Op != "extract" {
		t.Fatalf("path error op = %q, want %q", pathErr.Op, "extract")
	}
	if pathErr.Path != target {
		t.Fatalf("path error path = %q, want %q", pathErr.Path, target)
	}

	outsideInfo, statErr := os.Stat(outside)
	if statErr != nil {
		t.Fatalf("Stat(outside): %v", statErr)
	}
	if !nonUnixModTimeClose(outsideInfo.ModTime(), originalOutsideTime) {
		t.Fatalf("outside modtime = %s, want %s", outsideInfo.ModTime().UTC(), originalOutsideTime.UTC())
	}
}

func TestNonUnixApplyExtractMetadataRejectsSwappedPathDuringTimestampCall(t *testing.T) {
	tmp := t.TempDir()
	target := filepath.Join(tmp, "target.txt")
	if err := os.WriteFile(target, []byte("payload"), 0o600); err != nil {
		t.Fatalf("WriteFile(target): %v", err)
	}
	originalTargetTime := time.Date(2024, time.May, 23, 1, 2, 3, 0, time.UTC)
	if err := os.Chtimes(target, originalTargetTime, originalTargetTime); err != nil {
		t.Fatalf("Chtimes(target): %v", err)
	}

	outside := filepath.Join(tmp, "outside.txt")
	if err := os.WriteFile(outside, []byte("outside"), 0o600); err != nil {
		t.Fatalf("WriteFile(outside): %v", err)
	}

	prevBeforeCall := extractTestHookBeforePathMetadataTimestampCall
	prevAfterCall := extractTestHookAfterPathMetadataTimestampCall
	var movedPath string
	extractTestHookBeforePathMetadataTimestampCall = func(path, entryName string) error {
		if path != target || entryName != target {
			return nil
		}
		movedPath = target + ".moved"
		if err := os.Rename(target, movedPath); err != nil {
			if errors.Is(err, fs.ErrPermission) || errors.Is(err, os.ErrPermission) {
				t.Skipf("cannot swap metadata path in this environment: %v", err)
			}
			t.Fatalf("Rename(target -> moved): %v", err)
		}
		nonUnixSymlinkOrSkip(t, outside, target)
		return nil
	}
	extractTestHookAfterPathMetadataTimestampCall = func(path, entryName string) error {
		if path != target || entryName != target {
			return nil
		}
		if err := os.Remove(target); err != nil {
			t.Fatalf("Remove(swapped target): %v", err)
		}
		if err := os.Rename(movedPath, target); err != nil {
			t.Fatalf("Rename(moved -> target): %v", err)
		}
		return nil
	}
	t.Cleanup(func() {
		extractTestHookBeforePathMetadataTimestampCall = prevBeforeCall
		extractTestHookAfterPathMetadataTimestampCall = prevAfterCall
		if movedPath != "" {
			_ = os.Remove(movedPath)
		}
	})

	newTime := time.Date(2024, time.May, 24, 1, 2, 3, 0, time.UTC)
	err := applyExtractMetadata(target, 0o777, newTime)
	if !errors.Is(err, ErrInsecurePath) {
		t.Fatalf("applyExtractMetadata error = %v, want %v", err, ErrInsecurePath)
	}
	var pathErr *fs.PathError
	if !errors.As(err, &pathErr) {
		t.Fatalf("applyExtractMetadata error type = %T, want *fs.PathError", err)
	}
	if pathErr.Op != "extract" {
		t.Fatalf("path error op = %q, want %q", pathErr.Op, "extract")
	}
	if pathErr.Path != target {
		t.Fatalf("path error path = %q, want %q", pathErr.Path, target)
	}

	targetInfo, statErr := os.Stat(target)
	if statErr != nil {
		t.Fatalf("Stat(target): %v", statErr)
	}
	if !nonUnixModTimeClose(targetInfo.ModTime(), originalTargetTime) {
		t.Fatalf("target modtime = %s, want %s", targetInfo.ModTime().UTC(), originalTargetTime.UTC())
	}
}

func assertNonUnixExtractInsecurePathError(t *testing.T, err error, entryName string) {
	t.Helper()
	if !errors.Is(err, ErrInsecurePath) {
		t.Fatalf("ExtractAll error = %v, want %v", err, ErrInsecurePath)
	}
	var pathErr *fs.PathError
	if !errors.As(err, &pathErr) {
		t.Fatalf("ExtractAll error type = %T, want *fs.PathError", err)
	}
	if pathErr.Op != "extract" {
		t.Fatalf("path error op = %q, want %q", pathErr.Op, "extract")
	}
	if pathErr.Path != entryName {
		t.Fatalf("path error path = %q, want %q", pathErr.Path, entryName)
	}
}

func nonUnixSymlinkOrSkip(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		if runtime.GOOS == "windows" || errors.Is(err, fs.ErrPermission) || errors.Is(err, os.ErrPermission) {
			t.Skipf("Symlink not supported in this environment: %v", err)
		}
		t.Fatalf("Symlink(%q -> %q): %v", link, target, err)
	}
}

type nonUnixExtractEntry struct {
	header  FileHeader
	payload []byte
}

func nonUnixBuildExtractHeader(name string, mode fs.FileMode, modTime time.Time) FileHeader {
	h := FileHeader{
		Name:   name,
		Method: Store,
	}
	h.SetMode(mode)
	h.SetModTime(modTime)
	return h
}

func nonUnixWriteExtractArchive(t *testing.T, path string, entries []nonUnixExtractEntry) {
	t.Helper()

	var buf bytes.Buffer
	w := NewWriter(&buf)

	for _, entry := range entries {
		h := entry.header
		fw, err := w.CreateHeader(&h)
		if err != nil {
			t.Fatalf("CreateHeader(%s): %v", h.Name, err)
		}
		if len(entry.payload) == 0 {
			continue
		}
		if _, err := fw.Write(entry.payload); err != nil {
			t.Fatalf("Write(%s): %v", h.Name, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}

func nonUnixModTimeClose(got, want time.Time) bool {
	diff := got.Sub(want)
	if diff < 0 {
		diff = -diff
	}
	return diff <= 2*time.Second
}
