package arj

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestReadCloserExtractAll(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "extract.arj")

	writeExtractArchive(t, archivePath, []extractEntry{
		{
			header: buildExtractHeader("docs/", fs.ModeDir|0o750, time.Date(2021, time.July, 5, 12, 0, 2, 0, time.UTC)),
		},
		{
			header:  buildExtractHeader("docs/readme.txt", 0o640, time.Date(2022, time.August, 6, 9, 10, 4, 0, time.UTC)),
			payload: []byte("hello docs"),
		},
		{
			header:  buildExtractHeader("nested/child/run.sh", 0o755, time.Date(2023, time.September, 7, 8, 0, 6, 0, time.UTC)),
			payload: []byte("#!/bin/sh\necho ok\n"),
		},
	})

	rc, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer rc.Close()

	out := filepath.Join(tmp, "out")
	if err := os.MkdirAll(filepath.Join(out, "docs"), 0o755); err != nil {
		t.Fatalf("MkdirAll(out/docs): %v", err)
	}
	if err := os.MkdirAll(filepath.Join(out, "nested", "child"), 0o755); err != nil {
		t.Fatalf("MkdirAll(out/nested/child): %v", err)
	}
	expectedMeta := make(map[string]struct {
		mode    fs.FileMode
		modTime time.Time
	})
	for _, f := range rc.File {
		path, err := SafeExtractPath(out, f.Name)
		if err != nil {
			t.Fatalf("SafeExtractPath(%q): %v", f.Name, err)
		}
		expectedMeta[path] = struct {
			mode    fs.FileMode
			modTime time.Time
		}{
			mode:    f.Mode(),
			modTime: f.ModTime(),
		}
	}

	if err := rc.ExtractAll(out); err != nil {
		t.Fatalf("ExtractAll: %v", err)
	}

	gotReadme, err := os.ReadFile(filepath.Join(out, "docs", "readme.txt"))
	if err != nil {
		t.Fatalf("ReadFile(docs/readme.txt): %v", err)
	}
	if string(gotReadme) != "hello docs" {
		t.Fatalf("readme payload = %q, want %q", gotReadme, "hello docs")
	}

	gotScript, err := os.ReadFile(filepath.Join(out, "nested", "child", "run.sh"))
	if err != nil {
		t.Fatalf("ReadFile(nested/child/run.sh): %v", err)
	}
	if string(gotScript) != "#!/bin/sh\necho ok\n" {
		t.Fatalf("run.sh payload = %q, want %q", gotScript, "#!/bin/sh\necho ok\n")
	}

	if _, err := os.Stat(filepath.Join(out, "nested", "child")); err != nil {
		t.Fatalf("Stat(nested/child): %v", err)
	}

	for path, want := range expectedMeta {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("Stat(%s): %v", path, err)
		}
		if !modTimeClose(info.ModTime(), want.modTime) {
			t.Fatalf("modtime(%s) = %s, want %s", path, info.ModTime().UTC(), want.modTime.UTC())
		}
		if runtime.GOOS != "windows" {
			if gotPerm, wantPerm := info.Mode().Perm(), want.mode.Perm(); gotPerm != wantPerm {
				t.Fatalf("perm(%s) = %#o, want %#o", path, gotPerm, wantPerm)
			}
		}
	}
}

func TestReaderExtractAllRejectsInsecurePath(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	fw, err := w.CreateHeader(&FileHeader{Name: "../escape.txt", Method: Store})
	if err != nil {
		t.Fatalf("CreateHeader: %v", err)
	}
	if _, err := io.WriteString(fw, "escape"); err != nil {
		t.Fatalf("WriteString: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	root := filepath.Join(t.TempDir(), "out")
	err = r.ExtractAll(root)
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
	if pathErr.Path != "../escape.txt" {
		t.Fatalf("path error path = %q, want %q", pathErr.Path, "../escape.txt")
	}

	outside := filepath.Join(filepath.Dir(root), "escape.txt")
	if _, statErr := os.Stat(outside); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("outside file %s exists or stat failed: %v", outside, statErr)
	}
}

func TestReaderExtractAllRejectsPreexistingSymlinkParent(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "symlink-parent.arj")
	entryName := "docs/readme.txt"
	writeExtractArchive(t, archivePath, []extractEntry{
		{
			header:  buildExtractHeader(entryName, 0o600, time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC)),
			payload: []byte("should-not-escape"),
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

	symlinkOrSkip(t, outside, filepath.Join(out, "docs"))

	err = r.ExtractAll(out)
	assertExtractInsecurePathError(t, err, entryName)

	outsideWrite := filepath.Join(outside, "readme.txt")
	if _, statErr := os.Stat(outsideWrite); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("outside file %s exists or stat failed: %v", outsideWrite, statErr)
	}
}

func TestReaderExtractAllRejectsSymlinkInIntermediateDir(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "symlink-intermediate.arj")
	entryName := "nested/link/sub/file.txt"
	writeExtractArchive(t, archivePath, []extractEntry{
		{
			header:  buildExtractHeader(entryName, 0o600, time.Date(2024, time.January, 3, 3, 4, 5, 0, time.UTC)),
			payload: []byte("should-not-escape"),
		},
	})

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	out := filepath.Join(tmp, "out")
	if err := os.MkdirAll(filepath.Join(out, "nested"), 0o755); err != nil {
		t.Fatalf("MkdirAll(out/nested): %v", err)
	}
	outside := filepath.Join(tmp, "outside")
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatalf("MkdirAll(outside): %v", err)
	}

	symlinkOrSkip(t, outside, filepath.Join(out, "nested", "link"))

	err = r.ExtractAll(out)
	assertExtractInsecurePathError(t, err, entryName)

	outsideWrite := filepath.Join(outside, "sub", "file.txt")
	if _, statErr := os.Stat(outsideWrite); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("outside file %s exists or stat failed: %v", outsideWrite, statErr)
	}
}

func TestReaderExtractAllRejectsSymlinkRoot(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "symlink-root.arj")
	entryName := "file.txt"
	writeExtractArchive(t, archivePath, []extractEntry{
		{
			header:  buildExtractHeader(entryName, 0o644, time.Date(2024, time.February, 2, 3, 4, 5, 0, time.UTC)),
			payload: []byte("root-symlink"),
		},
	})

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	outside := filepath.Join(tmp, "outside")
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatalf("MkdirAll(outside): %v", err)
	}
	rootLink := filepath.Join(tmp, "out-link")
	symlinkOrSkip(t, outside, rootLink)

	err = r.ExtractAll(rootLink)
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
	if pathErr.Path != rootLink {
		t.Fatalf("path error path = %q, want %q", pathErr.Path, rootLink)
	}

	if _, statErr := os.Stat(filepath.Join(outside, entryName)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("outside file exists or stat failed: %v", statErr)
	}
}

func TestReaderExtractAllRejectsSymlinkAncestorInOutputRoot(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "symlink-ancestor-root.arj")
	entryName := "file.txt"
	writeExtractArchive(t, archivePath, []extractEntry{
		{
			header:  buildExtractHeader(entryName, 0o644, time.Date(2024, time.February, 3, 3, 4, 5, 0, time.UTC)),
			payload: []byte("ancestor-symlink"),
		},
	})

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	outside := filepath.Join(tmp, "outside")
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatalf("MkdirAll(outside): %v", err)
	}
	pivotLink := filepath.Join(tmp, "pivot-link")
	symlinkOrSkip(t, outside, pivotLink)

	out := filepath.Join(pivotLink, "child", "out")
	err = r.ExtractAll(out)
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
	if pathErr.Path != out {
		t.Fatalf("path error path = %q, want %q", pathErr.Path, out)
	}

	if _, statErr := os.Stat(filepath.Join(outside, "child", "out", entryName)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("outside file exists or stat failed: %v", statErr)
	}
}

func TestReaderExtractAllUsesDefaultLimits(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "default-limits.arj")
	writeExtractArchive(t, archivePath, makeManySmallFileEntries(DefaultExtractMaxFiles+1))

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	out := filepath.Join(tmp, "out")
	err = r.ExtractAll(out)
	if err == nil || !strings.Contains(err.Error(), "max files exceeded") {
		t.Fatalf("ExtractAll error = %v, want max files exceeded", err)
	}
	if _, statErr := os.Stat(filepath.Join(out, fmt.Sprintf("f-%05d.txt", DefaultExtractMaxFiles))); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("file beyond default max exists or stat failed: %v", statErr)
	}
}

func TestReaderExtractAllWithUnlimitedOptions(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "unlimited-limits.arj")
	writeExtractArchive(t, archivePath, makeManySmallFileEntries(DefaultExtractMaxFiles+1))

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	out := filepath.Join(tmp, "out")
	if err := r.ExtractAllWithOptions(out, UnlimitedExtractOptions()); err != nil {
		t.Fatalf("ExtractAllWithOptions(unlimited): %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(out, fmt.Sprintf("f-%05d.txt", DefaultExtractMaxFiles))); statErr != nil {
		t.Fatalf("expected file missing: %v", statErr)
	}
}

func TestStrictExtractOptions(t *testing.T) {
	opts := StrictExtractOptions()
	if !opts.Strict {
		t.Fatalf("StrictExtractOptions().Strict = false, want true")
	}
	if opts.MaxFiles != DefaultExtractMaxFiles {
		t.Fatalf("StrictExtractOptions().MaxFiles = %d, want %d", opts.MaxFiles, DefaultExtractMaxFiles)
	}
	if opts.MaxTotalBytes != DefaultExtractMaxTotalBytes {
		t.Fatalf("StrictExtractOptions().MaxTotalBytes = %d, want %d", opts.MaxTotalBytes, DefaultExtractMaxTotalBytes)
	}
	if opts.MaxFileBytes != DefaultExtractMaxFileBytes {
		t.Fatalf("StrictExtractOptions().MaxFileBytes = %d, want %d", opts.MaxFileBytes, DefaultExtractMaxFileBytes)
	}
}

func TestReaderExtractAllWithOptionsMaxFiles(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "max-files.arj")
	writeExtractArchive(t, archivePath, []extractEntry{
		{
			header:  buildExtractHeader("a.txt", 0o600, time.Date(2024, time.March, 1, 1, 2, 3, 0, time.UTC)),
			payload: []byte("a"),
		},
		{
			header:  buildExtractHeader("b.txt", 0o600, time.Date(2024, time.March, 1, 1, 2, 4, 0, time.UTC)),
			payload: []byte("b"),
		},
	})

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	out := filepath.Join(tmp, "out")
	err = r.ExtractAllWithOptions(out, ExtractOptions{MaxFiles: 1})
	if err == nil || !strings.Contains(err.Error(), "max files exceeded") {
		t.Fatalf("ExtractAllWithOptions error = %v, want max files exceeded", err)
	}
	if _, statErr := os.Stat(filepath.Join(out, "a.txt")); statErr != nil {
		t.Fatalf("first file missing: %v", statErr)
	}
	if _, statErr := os.Stat(filepath.Join(out, "b.txt")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("second file exists or stat failed: %v", statErr)
	}
}

func TestReaderExtractAllWithOptionsMaxFileBytes(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "max-file-bytes.arj")
	writeExtractArchive(t, archivePath, []extractEntry{
		{
			header:  buildExtractHeader("big.txt", 0o600, time.Date(2024, time.March, 2, 1, 2, 3, 0, time.UTC)),
			payload: []byte("123456"),
		},
	})

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	out := filepath.Join(tmp, "out")
	err = r.ExtractAllWithOptions(out, ExtractOptions{MaxFileBytes: 5})
	if err == nil || !strings.Contains(err.Error(), "max file bytes exceeded") {
		t.Fatalf("ExtractAllWithOptions error = %v, want max file bytes exceeded", err)
	}
	if _, statErr := os.Stat(filepath.Join(out, "big.txt")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("output file exists or stat failed: %v", statErr)
	}
}

func TestReaderExtractAllWithOptionsMaxTotalBytes(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "max-total-bytes.arj")
	writeExtractArchive(t, archivePath, []extractEntry{
		{
			header:  buildExtractHeader("first.txt", 0o600, time.Date(2024, time.March, 3, 1, 2, 3, 0, time.UTC)),
			payload: []byte("1111"),
		},
		{
			header:  buildExtractHeader("second.txt", 0o600, time.Date(2024, time.March, 3, 1, 2, 4, 0, time.UTC)),
			payload: []byte("2222"),
		},
	})

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	out := filepath.Join(tmp, "out")
	err = r.ExtractAllWithOptions(out, ExtractOptions{MaxTotalBytes: 6})
	if err == nil || !strings.Contains(err.Error(), "max total bytes exceeded") {
		t.Fatalf("ExtractAllWithOptions error = %v, want max total bytes exceeded", err)
	}
	first, readErr := os.ReadFile(filepath.Join(out, "first.txt"))
	if readErr != nil {
		t.Fatalf("ReadFile(first.txt): %v", readErr)
	}
	if string(first) != "1111" {
		t.Fatalf("first.txt payload = %q, want %q", first, "1111")
	}
	if _, statErr := os.Stat(filepath.Join(out, "second.txt")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("second file exists or stat failed: %v", statErr)
	}
}

func TestApplyExtractMetadataRejectsSymlinkPath(t *testing.T) {
	tmp := t.TempDir()
	target := filepath.Join(tmp, "target.txt")
	if err := os.WriteFile(target, []byte("payload"), 0o600); err != nil {
		t.Fatalf("WriteFile(target): %v", err)
	}
	originalTime := time.Date(2024, time.April, 4, 1, 2, 3, 0, time.UTC)
	if err := os.Chtimes(target, originalTime, originalTime); err != nil {
		t.Fatalf("Chtimes(target): %v", err)
	}

	link := filepath.Join(tmp, "target-link")
	symlinkOrSkip(t, target, link)

	newTime := time.Date(2024, time.April, 5, 1, 2, 3, 0, time.UTC)
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
	if !modTimeClose(info.ModTime(), originalTime) {
		t.Fatalf("target modtime = %s, want %s", info.ModTime().UTC(), originalTime.UTC())
	}
	if runtime.GOOS != "windows" {
		if gotPerm := info.Mode().Perm(); gotPerm != 0o600 {
			t.Fatalf("target perm = %#o, want %#o", gotPerm, fs.FileMode(0o600))
		}
	}
}

func TestApplyExtractMetadataRejectsSwappedPathDuringTimestampCall(t *testing.T) {
	tmp := t.TempDir()
	target := filepath.Join(tmp, "target.txt")
	if err := os.WriteFile(target, []byte("payload"), 0o600); err != nil {
		t.Fatalf("WriteFile(target): %v", err)
	}
	originalTargetTime := time.Date(2024, time.April, 6, 1, 2, 3, 0, time.UTC)
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
		symlinkOrSkip(t, outside, target)
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

	newTime := time.Date(2024, time.April, 7, 1, 2, 3, 0, time.UTC)
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

	info, statErr := os.Stat(target)
	if statErr != nil {
		t.Fatalf("Stat(target): %v", statErr)
	}
	if !modTimeClose(info.ModTime(), originalTargetTime) {
		t.Fatalf("target modtime = %s, want %s", info.ModTime().UTC(), originalTargetTime.UTC())
	}
}

func TestApplyExtractMetadataToTempFileRejectsSwappedPathDuringTimestampCall(t *testing.T) {
	tmp := t.TempDir()
	staged, err := os.CreateTemp(tmp, "staged-*")
	if err != nil {
		t.Fatalf("CreateTemp(staged): %v", err)
	}
	stagedPath := staged.Name()
	t.Cleanup(func() {
		_ = staged.Close()
		_ = os.Remove(stagedPath)
	})
	if _, err := staged.Write([]byte("payload")); err != nil {
		t.Fatalf("Write(staged): %v", err)
	}
	originalTime := time.Date(2024, time.April, 8, 1, 2, 3, 0, time.UTC)
	if err := os.Chtimes(stagedPath, originalTime, originalTime); err != nil {
		t.Fatalf("Chtimes(staged): %v", err)
	}

	outside := filepath.Join(tmp, "outside.txt")
	if err := os.WriteFile(outside, []byte("outside"), 0o600); err != nil {
		t.Fatalf("WriteFile(outside): %v", err)
	}

	entryName := "note.txt"
	prevBeforeCall := extractTestHookBeforeTempPathMetadataTimestampCall
	prevAfterCall := extractTestHookAfterTempPathMetadataTimestampCall
	var movedPath string
	extractTestHookBeforeTempPathMetadataTimestampCall = func(path, name string, stagedFile *os.File) error {
		if path != stagedPath || name != entryName {
			return nil
		}
		movedPath = path + ".moved"
		if err := os.Rename(path, movedPath); err != nil {
			if errors.Is(err, fs.ErrPermission) || errors.Is(err, os.ErrPermission) {
				t.Skipf("cannot swap temp metadata path in this environment: %v", err)
			}
			t.Fatalf("Rename(staged -> moved): %v", err)
		}
		symlinkOrSkip(t, outside, path)
		return nil
	}
	extractTestHookAfterTempPathMetadataTimestampCall = func(path, name string, stagedFile *os.File) error {
		if path != stagedPath || name != entryName {
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

	newTime := time.Date(2024, time.April, 9, 1, 2, 3, 0, time.UTC)
	err = applyExtractMetadataToTempFile(staged, stagedPath, entryName, 0o777, newTime)
	if !errors.Is(err, ErrInsecurePath) {
		t.Fatalf("applyExtractMetadataToTempFile error = %v, want %v", err, ErrInsecurePath)
	}
	var pathErr *fs.PathError
	if !errors.As(err, &pathErr) {
		t.Fatalf("applyExtractMetadataToTempFile error type = %T, want *fs.PathError", err)
	}
	if pathErr.Op != "extract" {
		t.Fatalf("path error op = %q, want %q", pathErr.Op, "extract")
	}
	if pathErr.Path != entryName {
		t.Fatalf("path error path = %q, want %q", pathErr.Path, entryName)
	}

	info, statErr := staged.Stat()
	if statErr != nil {
		t.Fatalf("staged.Stat: %v", statErr)
	}
	if !modTimeClose(info.ModTime(), originalTime) {
		t.Fatalf("staged modtime = %s, want %s", info.ModTime().UTC(), originalTime.UTC())
	}
}

func TestReaderExtractAllMetadataStagedBeforeCommitAndTempCleanedOnFailure(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "metadata-staged-before-commit.arj")
	entryName := "note.txt"
	wantMode := fs.FileMode(0o640)
	wantTime := time.Date(2024, time.July, 1, 10, 11, 12, 0, time.UTC)
	writeExtractArchive(t, archivePath, []extractEntry{
		{
			header:  buildExtractHeader(entryName, wantMode, wantTime),
			payload: []byte("new"),
		},
	})

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	out := filepath.Join(tmp, "out")
	injectedErr := errors.New("forced-before-commit")
	hookCalled := false
	prevHook := extractTestHookBeforeCommit
	extractTestHookBeforeCommit = func(name string, staged *os.File) error {
		if name != entryName {
			return nil
		}
		hookCalled = true
		info, statErr := staged.Stat()
		if statErr != nil {
			t.Fatalf("staged.Stat: %v", statErr)
		}
		if runtime.GOOS != "windows" {
			if gotPerm := info.Mode().Perm(); gotPerm != wantMode.Perm() {
				t.Fatalf("staged perm = %#o, want %#o", gotPerm, wantMode.Perm())
			}
		}
		if !modTimeClose(info.ModTime(), wantTime) {
			t.Fatalf("staged modtime = %s, want %s", info.ModTime().UTC(), wantTime.UTC())
		}
		return injectedErr
	}
	t.Cleanup(func() {
		extractTestHookBeforeCommit = prevHook
	})

	err = r.ExtractAll(out)
	if !errors.Is(err, injectedErr) {
		t.Fatalf("ExtractAll error = %v, want %v", err, injectedErr)
	}
	if !hookCalled {
		t.Fatalf("before-commit hook was not called")
	}

	if _, statErr := os.Stat(filepath.Join(out, entryName)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("output file exists or stat failed: %v", statErr)
	}
	tempMatches, globErr := filepath.Glob(filepath.Join(out, ".arj-extract-*"))
	if globErr != nil {
		t.Fatalf("Glob temp files: %v", globErr)
	}
	if len(tempMatches) != 0 {
		t.Fatalf("staged temp files remain after failure: %v", tempMatches)
	}
}

func TestReaderExtractAllPreCommitFailureKeepsExistingDestination(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "atomicity-existing-destination.arj")
	entryName := "note.txt"
	writeExtractArchive(t, archivePath, []extractEntry{
		{
			header:  buildExtractHeader(entryName, 0o640, time.Date(2024, time.July, 2, 10, 11, 12, 0, time.UTC)),
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

	injectedErr := errors.New("forced-before-commit-atomicity")
	prevHook := extractTestHookBeforeCommit
	extractTestHookBeforeCommit = func(name string, staged *os.File) error {
		if name == entryName {
			return injectedErr
		}
		return nil
	}
	t.Cleanup(func() {
		extractTestHookBeforeCommit = prevHook
	})

	err = r.ExtractAll(out)
	if !errors.Is(err, injectedErr) {
		t.Fatalf("ExtractAll error = %v, want %v", err, injectedErr)
	}

	gotPayload, readErr := os.ReadFile(target)
	if readErr != nil {
		t.Fatalf("ReadFile(target): %v", readErr)
	}
	if string(gotPayload) != string(originalPayload) {
		t.Fatalf("target payload = %q, want %q", gotPayload, originalPayload)
	}

	tempMatches, globErr := filepath.Glob(filepath.Join(out, ".arj-extract-*"))
	if globErr != nil {
		t.Fatalf("Glob temp files: %v", globErr)
	}
	if len(tempMatches) != 0 {
		t.Fatalf("staged temp files remain after failure: %v", tempMatches)
	}
}

func TestMultiReadCloserExtractAll(t *testing.T) {
	tmp := t.TempDir()
	base := filepath.Join(tmp, "split")

	writeVolumeArchive(t, base+".arj", []volumeEntry{
		{name: "joined.txt", flags: FlagVolume, payload: []byte("hello ")},
	})
	writeVolumeArchive(t, base+".a01", []volumeEntry{
		{name: "joined.txt", flags: FlagExtFile, payload: []byte("world")},
		{name: "tail/note.txt", payload: []byte("tail")},
	})

	mr, err := OpenMultiReader(base + ".arj")
	if err != nil {
		t.Fatalf("OpenMultiReader: %v", err)
	}
	defer mr.Close()

	out := filepath.Join(tmp, "out")
	if err := os.MkdirAll(filepath.Join(out, "tail"), 0o755); err != nil {
		t.Fatalf("MkdirAll(out/tail): %v", err)
	}
	if err := mr.ExtractAll(out); err != nil {
		t.Fatalf("ExtractAll: %v", err)
	}

	joined, err := os.ReadFile(filepath.Join(out, "joined.txt"))
	if err != nil {
		t.Fatalf("ReadFile(joined.txt): %v", err)
	}
	if string(joined) != "hello world" {
		t.Fatalf("joined payload = %q, want %q", joined, "hello world")
	}

	tail, err := os.ReadFile(filepath.Join(out, "tail", "note.txt"))
	if err != nil {
		t.Fatalf("ReadFile(tail/note.txt): %v", err)
	}
	if string(tail) != "tail" {
		t.Fatalf("tail payload = %q, want %q", tail, "tail")
	}
}

func assertExtractInsecurePathError(t *testing.T, err error, entryName string) {
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

func symlinkOrSkip(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		if runtime.GOOS == "windows" || errors.Is(err, fs.ErrPermission) || errors.Is(err, os.ErrPermission) {
			t.Skipf("Symlink not supported in this environment: %v", err)
		}
		t.Fatalf("Symlink(%q -> %q): %v", link, target, err)
	}
}

type extractEntry struct {
	header  FileHeader
	payload []byte
}

func buildExtractHeader(name string, mode fs.FileMode, modTime time.Time) FileHeader {
	h := FileHeader{
		Name:   name,
		Method: Store,
	}
	h.SetMode(mode)
	h.SetModTime(modTime)
	return h
}

func writeExtractArchive(t *testing.T, path string, entries []extractEntry) {
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

func makeManySmallFileEntries(n int) []extractEntry {
	entries := make([]extractEntry, 0, n)
	for i := 0; i < n; i++ {
		entries = append(entries, extractEntry{
			header: buildExtractHeader(
				fmt.Sprintf("f-%05d.txt", i),
				0o600,
				time.Date(2024, time.May, 1, 1, 2, 3, 0, time.UTC),
			),
			payload: []byte("x"),
		})
	}
	return entries
}

func modTimeClose(got, want time.Time) bool {
	diff := got.Sub(want)
	if diff < 0 {
		diff = -diff
	}
	return diff <= 2*time.Second
}
