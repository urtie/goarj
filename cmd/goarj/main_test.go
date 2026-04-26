package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	goarj "github.com/urtie/goarj"
)

func TestRunArchiveExtractDirectoryRoundTrip(t *testing.T) {
	tmp := t.TempDir()

	sourceDir := filepath.Join(tmp, "src")
	if err := os.MkdirAll(filepath.Join(sourceDir, "nested", "deep"), 0o755); err != nil {
		t.Fatalf("MkdirAll source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "root.txt"), []byte("root payload"), 0o644); err != nil {
		t.Fatalf("WriteFile root.txt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "nested", "deep", "leaf.txt"), []byte("leaf payload"), 0o600); err != nil {
		t.Fatalf("WriteFile leaf.txt: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(sourceDir, "empty"), 0o755); err != nil {
		t.Fatalf("MkdirAll empty dir: %v", err)
	}

	archivePath := filepath.Join(tmp, "archive.arj")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := run([]string{"archive", archivePath, sourceDir}, &stdout, &stderr); err != nil {
		t.Fatalf("run archive: %v", err)
	}

	outDir := filepath.Join(tmp, "out")
	if err := run([]string{"extract", archivePath, outDir}, &stdout, &stderr); err != nil {
		t.Fatalf("run extract: %v", err)
	}

	checkFileContent(t, filepath.Join(outDir, "root.txt"), "root payload")
	checkFileContent(t, filepath.Join(outDir, "nested", "deep", "leaf.txt"), "leaf payload")

	info, err := os.Stat(filepath.Join(outDir, "empty"))
	if err != nil {
		t.Fatalf("Stat empty dir: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("expected %q to be a directory", filepath.Join(outDir, "empty"))
	}
}

func TestRunArchiveExtractSingleFileRoundTrip(t *testing.T) {
	tmp := t.TempDir()

	sourcePath := filepath.Join(tmp, "hello.txt")
	if err := os.WriteFile(sourcePath, []byte("hello file"), 0o644); err != nil {
		t.Fatalf("WriteFile source: %v", err)
	}

	archivePath := filepath.Join(tmp, "single.arj")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := run([]string{"archive", archivePath, sourcePath}, &stdout, &stderr); err != nil {
		t.Fatalf("run archive: %v", err)
	}
	r, err := goarj.OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader archive: %v", err)
	}
	if got, want := len(r.File), 1; got != want {
		t.Fatalf("archive file count = %d, want %d", got, want)
	}
	if got, want := r.File[0].Method, uint16(goarj.Method4); got != want {
		t.Fatalf("archive method = %d, want %d", got, want)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close reader: %v", err)
	}

	outDir := filepath.Join(tmp, "out")
	if err := run([]string{"extract", archivePath, outDir}, &stdout, &stderr); err != nil {
		t.Fatalf("run extract: %v", err)
	}

	checkFileContent(t, filepath.Join(outDir, "hello.txt"), "hello file")
}

func TestAddSingleFileRejectsOversizedFileBeforeOpen(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "missing-huge.bin")
	sourceInfo := (&goarj.FileHeader{
		Name:               "missing-huge.bin",
		UncompressedSize64: uint64(1) << 32,
	}).FileInfo()

	var buf bytes.Buffer
	writer := goarj.NewWriter(&buf)
	err := addSingleFile(writer, sourcePath, sourceInfo)
	if !errors.Is(err, goarj.ErrFileTooLarge) {
		t.Fatalf("addSingleFile error = %v, want wrapped %v", err, goarj.ErrFileTooLarge)
	}
	if strings.Contains(err.Error(), "open source") {
		t.Fatalf("addSingleFile opened oversized source: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
}

func TestRunExtractIgnoresSiblingVolumeLikeFiles(t *testing.T) {
	tmp := t.TempDir()

	sourcePath := filepath.Join(tmp, "testfile")
	if err := os.WriteFile(sourcePath, []byte("payload"), 0o644); err != nil {
		t.Fatalf("WriteFile source: %v", err)
	}

	archivePath := filepath.Join(tmp, "test.arj")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := run([]string{"archive", archivePath, sourcePath}, &stdout, &stderr); err != nil {
		t.Fatalf("run archive: %v", err)
	}

	// Sibling files like test.a01 should not affect extraction of test.arj.
	if err := os.WriteFile(filepath.Join(tmp, "test.a01"), []byte("not-an-arj"), 0o644); err != nil {
		t.Fatalf("WriteFile sibling .a01: %v", err)
	}

	outDir := filepath.Join(tmp, "out")
	if err := run([]string{"extract", archivePath, outDir}, &stdout, &stderr); err != nil {
		t.Fatalf("run extract: %v", err)
	}

	checkFileContent(t, filepath.Join(outDir, "testfile"), "payload")
}

func TestRunArchiveRejectsArchiveInsideSourceDirectory(t *testing.T) {
	tmp := t.TempDir()

	sourceDir := filepath.Join(tmp, "src")
	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatalf("MkdirAll source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "file.txt"), []byte("payload"), 0o644); err != nil {
		t.Fatalf("WriteFile source: %v", err)
	}

	archivePath := filepath.Join(sourceDir, "archive.arj")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := run([]string{"archive", archivePath, sourceDir}, &stdout, &stderr)
	if err == nil {
		t.Fatalf("run archive error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "outside source directory") {
		t.Fatalf("run archive error = %q, want outside-source-directory message", err)
	}
	if _, statErr := os.Stat(archivePath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("archive path stat error = %v, want %v", statErr, os.ErrNotExist)
	}
}

func TestRunArchiveRejectsArchivePathEqualToSourceFile(t *testing.T) {
	tmp := t.TempDir()

	sourcePath := filepath.Join(tmp, "data.txt")
	const original = "ORIGINAL"
	if err := os.WriteFile(sourcePath, []byte(original), 0o644); err != nil {
		t.Fatalf("WriteFile source: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := run([]string{"archive", sourcePath, sourcePath}, &stdout, &stderr)
	if err == nil {
		t.Fatalf("run archive error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "must differ from source file") &&
		!strings.Contains(err.Error(), "resolves to source file") {
		t.Fatalf("run archive error = %q, want source-file-overlap message", err)
	}
	checkFileContent(t, sourcePath, original)
}

func TestRunUsageErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "missing command", args: nil},
		{name: "unknown command", args: []string{"unknown"}},
		{name: "archive missing args", args: []string{"archive"}},
		{name: "extract too many args", args: []string{"extract", "a.arj", "out", "extra"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			err := run(tt.args, &stdout, &stderr)
			if err == nil {
				t.Fatalf("run(%v) error = nil, want non-nil", tt.args)
			}

			usageText := stdout.String() + stderr.String()
			if strings.Contains(usageText, "Usage:") {
				return
			}
			if !strings.Contains(strings.ToLower(err.Error()), "usage:") {
				t.Fatalf("usage output missing; err=%q stdout=%q stderr=%q", err, stdout.String(), stderr.String())
			}
		})
	}
}

func checkFileContent(t *testing.T, path, want string) {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile %s: %v", path, err)
	}
	if got := string(data); got != want {
		t.Fatalf("ReadFile %s = %q, want %q", path, got, want)
	}
}
