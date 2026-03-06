package arj

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestSafeExtractPath(t *testing.T) {
	root := filepath.Join("extract-root")
	got, err := SafeExtractPath(root, "docs/readme.txt")
	if err != nil {
		t.Fatalf("SafeExtractPath: %v", err)
	}
	want := filepath.Join(root, "docs", "readme.txt")
	if got != want {
		t.Fatalf("SafeExtractPath path = %q, want %q", got, want)
	}
}

func TestSafeExtractPathDirectoryEntry(t *testing.T) {
	root := filepath.Join("extract-root")
	got, err := SafeExtractPath(root, "docs/")
	if err != nil {
		t.Fatalf("SafeExtractPath: %v", err)
	}
	want := filepath.Join(root, "docs")
	if got != want {
		t.Fatalf("SafeExtractPath path = %q, want %q", got, want)
	}
}

func TestSafeExtractPathRejectsInsecureNames(t *testing.T) {
	root := filepath.Join("extract-root")
	tests := []string{
		"",
		".",
		"..",
		"../secret.txt",
		"nested/../../secret.txt",
		"/etc/passwd",
		"nested//file.txt",
		"nested/./file.txt",
		`nested\file.txt`,
	}
	if runtime.GOOS == "windows" {
		tests = append(tests, "C:/Windows/System32/drivers/etc/hosts")
	}

	for _, name := range tests {
		_, err := SafeExtractPath(root, name)
		if !errors.Is(err, ErrInsecurePath) {
			t.Fatalf("SafeExtractPath(%q) error = %v, want %v", name, err, ErrInsecurePath)
		}
		var pathErr *fs.PathError
		if !errors.As(err, &pathErr) {
			t.Fatalf("SafeExtractPath(%q) error type = %T, want *fs.PathError", name, err)
		}
		if pathErr.Op != "extract" {
			t.Fatalf("SafeExtractPath(%q) op = %q, want %q", name, pathErr.Op, "extract")
		}
		if pathErr.Path != name {
			t.Fatalf("SafeExtractPath(%q) path = %q, want %q", name, pathErr.Path, name)
		}
	}
}

func TestHasWindowsTrimmedPathComponent(t *testing.T) {
	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "clean", path: "docs/readme.txt", want: false},
		{name: "trailing dot file", path: "readme.txt.", want: true},
		{name: "trailing space file", path: "readme.txt ", want: true},
		{name: "trailing dot dir", path: "docs./readme.txt", want: true},
		{name: "trailing space dir", path: "docs /readme.txt", want: true},
	}

	for _, tc := range tests {
		if got := hasWindowsTrimmedPathComponent(tc.path); got != tc.want {
			t.Fatalf("hasWindowsTrimmedPathComponent(%q) = %v, want %v", tc.path, got, tc.want)
		}
	}
}

func TestEnsureNoSymlinkComponentsRejectsSymlinkRoot(t *testing.T) {
	tmp := t.TempDir()
	rootTarget := filepath.Join(tmp, "root-target")
	if err := os.MkdirAll(rootTarget, 0o755); err != nil {
		t.Fatalf("MkdirAll(root-target): %v", err)
	}
	rootLink := filepath.Join(tmp, "root-link")
	symlinkOrSkip(t, rootTarget, rootLink)

	target := filepath.Join(rootLink, "file.txt")
	err := ensureNoSymlinkComponents(rootLink, target, "file.txt", false)
	if !errors.Is(err, ErrInsecurePath) {
		t.Fatalf("ensureNoSymlinkComponents error = %v, want %v", err, ErrInsecurePath)
	}

	var pathErr *fs.PathError
	if !errors.As(err, &pathErr) {
		t.Fatalf("ensureNoSymlinkComponents error type = %T, want *fs.PathError", err)
	}
	if pathErr.Op != "extract" {
		t.Fatalf("path error op = %q, want %q", pathErr.Op, "extract")
	}
	if pathErr.Path != "file.txt" {
		t.Fatalf("path error path = %q, want %q", pathErr.Path, "file.txt")
	}
}
