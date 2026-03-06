//go:build windows

package arj

import (
	"errors"
	"io/fs"
	"testing"
)

func TestSafeExtractPathRejectsWindowsDriveRelativeUNCADSAndTrimmedComponents(t *testing.T) {
	root := `C:\extract-root`
	tests := []string{
		`C:relative.txt`,
		`C:/Windows/System32/drivers/etc/hosts`,
		`//server/share/file.txt`,
		`\\server\share\file.txt`,
		`file.txt:Zone.Identifier`,
		`docs/readme.txt:stream`,
		`file.txt.`,
		`file.txt `,
		`docs./readme.txt`,
		`docs /readme.txt`,
	}

	for _, name := range tests {
		_, err := SafeExtractPath(root, name)
		assertWindowsInsecureExtractPathError(t, name, err)
	}
}

func TestExtractPathAnchorWindowsDriveRoot(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{input: `C:\`, want: `C:\`},
		{input: `C:\work\out`, want: `C:\`},
		{input: `D:\tmp\child`, want: `D:\`},
	}

	for _, tc := range tests {
		got, err := extractPathAnchor(tc.input)
		if err != nil {
			t.Fatalf("extractPathAnchor(%q): %v", tc.input, err)
		}
		if got != tc.want {
			t.Fatalf("extractPathAnchor(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestExtractPathAnchorWindowsRejectsInvalidInput(t *testing.T) {
	tests := []string{
		``,
		`.`,
		`relative\path`,
		`relative/path`,
		`C:`,
		`C:relative`,
	}

	for _, input := range tests {
		if _, err := extractPathAnchor(input); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("extractPathAnchor(%q) error = %v, want %v", input, err, fs.ErrInvalid)
		}
	}
}

func assertWindowsInsecureExtractPathError(t *testing.T, name string, err error) {
	t.Helper()
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
