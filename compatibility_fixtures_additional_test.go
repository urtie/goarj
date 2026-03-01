package arj

import (
	"bytes"
	"io"
	"path/filepath"
	"testing"
)

type compatFixtureEntry struct {
	name    string
	method  uint16
	comment string
	payload []byte
}

func TestAdditionalCompatibilityFixtures(t *testing.T) {
	t.Parallel()

	longName := "this-is-a-very-long-filename-for-arj-compatibility-fixture-testing-abcdefghijklmnopqrstuvwxyz-0123456789.txt"

	fixtures := []struct {
		name           string
		archiveComment string
		files          []compatFixtureEntry
	}{
		{
			name:           "compat_multi_file.arj",
			archiveComment: "",
			files: []compatFixtureEntry{
				{
					name:    "alpha.txt",
					method:  Method1,
					comment: "",
					payload: []byte("alpha payload line 1\nalpha payload line 2\n"),
				},
				{
					name:    "beta.bin",
					method:  Store,
					comment: "",
					payload: []byte("BETA-00112233445566778899\n"),
				},
				{
					name:    "gamma.dat",
					method:  Store,
					comment: "",
					payload: []byte("gamma fixture bytes with punctuation !@#$%^&*()\n"),
				},
			},
		},
		{
			name:           "compat_nested_paths.arj",
			archiveComment: "",
			files: []compatFixtureEntry{
				{
					name:    "root.txt",
					method:  Store,
					comment: "",
					payload: []byte("root-level payload\n"),
				},
				{
					name:    "dir1/child.txt",
					method:  Store,
					comment: "",
					payload: []byte("child payload in dir1\n"),
				},
				{
					name:    "dir1/dir2/deep.bin",
					method:  Store,
					comment: "",
					payload: []byte("deep payload dir1/dir2\n"),
				},
			},
		},
		{
			name:           "compat_longname_comment.arj",
			archiveComment: "archive comment for long filename fixture",
			files: []compatFixtureEntry{
				{
					name:    longName,
					method:  Method1,
					comment: "file comment for long filename fixture",
					payload: []byte("long filename payload for compatibility fixture\n"),
				},
			},
		},
		{
			name:           "compat_mixed_methods.arj",
			archiveComment: "",
			files: []compatFixtureEntry{
				{
					name:    "method1.txt",
					method:  Method1,
					comment: "",
					payload: []byte("method1 payload payload payload payload payload\n"),
				},
				{
					name:    "method2.txt",
					method:  Method2,
					comment: "",
					payload: []byte("method2 payload payload payload payload payload\n"),
				},
				{
					name:    "method3.txt",
					method:  Method3,
					comment: "",
					payload: []byte("method3 payload payload payload payload payload\n"),
				},
				{
					name:    "method4.txt",
					method:  Method4,
					comment: "",
					payload: []byte("method4 payload payload payload payload payload\n"),
				},
			},
		},
	}

	for _, tc := range fixtures {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rc, err := OpenReader(filepath.Join("testdata", tc.name))
			if err != nil {
				t.Fatalf("OpenReader: %v", err)
			}
			defer rc.Close()

			if got, want := rc.Comment, tc.archiveComment; got != want {
				t.Fatalf("archive comment = %q, want %q", got, want)
			}
			if got, want := len(rc.File), len(tc.files); got != want {
				t.Fatalf("file count = %d, want %d", got, want)
			}

			wantByName := make(map[string]compatFixtureEntry, len(tc.files))
			for _, wantFile := range tc.files {
				if _, exists := wantByName[wantFile.name]; exists {
					t.Fatalf("test fixture %s has duplicate expected entry %q", tc.name, wantFile.name)
				}
				wantByName[wantFile.name] = wantFile
			}

			seen := make(map[string]bool, len(tc.files))
			for i, f := range rc.File {
				wantFile, ok := wantByName[f.Name]
				if !ok {
					t.Fatalf("unexpected archive entry[%d] %q", i, f.Name)
				}
				if seen[f.Name] {
					t.Fatalf("duplicate archive entry[%d] %q", i, f.Name)
				}
				seen[f.Name] = true

				if got, want := f.Method, wantFile.method; got != want {
					t.Fatalf("entry[%d] method = %d, want %d", i, got, want)
				}
				if got, want := f.Comment, wantFile.comment; got != want {
					t.Fatalf("entry[%d] comment = %q, want %q", i, got, want)
				}

				frc, err := f.Open()
				if err != nil {
					t.Fatalf("entry[%d] open: %v", i, err)
				}
				gotPayload, err := io.ReadAll(frc)
				closeErr := frc.Close()
				if err != nil {
					t.Fatalf("entry[%d] read: %v", i, err)
				}
				if closeErr != nil {
					t.Fatalf("entry[%d] close: %v", i, closeErr)
				}
				if !bytes.Equal(gotPayload, wantFile.payload) {
					t.Fatalf("entry[%d] payload mismatch for %q", i, f.Name)
				}
			}

			for name := range wantByName {
				if !seen[name] {
					t.Fatalf("missing expected entry %q", name)
				}
			}
		})
	}
}
