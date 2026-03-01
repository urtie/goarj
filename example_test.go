package arj_test

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"

	arj "github.com/urtie/goarj"
)

func ExampleNewWriter() {
	var buf bytes.Buffer
	w := arj.NewWriter(&buf)

	fw, err := w.Create("hello.txt")
	if err != nil {
		panic(err)
	}
	if _, err := io.WriteString(fw, "hello"); err != nil {
		panic(err)
	}
	if err := w.Close(); err != nil {
		panic(err)
	}

	fmt.Println(buf.Len() > 0)
	// Output: true
}

func ExampleNewReader() {
	archive, err := buildExampleArchive([]exampleEntry{
		{name: "hello.txt", payload: "hello"},
	})
	if err != nil {
		panic(err)
	}

	r, err := arj.NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		panic(err)
	}

	fmt.Println(r.File[0].Name)
	// Output: hello.txt
}

func ExampleReader_Open() {
	archive, err := buildExampleArchive([]exampleEntry{
		{name: "docs/readme.txt", payload: "hello"},
	})
	if err != nil {
		panic(err)
	}

	r, err := arj.NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		panic(err)
	}

	f, err := r.Open("docs/readme.txt")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(data))
	// Output: hello
}

func ExampleReader_ExtractAll() {
	archive, err := buildExampleArchive([]exampleEntry{
		{name: "docs/readme.txt", payload: "hello"},
	})
	if err != nil {
		panic(err)
	}

	r, err := arj.NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		panic(err)
	}

	tmpDir, err := os.MkdirTemp("", "arj-extract-all-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	out := filepath.Join(tmpDir, "out")
	if err := r.ExtractAll(out); err != nil {
		panic(err)
	}

	data, err := os.ReadFile(filepath.Join(out, "docs", "readme.txt"))
	if err != nil {
		panic(err)
	}

	fmt.Println(string(data))
	// Output: hello
}

func ExampleOpenReader() {
	archive, err := buildExampleArchive([]exampleEntry{
		{name: "note.txt", payload: "from disk"},
	})
	if err != nil {
		panic(err)
	}

	tmpDir, err := os.MkdirTemp("", "arj-open-reader-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	path := filepath.Join(tmpDir, "example.arj")
	if err := os.WriteFile(path, archive, 0o600); err != nil {
		panic(err)
	}

	r, err := arj.OpenReader(path)
	if err != nil {
		panic(err)
	}
	defer r.Close()

	fmt.Println(len(r.File), r.File[0].Name)
	// Output: 1 note.txt
}

func ExampleOpenMultiReader() {
	tmpDir, err := os.MkdirTemp("", "arj-open-multi-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	base := filepath.Join(tmpDir, "split")
	if err := writeExampleArchive(base+".arj", []exampleEntry{
		{name: "joined.txt", flags: arj.FlagVolume, payload: "hello "},
	}); err != nil {
		panic(err)
	}
	if err := writeExampleArchive(base+".a01", []exampleEntry{
		{name: "joined.txt", flags: arj.FlagExtFile, payload: "world"},
	}); err != nil {
		panic(err)
	}

	r, err := arj.OpenMultiReader(base + ".arj")
	if err != nil {
		panic(err)
	}
	defer r.Close()

	f, err := r.Open("joined.txt")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(data))
	// Output: hello world
}

func ExampleSafeExtractPath() {
	out := "extract-root"

	path, err := arj.SafeExtractPath(out, "docs/readme.txt")
	if err != nil {
		panic(err)
	}
	fmt.Println(filepath.ToSlash(path))

	_, err = arj.SafeExtractPath(out, "../passwd")
	fmt.Println(errors.Is(err, arj.ErrInsecurePath))
	// Output:
	// extract-root/docs/readme.txt
	// true
}

func TestExampleBuildArchiveRoundTrip(t *testing.T) {
	entries := []exampleEntry{
		{name: "docs/readme.txt", payload: "hello"},
		{name: "bin/data.bin", payload: "binary-data-0123456789"},
		{name: "empty.txt", payload: ""},
	}

	archive, err := buildExampleArchive(entries)
	if err != nil {
		t.Fatalf("buildExampleArchive: %v", err)
	}
	r, err := arj.NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if got, want := len(r.File), len(entries); got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}

	wantByName := make(map[string]string, len(entries))
	for _, entry := range entries {
		wantByName[entry.name] = entry.payload
	}

	for i := range r.File {
		file := r.File[i]
		wantPayload, ok := wantByName[file.Name]
		if !ok {
			t.Fatalf("unexpected archive entry %q", file.Name)
		}
		if got, want := file.Method, uint16(arj.Store); got != want {
			t.Fatalf("%s method = %d, want %d", file.Name, got, want)
		}
		wantBytes := []byte(wantPayload)
		if got, want := file.UncompressedSize64, uint64(len(wantBytes)); got != want {
			t.Fatalf("%s uncompressed size = %d, want %d", file.Name, got, want)
		}
		if got, want := file.CRC32, crc32.ChecksumIEEE(wantBytes); got != want {
			t.Fatalf("%s CRC32 = 0x%08x, want 0x%08x", file.Name, got, want)
		}

		rc, err := file.Open()
		if err != nil {
			t.Fatalf("Open %s: %v", file.Name, err)
		}
		gotBytes, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		if readErr != nil {
			t.Fatalf("ReadAll %s: %v", file.Name, readErr)
		}
		if closeErr != nil {
			t.Fatalf("Close %s: %v", file.Name, closeErr)
		}
		if !bytes.Equal(gotBytes, wantBytes) {
			t.Fatalf("%s payload = %q, want %q", file.Name, gotBytes, wantBytes)
		}
		delete(wantByName, file.Name)
	}
	if len(wantByName) != 0 {
		t.Fatalf("missing entries after scan: %v", wantByName)
	}

	rc, err := r.Open("docs/readme.txt")
	if err != nil {
		t.Fatalf("Open docs/readme.txt: %v", err)
	}
	readme, readErr := io.ReadAll(rc)
	closeErr := rc.Close()
	if readErr != nil {
		t.Fatalf("ReadAll docs/readme.txt: %v", readErr)
	}
	if closeErr != nil {
		t.Fatalf("Close docs/readme.txt: %v", closeErr)
	}
	if got, want := string(readme), "hello"; got != want {
		t.Fatalf("docs/readme.txt payload = %q, want %q", got, want)
	}
}

type exampleEntry struct {
	name    string
	flags   uint8
	payload string
}

func buildExampleArchive(entries []exampleEntry) ([]byte, error) {
	var buf bytes.Buffer
	w := arj.NewWriter(&buf)

	for _, entry := range entries {
		fh := &arj.FileHeader{
			Name:   entry.name,
			Method: arj.Store,
			Flags:  entry.flags,
		}
		fw, err := w.CreateHeader(fh)
		if err != nil {
			return nil, err
		}
		if _, err := io.WriteString(fw, entry.payload); err != nil {
			return nil, err
		}
	}

	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeExampleArchive(path string, entries []exampleEntry) error {
	archive, err := buildExampleArchive(entries)
	if err != nil {
		return err
	}
	return os.WriteFile(path, archive, 0o600)
}
