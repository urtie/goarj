package arj

import (
	"bytes"
	"errors"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type streamTestEntry struct {
	header  FileHeader
	payload []byte
}

func buildStreamArchive(t *testing.T, entries []streamTestEntry) []byte {
	t.Helper()

	var buf bytes.Buffer
	w := NewWriter(&buf)
	for _, entry := range entries {
		h := entry.header
		fw, err := w.CreateHeader(&h)
		if err != nil {
			t.Fatalf("CreateHeader(%q): %v", h.Name, err)
		}
		if _, err := fw.Write(entry.payload); err != nil {
			t.Fatalf("Write(%q): %v", h.Name, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	return append([]byte(nil), buf.Bytes()...)
}

func TestStreamReaderNextSequential(t *testing.T) {
	archive := buildStreamArchive(t, []streamTestEntry{
		{
			header: FileHeader{Name: "docs/", Method: Store, fileType: arjFileTypeDirectory},
		},
		{
			header:  FileHeader{Name: "docs/readme.txt", Method: Store},
			payload: []byte("hello docs"),
		},
		{
			header:  FileHeader{Name: "run.sh", Method: Store},
			payload: []byte("#!/bin/sh\necho ok\n"),
		},
	})

	sr, err := NewStreamReader(bytes.NewReader(archive))
	if err != nil {
		t.Fatalf("NewStreamReader: %v", err)
	}

	want := []struct {
		name    string
		payload string
		dir     bool
	}{
		{name: "docs/", payload: "", dir: true},
		{name: "docs/readme.txt", payload: "hello docs"},
		{name: "run.sh", payload: "#!/bin/sh\necho ok\n"},
	}
	for i, expected := range want {
		h, rc, err := sr.Next()
		if err != nil {
			t.Fatalf("Next(%d): %v", i, err)
		}
		if h == nil || rc == nil {
			t.Fatalf("Next(%d) returned nil header/reader", i)
		}
		if h.Name != expected.name {
			t.Fatalf("entry name(%d) = %q, want %q", i, h.Name, expected.name)
		}
		if gotDir := h.isDir(); gotDir != expected.dir {
			t.Fatalf("entry isDir(%d) = %v, want %v", i, gotDir, expected.dir)
		}

		got, err := io.ReadAll(rc)
		if err != nil {
			t.Fatalf("ReadAll(%q): %v", h.Name, err)
		}
		if err := rc.Close(); err != nil {
			t.Fatalf("Close(%q): %v", h.Name, err)
		}
		if string(got) != expected.payload {
			t.Fatalf("payload(%q) = %q, want %q", h.Name, got, expected.payload)
		}
	}

	if _, _, err := sr.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("final Next error = %v, want %v", err, io.EOF)
	}
}

func TestStreamReaderNextClosesPreviousUnreadEntry(t *testing.T) {
	archive := buildStreamArchive(t, []streamTestEntry{
		{
			header:  FileHeader{Name: "first.bin", Method: Store},
			payload: []byte("abcdefgh"),
		},
		{
			header:  FileHeader{Name: "second.bin", Method: Store},
			payload: []byte("wxyz"),
		},
	})

	sr, err := NewStreamReader(bytes.NewReader(archive))
	if err != nil {
		t.Fatalf("NewStreamReader: %v", err)
	}

	_, firstRC, err := sr.Next()
	if err != nil {
		t.Fatalf("Next(first): %v", err)
	}
	buf := make([]byte, 1)
	if _, err := firstRC.Read(buf); err != nil {
		t.Fatalf("Read(first): %v", err)
	}

	h, secondRC, err := sr.Next()
	if err != nil {
		t.Fatalf("Next(second): %v", err)
	}
	if h.Name != "second.bin" {
		t.Fatalf("second header name = %q, want %q", h.Name, "second.bin")
	}

	if n, err := firstRC.Read(buf); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("first reader after Next = (%d, %v), want (0, %v)", n, err, io.EOF)
	}

	got, err := io.ReadAll(secondRC)
	if err != nil {
		t.Fatalf("ReadAll(second): %v", err)
	}
	if err := secondRC.Close(); err != nil {
		t.Fatalf("Close(second): %v", err)
	}
	if string(got) != "wxyz" {
		t.Fatalf("second payload = %q, want %q", got, "wxyz")
	}
}

func TestNewStreamReaderSkipsLeadingPrefix(t *testing.T) {
	archive := buildStreamArchive(t, []streamTestEntry{
		{
			header:  FileHeader{Name: "file.txt", Method: Store},
			payload: []byte("ok"),
		},
	})
	prefix := []byte("sfx-prefix-data")
	stream := append(append([]byte(nil), prefix...), archive...)

	sr, err := NewStreamReader(bytes.NewReader(stream))
	if err != nil {
		t.Fatalf("NewStreamReader: %v", err)
	}
	if got, want := sr.BaseOffset(), int64(len(prefix)); got != want {
		t.Fatalf("BaseOffset = %d, want %d", got, want)
	}

	h, rc, err := sr.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if h.Name != "file.txt" {
		t.Fatalf("entry name = %q, want %q", h.Name, "file.txt")
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "ok" {
		t.Fatalf("payload = %q, want %q", got, "ok")
	}
}

func TestStreamReaderWithOptionsMaxEntries(t *testing.T) {
	archive := buildStreamArchive(t, []streamTestEntry{
		{
			header:  FileHeader{Name: "one.txt", Method: Store},
			payload: []byte("1"),
		},
		{
			header:  FileHeader{Name: "two.txt", Method: Store},
			payload: []byte("2"),
		},
	})

	sr, err := NewStreamReaderWithOptions(
		bytes.NewReader(archive),
		StreamReaderOptions{ParserLimits: ParserLimits{MaxEntries: 1}},
	)
	if err != nil {
		t.Fatalf("NewStreamReaderWithOptions: %v", err)
	}

	if _, _, err := sr.Next(); err != nil {
		t.Fatalf("Next(first): %v", err)
	}
	_, _, err = sr.Next()
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("Next(second) error = %v, want %v", err, ErrFormat)
	}
	if !strings.Contains(err.Error(), "max entries exceeded") {
		t.Fatalf("Next(second) error = %v, want max entries exceeded", err)
	}
}

func TestExtractAllStream(t *testing.T) {
	archive := buildStreamArchive(t, []streamTestEntry{
		{
			header: FileHeader{Name: "docs/", Method: Store, fileType: arjFileTypeDirectory},
		},
		{
			header: FileHeader{
				Name:   "docs/readme.txt",
				Method: Store,
			},
			payload: []byte("stream extract"),
		},
	})
	out := filepath.Join(t.TempDir(), "out")

	if err := ExtractAllStream(bytes.NewReader(archive), out); err != nil {
		t.Fatalf("ExtractAllStream: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(out, "docs", "readme.txt"))
	if err != nil {
		t.Fatalf("ReadFile(readme): %v", err)
	}
	if string(got) != "stream extract" {
		t.Fatalf("readme payload = %q, want %q", got, "stream extract")
	}
}

func TestExtractAllStreamPreOpenQuotaCheckBeforeDecompressorPath(t *testing.T) {
	payload := []byte("pre-open-quota")
	hdr := &FileHeader{
		Name:               "oversized.bin",
		Method:             250, // deliberately unregistered decompressor
		CRC32:              crc32.ChecksumIEEE(payload),
		CompressedSize64:   uint64(len(payload)),
		UncompressedSize64: uint64(len(payload)),
	}
	archive := buildSingleRawArchive(t, hdr, payload)

	out := filepath.Join(t.TempDir(), "out")
	err := ExtractAllStreamWithOptions(
		bytes.NewReader(archive),
		out,
		ExtractOptions{MaxFileBytes: int64(len(payload) - 1)},
	)
	if err == nil {
		t.Fatalf("ExtractAllStreamWithOptions error = nil, want quota error")
	}
	if !strings.Contains(err.Error(), "max file bytes exceeded") {
		t.Fatalf("ExtractAllStreamWithOptions error = %v, want max file bytes exceeded", err)
	}
	if errors.Is(err, ErrAlgorithm) {
		t.Fatalf("ExtractAllStreamWithOptions error unexpectedly matched %v", ErrAlgorithm)
	}
	if _, statErr := os.Stat(filepath.Join(out, "oversized.bin")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("oversized output exists or stat failed: %v", statErr)
	}
}

func TestExtractAllStreamRejectsInsecurePath(t *testing.T) {
	archive := buildStreamArchive(t, []streamTestEntry{
		{
			header:  FileHeader{Name: "../escape.txt", Method: Store},
			payload: []byte("escape"),
		},
	})

	err := ExtractAllStream(bytes.NewReader(archive), filepath.Join(t.TempDir(), "out"))
	if !errors.Is(err, ErrInsecurePath) {
		t.Fatalf("ExtractAllStream error = %v, want %v", err, ErrInsecurePath)
	}

	var pathErr *fs.PathError
	if !errors.As(err, &pathErr) {
		t.Fatalf("ExtractAllStream error type = %T, want *fs.PathError", err)
	}
	if pathErr.Op != "extract" {
		t.Fatalf("path error op = %q, want %q", pathErr.Op, "extract")
	}
	if pathErr.Path != "../escape.txt" {
		t.Fatalf("path error path = %q, want %q", pathErr.Path, "../escape.txt")
	}
}
