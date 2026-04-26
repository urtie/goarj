package arj

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.SetComment("archive-comment"); err != nil {
		t.Fatalf("SetComment: %v", err)
	}

	fw, err := w.Create("a.txt")
	if err != nil {
		t.Fatalf("Create a.txt: %v", err)
	}
	if _, err := io.WriteString(fw, "alpha"); err != nil {
		t.Fatalf("write a.txt: %v", err)
	}

	h := &FileHeader{
		Name:     "b.txt",
		Method:   Store,
		Modified: time.Date(2024, 12, 25, 10, 30, 8, 0, time.UTC),
	}
	fw, err = w.CreateHeader(h)
	if err != nil {
		t.Fatalf("CreateHeader b.txt: %v", err)
	}
	if _, err := io.WriteString(fw, "bravo"); err != nil {
		t.Fatalf("write b.txt: %v", err)
	}

	fw, err = w.Create("dir/")
	if err != nil {
		t.Fatalf("Create dir/: %v", err)
	}
	if _, err := fw.Write(nil); err != nil {
		t.Fatalf("write dir/: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if got, want := r.Comment, "archive-comment"; got != want {
		t.Fatalf("comment = %q, want %q", got, want)
	}
	if got, want := len(r.File), 3; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	if got, want := r.File[0].Method, uint16(Method4); got != want {
		t.Fatalf("a.txt method = %d, want %d", got, want)
	}

	rc, err := r.File[0].Open()
	if err != nil {
		t.Fatalf("Open a.txt: %v", err)
	}
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll a.txt: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close a.txt: %v", err)
	}
	if got, want := string(data), "alpha"; got != want {
		t.Fatalf("a.txt payload = %q, want %q", got, want)
	}

	if got, want := r.File[1].Name, "b.txt"; got != want {
		t.Fatalf("second name = %q, want %q", got, want)
	}
	if got, want := r.File[1].ModTime(), h.Modified; !got.Equal(want) {
		t.Fatalf("b.txt modtime = %v, want %v", got, want)
	}

	if got, want := r.File[2].Name, "dir/"; got != want {
		t.Fatalf("third name = %q, want %q", got, want)
	}
	if !r.File[2].isDir() {
		t.Fatalf("dir/ isDir = false, want true")
	}
	if r.File[2].UncompressedSize64 != 0 || r.File[2].CompressedSize64 != 0 || r.File[2].CRC32 != 0 {
		t.Fatalf("dir/ sizes/crc = (%d,%d,%#08x), want (0,0,0)", r.File[2].UncompressedSize64, r.File[2].CompressedSize64, r.File[2].CRC32)
	}
	rc, err = r.File[2].Open()
	if err != nil {
		t.Fatalf("Open dir/: %v", err)
	}
	dirData, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll dir/: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close dir/: %v", err)
	}
	if len(dirData) != 0 {
		t.Fatalf("dir/ payload len = %d, want 0", len(dirData))
	}
}

func TestOpenReader(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	fw, err := w.Create("x.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := io.WriteString(fw, "x"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	tmp := filepath.Join(t.TempDir(), "test.arj")
	if err := os.WriteFile(tmp, buf.Bytes(), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	rc, err := OpenReader(tmp)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer rc.Close()

	if got, want := len(rc.File), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
}

func TestCreateUsesMethod4CompressionByDefault(t *testing.T) {
	payload := bytes.Repeat([]byte("compress-default-path-"), 512)

	var buf bytes.Buffer
	w := NewWriter(&buf)
	fw, err := w.Create("payload.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := fw.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if got, want := len(r.File), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}

	f := r.File[0]
	if got, want := f.Method, uint16(Method4); got != want {
		t.Fatalf("method = %d, want %d", got, want)
	}
	if f.CompressedSize64 >= f.UncompressedSize64 {
		t.Fatalf("compressed size = %d, want less than uncompressed %d", f.CompressedSize64, f.UncompressedSize64)
	}
}

func TestFindMainHeaderWithPrefix(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	fw, err := w.Create("pref.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := io.WriteString(fw, "pref"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	prefix := []byte("MZ\x90\x00stub-data")
	payload := append(append([]byte{}, prefix...), buf.Bytes()...)

	r, err := NewReader(bytes.NewReader(payload), int64(len(payload)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if got, want := len(r.File), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	off, err := r.File[0].DataOffset()
	if err != nil {
		t.Fatalf("DataOffset: %v", err)
	}
	if off <= int64(len(prefix)) {
		t.Fatalf("data offset = %d, want > %d", off, len(prefix))
	}
}

func TestChecksumError(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	fw, err := w.CreateHeader(&FileHeader{Name: "sum.txt", Method: Store})
	if err != nil {
		t.Fatalf("CreateHeader: %v", err)
	}
	if _, err := io.WriteString(fw, "checksum"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	orig := buf.Bytes()
	r, err := NewReader(bytes.NewReader(orig), int64(len(orig)))
	if err != nil {
		t.Fatalf("NewReader original: %v", err)
	}
	off, err := r.File[0].DataOffset()
	if err != nil {
		t.Fatalf("DataOffset: %v", err)
	}

	corrupt := append([]byte{}, orig...)
	corrupt[off] ^= 0xff

	r2, err := NewReader(bytes.NewReader(corrupt), int64(len(corrupt)))
	if err != nil {
		t.Fatalf("NewReader corrupt: %v", err)
	}
	rc, err := r2.File[0].Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_, err = io.ReadAll(rc)
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("ReadAll error = %v, want %v", err, ErrChecksum)
	}
}

func TestFileHeaderModePreservesDirectoryTypeForTypeLessUnixMode(t *testing.T) {
	h := FileHeader{
		Name:     "docs/",
		fileType: arjFileTypeDirectory,
		fileMode: uint16(fileModeToUnixMode(0o750)),
	}

	got := h.Mode()
	if got&fs.ModeDir == 0 {
		t.Fatalf("Mode() = %v, want directory mode bit", got)
	}
	if got.Perm() != 0o750 {
		t.Fatalf("Mode().Perm() = %v, want %v", got.Perm(), fs.FileMode(0o750))
	}
	if !h.FileInfo().IsDir() {
		t.Fatalf("FileInfo().IsDir() = false, want true")
	}
}

func TestUnsupportedMethod(t *testing.T) {
	var buf bytes.Buffer
	const unknownMethod uint16 = 99

	w := NewWriter(&buf)
	w.RegisterCompressor(unknownMethod, func(out io.Writer) (io.WriteCloser, error) {
		return nopWriteCloser{Writer: out}, nil
	})

	fw, err := w.CreateHeader(&FileHeader{Name: "x.bin", Method: unknownMethod})
	if err != nil {
		t.Fatalf("CreateHeader: %v", err)
	}
	if _, err := io.WriteString(fw, "x"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, err = r.File[0].Open()
	if !errors.Is(err, ErrAlgorithm) {
		t.Fatalf("Open error = %v, want %v", err, ErrAlgorithm)
	}
}

type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error { return nil }
