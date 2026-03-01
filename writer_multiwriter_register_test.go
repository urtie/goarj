package arj

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestWriterCreateHeaderNilReturnsError(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.CreateHeader(nil); !errors.Is(err, errNilFileHeader) {
		t.Fatalf("CreateHeader(nil) error = %v, want %v", err, errNilFileHeader)
	}
}

func TestWriterCreateRawNilReturnsError(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.CreateRaw(nil); !errors.Is(err, errNilFileHeader) {
		t.Fatalf("CreateRaw(nil) error = %v, want %v", err, errNilFileHeader)
	}
}

func TestWriterCopyNilReturnsError(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.Copy(nil); !errors.Is(err, errNilFileHeader) {
		t.Fatalf("Copy(nil) error = %v, want %v", err, errNilFileHeader)
	}
}

func TestMultiVolumeWriterCreateHeaderNilReturnsError(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "nil-header.arj")
	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 1024})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}
	if _, err := mw.CreateHeader(nil); !errors.Is(err, errNilFileHeader) {
		t.Fatalf("CreateHeader(nil) error = %v, want %v", err, errNilFileHeader)
	}
	if parts := mw.Parts(); len(parts) != 0 {
		t.Fatalf("parts len = %d, want 0", len(parts))
	}
}

func TestWriterWriteFailsEarlyAtSizeLimit(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	iw, err := w.Create("limit.bin")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	fw := iw.(*fileWriter)
	fw.plainN = maxARJFileSize - 2

	n, err := fw.Write([]byte("abcd"))
	if got, want := n, 2; got != want {
		t.Fatalf("Write bytes = %d, want %d", got, want)
	}
	if !errors.Is(err, errFileTooLarge) {
		t.Fatalf("Write error = %v, want %v", err, errFileTooLarge)
	}
	if got, want := fw.plainN, maxARJFileSize; got != want {
		t.Fatalf("plainN = %d, want %d", got, want)
	}

	n, err = fw.Write([]byte("x"))
	if got, want := n, 0; got != want {
		t.Fatalf("second Write bytes = %d, want %d", got, want)
	}
	if !errors.Is(err, errFileTooLarge) {
		t.Fatalf("second Write error = %v, want %v", err, errFileTooLarge)
	}
	if err := fw.Close(); !errors.Is(err, errFileTooLarge) {
		t.Fatalf("Close error = %v, want %v", err, errFileTooLarge)
	}
}

func TestMultiVolumeWriterWriteFailsEarlyAtSizeLimit(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "limit.arj")
	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 1024})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}

	iw, err := mw.Create("limit.bin")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	fw := iw.(*multiVolumeCompressedFileWriter)
	fw.plainN = maxARJFileSize - 2

	n, err := fw.Write([]byte("abcd"))
	if got, want := n, 2; got != want {
		t.Fatalf("Write bytes = %d, want %d", got, want)
	}
	if !errors.Is(err, errFileTooLarge) {
		t.Fatalf("Write error = %v, want %v", err, errFileTooLarge)
	}
	if got, want := fw.plainN, maxARJFileSize; got != want {
		t.Fatalf("plainN = %d, want %d", got, want)
	}

	n, err = fw.Write([]byte("x"))
	if got, want := n, 0; got != want {
		t.Fatalf("second Write bytes = %d, want %d", got, want)
	}
	if !errors.Is(err, errFileTooLarge) {
		t.Fatalf("second Write error = %v, want %v", err, errFileTooLarge)
	}
	if err := fw.Close(); !errors.Is(err, errFileTooLarge) {
		t.Fatalf("Close error = %v, want %v", err, errFileTooLarge)
	}
}

func TestFileWriterCloseReturnsFirstWriteError(t *testing.T) {
	const method uint16 = 250
	wantErr := errors.New("test: file writer write failure")

	var archive bytes.Buffer
	w := NewWriter(&archive)
	w.RegisterCompressor(method, func(io.Writer) (io.WriteCloser, error) {
		return &failOnWriteCloser{err: wantErr}, nil
	})

	iw, err := w.CreateHeader(&FileHeader{Name: "broken.bin", Method: method})
	if err != nil {
		t.Fatalf("CreateHeader: %v", err)
	}
	fw := iw.(*fileWriter)

	if n, err := fw.Write([]byte("payload")); n != 0 || !errors.Is(err, wantErr) {
		t.Fatalf("Write = (%d, %v), want (0, %v)", n, err, wantErr)
	}
	if err := fw.Close(); !errors.Is(err, wantErr) {
		t.Fatalf("Close error = %v, want %v", err, wantErr)
	}
	if _, err := w.Create("after-close-failure.bin"); !errors.Is(err, wantErr) {
		t.Fatalf("Create after close failure error = %v, want %v", err, wantErr)
	}
}

func TestMultiVolumeFileWriterCloseReturnsFirstWriteError(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "multi-write-error.arj")
	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 8 * 1024})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}

	iw, err := mw.Create("broken.bin")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	fw := iw.(*multiVolumeCompressedFileWriter)
	wantErr := errors.New("test: multi volume write failure")
	fw.latchWriteErr(wantErr)
	if err := fw.Close(); !errors.Is(err, wantErr) {
		t.Fatalf("Close error = %v, want wrapped %v", err, wantErr)
	}
}

func TestMultiVolumeRawFileWriterCloseReturnsFirstWriteError(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "multi-raw-write-error.arj")
	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 8 * 1024})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}

	iw, err := mw.CreateRaw(&FileHeader{
		Name:               "raw.bin",
		Method:             Store,
		CompressedSize64:   1,
		UncompressedSize64: 1,
	})
	if err != nil {
		t.Fatalf("CreateRaw: %v", err)
	}
	fw := iw.(*multiVolumeRawFileWriter)

	tmp, err := os.CreateTemp(t.TempDir(), "closed-entry-buffer-*")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	tmpPath := tmp.Name()
	if err := tmp.Close(); err != nil {
		t.Fatalf("Close temp file: %v", err)
	}
	fw.raw.file = tmp
	fw.raw.filePath = tmpPath
	fw.raw.removePath = tmpPath

	_, writeErr := fw.Write([]byte("x"))
	if writeErr == nil {
		t.Fatalf("Write error = nil, want non-nil")
	}
	if err := fw.Close(); !errors.Is(err, writeErr) {
		t.Fatalf("Close error = %v, want wrapped %v", err, writeErr)
	}
}

func TestMultiVolumeWriterLatchesSinkWriteFailure(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "multi-sink-write-latch.arj")
	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 64 << 10})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}

	iw, err := mw.Create("broken.bin")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := iw.Write(bytes.Repeat([]byte{'x'}, 16<<10)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if err := mw.ensureCurrentVolume(); err != nil {
		t.Fatalf("ensureCurrentVolume: %v", err)
	}
	if err := mw.currentFile.Close(); err != nil {
		t.Fatalf("close current file: %v", err)
	}

	firstErr := iw.(*multiVolumeCompressedFileWriter).Close()
	if firstErr == nil {
		firstErr = mw.Flush()
	}
	if firstErr == nil {
		t.Fatalf("sink failure did not surface from entry close/flush")
	}
	if mw.failed == nil {
		t.Fatalf("mw.failed = nil, want latched failure")
	}
	if !errors.Is(firstErr, mw.failed) {
		t.Fatalf("first error = %v, want wrapped %v", firstErr, mw.failed)
	}

	if err := mw.prepare(); !errors.Is(err, mw.failed) {
		t.Fatalf("prepare error = %v, want wrapped %v", err, mw.failed)
	}
	if err := mw.Flush(); !errors.Is(err, mw.failed) {
		t.Fatalf("Flush error = %v, want wrapped %v", err, mw.failed)
	}
	if _, err := mw.Create("after-failure.bin"); !errors.Is(err, mw.failed) {
		t.Fatalf("Create error = %v, want wrapped %v", err, mw.failed)
	}
	if _, err := mw.CreateRaw(&FileHeader{
		Name:               "after-failure-raw.bin",
		Method:             Store,
		CompressedSize64:   0,
		UncompressedSize64: 0,
	}); !errors.Is(err, mw.failed) {
		t.Fatalf("CreateRaw error = %v, want wrapped %v", err, mw.failed)
	}
	if err := mw.Close(); !errors.Is(err, mw.failed) {
		t.Fatalf("Close error = %v, want wrapped %v", err, mw.failed)
	}
}

func TestMultiVolumeWriterLatchesFinalizeFailure(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "multi-finalize-latch.arj")
	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 64 << 10})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}

	iw, err := mw.Create("ok.bin")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := iw.Write([]byte("payload")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := iw.(*multiVolumeCompressedFileWriter).Close(); err != nil {
		t.Fatalf("Close entry: %v", err)
	}
	if mw.currentFile == nil {
		t.Fatalf("current file = nil, want active volume")
	}
	if err := mw.currentFile.Close(); err != nil {
		t.Fatalf("close current file: %v", err)
	}

	firstErr := mw.Close()
	if firstErr == nil {
		t.Fatalf("Close error = nil, want sink finalize failure")
	}
	if mw.failed == nil {
		t.Fatalf("mw.failed = nil, want latched failure")
	}
	if !errors.Is(firstErr, mw.failed) {
		t.Fatalf("first close error = %v, want wrapped %v", firstErr, mw.failed)
	}

	if err := mw.prepare(); !errors.Is(err, mw.failed) {
		t.Fatalf("prepare error = %v, want wrapped %v", err, mw.failed)
	}
	if err := mw.Flush(); !errors.Is(err, mw.failed) {
		t.Fatalf("Flush error = %v, want wrapped %v", err, mw.failed)
	}
	if _, err := mw.Create("after-finalize-failure.bin"); !errors.Is(err, mw.failed) {
		t.Fatalf("Create error = %v, want wrapped %v", err, mw.failed)
	}
	if _, err := mw.CreateRaw(&FileHeader{
		Name:               "after-finalize-failure-raw.bin",
		Method:             Store,
		CompressedSize64:   0,
		UncompressedSize64: 0,
	}); !errors.Is(err, mw.failed) {
		t.Fatalf("CreateRaw error = %v, want wrapped %v", err, mw.failed)
	}
	if err := mw.Close(); !errors.Is(err, mw.failed) {
		t.Fatalf("second Close error = %v, want wrapped %v", err, mw.failed)
	}
}

func TestWriterDefaultEntryBufferLimitGuard(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	iw, err := w.CreateHeader(&FileHeader{Name: "default-limit.bin", Method: Store})
	if err != nil {
		t.Fatalf("CreateHeader: %v", err)
	}
	fw := iw.(*fileWriter)
	if got, want := fw.entryBufferLimit, DefaultMaxEntryBufferSize; got != want {
		t.Fatalf("entryBufferLimit = %d, want %d", got, want)
	}

	// Simulate a fully buffered entry so guard behavior remains deterministic
	// even if default limits change.
	fw.comp.size = fw.entryBufferLimit
	n, err := fw.Write([]byte("x"))
	if got, want := n, 0; got != want {
		t.Fatalf("Write bytes = %d, want %d", got, want)
	}
	if !errors.Is(err, ErrBufferLimitExceeded) {
		t.Fatalf("Write error = %v, want %v", err, ErrBufferLimitExceeded)
	}
	var limitErr *BufferLimitError
	if !errors.As(err, &limitErr) {
		t.Fatalf("Write error type = %T, want *BufferLimitError", err)
	}
	if got, want := limitErr.Scope, bufferScopeWriterEntryCompressed; got != want {
		t.Fatalf("limit scope = %q, want %q", got, want)
	}
	if got, want := limitErr.Limit, DefaultMaxEntryBufferSize; got != want {
		t.Fatalf("limit value = %d, want %d", got, want)
	}
	if got, want := limitErr.Buffered, DefaultMaxEntryBufferSize; got != want {
		t.Fatalf("buffered = %d, want %d", got, want)
	}
	if got, want := limitErr.Attempted, uint64(1); got != want {
		t.Fatalf("attempted = %d, want %d", got, want)
	}
	if err := fw.Close(); !errors.Is(err, ErrBufferLimitExceeded) {
		t.Fatalf("Close error = %v, want %v", err, ErrBufferLimitExceeded)
	}
}

func TestWriterBufferLimitOverride(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.SetBufferLimits(WriteBufferLimits{
		MaxEntryBufferSize:         4,
		MaxMethod14InputBufferSize: 8,
	})

	iw, err := w.CreateHeader(&FileHeader{Name: "override-limit.bin", Method: Store})
	if err != nil {
		t.Fatalf("CreateHeader: %v", err)
	}
	fw := iw.(*fileWriter)
	if got, want := fw.entryBufferLimit, uint64(4); got != want {
		t.Fatalf("entryBufferLimit = %d, want %d", got, want)
	}

	n, err := fw.Write([]byte("abcdef"))
	if got, want := n, 4; got != want {
		t.Fatalf("Write bytes = %d, want %d", got, want)
	}
	if !errors.Is(err, ErrBufferLimitExceeded) {
		t.Fatalf("Write error = %v, want %v", err, ErrBufferLimitExceeded)
	}

	var limitErr *BufferLimitError
	if !errors.As(err, &limitErr) {
		t.Fatalf("Write error type = %T, want *BufferLimitError", err)
	}
	if got, want := limitErr.Limit, uint64(4); got != want {
		t.Fatalf("limit = %d, want %d", got, want)
	}

	var buf2 bytes.Buffer
	w2 := NewWriter(&buf2)
	w2.SetBufferLimits(WriteBufferLimits{
		MaxEntryBufferSize:         8,
		MaxMethod14InputBufferSize: 8,
	})
	iw2, err := w2.CreateHeader(&FileHeader{Name: "override-ok.bin", Method: Store})
	if err != nil {
		t.Fatalf("CreateHeader second: %v", err)
	}
	if n, err := iw2.Write([]byte("12345678")); err != nil || n != 8 {
		t.Fatalf("Write second = (%d, %v), want (8, nil)", n, err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("Close second writer: %v", err)
	}
}

func TestMultiVolumeWriterCompressedStreamingIgnoresBufferLimits(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "entry-buffer.arj")
	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{
		VolumeSize: 8 * 1024,
		BufferLimits: WriteBufferLimits{
			MaxEntryBufferSize: 4,
		},
	})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}

	iw, err := mw.Create("small.bin")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, ok := iw.(*multiVolumeCompressedFileWriter); !ok {
		t.Fatalf("writer type = %T, want *multiVolumeCompressedFileWriter", iw)
	}

	n, err := iw.Write([]byte("abcdef"))
	if got, want := n, 6; got != want {
		t.Fatalf("Write bytes = %d, want %d", got, want)
	}
	if err != nil {
		t.Fatalf("Write error = %v, want nil", err)
	}
	if err := iw.(io.Closer).Close(); err != nil {
		t.Fatalf("Close entry: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()
	if got, want := len(r.File), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	if got := mustReadFileEntry(t, r.File[0]); string(got) != "abcdef" {
		t.Fatalf("payload = %q, want %q", got, "abcdef")
	}

	archivePath2 := filepath.Join(t.TempDir(), "entry-buffer-default.arj")
	mw2, err := NewMultiVolumeWriter(archivePath2, MultiVolumeWriterOptions{VolumeSize: 8 * 1024})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter second: %v", err)
	}
	mw2.SetBufferLimits(WriteBufferLimits{MaxEntryBufferSize: 8})
	iw2, err := mw2.Create("ok.bin")
	if err != nil {
		t.Fatalf("Create second: %v", err)
	}
	if n, err := iw2.Write([]byte("12345678")); err != nil || n != 8 {
		t.Fatalf("Write second = (%d, %v), want (8, nil)", n, err)
	}
	if err := mw2.Close(); err != nil {
		t.Fatalf("Close second writer: %v", err)
	}
}

func TestWriterEntryBufferSpillsToDiskAndCleansUp(t *testing.T) {
	var archive bytes.Buffer
	w := NewWriter(&archive)
	w.SetBufferLimits(WriteBufferLimits{
		MaxEntryBufferSize:         maxInMemoryEntrySpoolSize + (2 << 20),
		MaxMethod14InputBufferSize: DefaultMaxMethod14InputBufferSize,
	})

	iw, err := w.CreateHeader(&FileHeader{Name: "spill.bin", Method: Store})
	if err != nil {
		t.Fatalf("CreateHeader: %v", err)
	}
	fw := iw.(*fileWriter)

	payload := bytes.Repeat([]byte{'s'}, int(maxInMemoryEntrySpoolSize)+(256<<10))
	if n, err := fw.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("Write = (%d, %v), want (%d, nil)", n, err, len(payload))
	}
	if !fw.comp.Spilled() {
		t.Fatalf("comp buffer not spilled")
	}
	if got, max := fw.comp.InMemorySize(), uint64(maxInMemoryEntrySpoolSize); got > max {
		t.Fatalf("in-memory size = %d, want <= %d", got, max)
	}
	tempPath := fw.comp.TempPath()
	if tempPath == "" {
		t.Fatalf("temp path empty after spill")
	}
	if _, err := os.Stat(tempPath); err != nil {
		t.Fatalf("Stat temp file: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	if _, err := os.Stat(tempPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temp file after close err = %v, want %v", err, os.ErrNotExist)
	}

	r, err := NewReader(bytes.NewReader(archive.Bytes()), int64(archive.Len()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if got, want := len(r.File), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	if got := mustReadFileEntry(t, r.File[0]); !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch")
	}
}

func TestWriterEntryBufferSpillTempFileRemovedOnCloseError(t *testing.T) {
	var archive bytes.Buffer
	w := NewWriter(&archive)
	w.SetBufferLimits(WriteBufferLimits{MaxEntryBufferSize: maxInMemoryEntrySpoolSize + (2 << 20)})

	iw, err := w.CreateHeader(&FileHeader{Name: "spill-dir/"})
	if err != nil {
		t.Fatalf("CreateHeader: %v", err)
	}
	fw := iw.(*fileWriter)

	payload := bytes.Repeat([]byte{'d'}, int(maxInMemoryEntrySpoolSize)+(256<<10))
	if n, err := fw.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("Write = (%d, %v), want (%d, nil)", n, err, len(payload))
	}
	if !fw.comp.Spilled() {
		t.Fatalf("comp buffer not spilled")
	}
	tempPath := fw.comp.TempPath()
	if tempPath == "" {
		t.Fatalf("temp path empty after spill")
	}
	if _, err := os.Stat(tempPath); err != nil {
		t.Fatalf("Stat temp file: %v", err)
	}

	if err := fw.Close(); !errors.Is(err, errDirectoryFileData) {
		t.Fatalf("Close error = %v, want %v", err, errDirectoryFileData)
	}
	if _, err := os.Stat(tempPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temp file after close err = %v, want %v", err, os.ErrNotExist)
	}
}

func TestWriterSeekableCreateHeaderStreamsAndPatchesLocalHeader(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "seekable-stream.arj")
	f, err := os.OpenFile(archivePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer f.Close()

	w := NewWriter(f)
	iw, err := w.CreateHeader(&FileHeader{Name: "stream.bin", Method: Store})
	if err != nil {
		t.Fatalf("CreateHeader: %v", err)
	}
	fw := iw.(*fileWriter)
	if fw.comp != nil {
		t.Fatalf("comp buffer present for seekable output")
	}

	beforeInfo, err := f.Stat()
	if err != nil {
		t.Fatalf("Stat before write: %v", err)
	}
	beforeSize := beforeInfo.Size()

	payload := bytes.Repeat([]byte("seekable-stream-"), 512)
	n, err := iw.Write(payload)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got, want := n, len(payload); got != want {
		t.Fatalf("Write bytes = %d, want %d", got, want)
	}

	afterInfo, err := f.Stat()
	if err != nil {
		t.Fatalf("Stat after write: %v", err)
	}
	if afterInfo.Size() <= beforeSize {
		t.Fatalf("archive size after write = %d, want > %d", afterInfo.Size(), beforeSize)
	}

	if err := iw.(io.Closer).Close(); err != nil {
		t.Fatalf("Close entry: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()
	if got, want := len(r.File), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	fh := r.File[0]
	if got, want := fh.UncompressedSize64, uint64(len(payload)); got != want {
		t.Fatalf("uncompressed size = %d, want %d", got, want)
	}
	if got, want := fh.CompressedSize64, uint64(len(payload)); got != want {
		t.Fatalf("compressed size = %d, want %d", got, want)
	}
	if got := mustReadFileEntry(t, fh); !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch")
	}
}

func TestMultiVolumeWriterStoreStreamsAcrossVolumesDuringWrite(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "store-streaming.arj")
	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 512})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}

	iw, err := mw.CreateHeader(&FileHeader{Name: "store.bin", Method: Store})
	if err != nil {
		t.Fatalf("CreateHeader: %v", err)
	}
	if _, ok := iw.(*multiVolumeStoreFileWriter); !ok {
		t.Fatalf("writer type = %T, want *multiVolumeStoreFileWriter", iw)
	}

	payload := bytes.Repeat([]byte("store-stream-segment-"), 700)
	n, err := iw.Write(payload)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got, want := n, len(payload); got != want {
		t.Fatalf("Write bytes = %d, want %d", got, want)
	}

	if got := len(mw.Parts()); got < 2 {
		t.Fatalf("parts during active store stream = %d, want >= 2", got)
	}

	if err := iw.(io.Closer).Close(); err != nil {
		t.Fatalf("Close entry: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	mr, err := OpenMultiReader(archivePath)
	if err != nil {
		t.Fatalf("OpenMultiReader: %v", err)
	}
	defer mr.Close()
	if got, want := len(mr.File), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	if got := mustReadFileEntry(t, mr.File[0]); !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch")
	}
}

func TestMultiVolumeWriterCompressedStreamsAcrossVolumesDuringWrite(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "compressed-streaming-parts.arj")
	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 768})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}

	iw, err := mw.CreateHeader(&FileHeader{Name: "compressed.bin", Method: Method1})
	if err != nil {
		t.Fatalf("CreateHeader: %v", err)
	}
	if _, ok := iw.(*multiVolumeCompressedFileWriter); !ok {
		t.Fatalf("writer type = %T, want *multiVolumeCompressedFileWriter", iw)
	}

	payload := make([]byte, 192<<10)
	for i := range payload {
		payload[i] = byte((i * 131) % 251)
	}
	n, err := iw.Write(payload)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got, want := n, len(payload); got != want {
		t.Fatalf("Write bytes = %d, want %d", got, want)
	}

	if got := len(mw.Parts()); got < 2 {
		t.Fatalf("parts during active compressed stream = %d, want >= 2", got)
	}

	if err := iw.(io.Closer).Close(); err != nil {
		t.Fatalf("Close entry: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	mr, err := OpenMultiReader(archivePath)
	if err != nil {
		t.Fatalf("OpenMultiReader: %v", err)
	}
	defer mr.Close()
	if got, want := len(mr.File), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	if got := mustReadFileEntry(t, mr.File[0]); !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch")
	}
}

func TestMultiVolumeWriterCompressedStreamingRoundTrip(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "compressed-streaming.arj")
	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{
		VolumeSize: 32 << 20,
		BufferLimits: WriteBufferLimits{
			MaxEntryBufferSize: 64,
		},
	})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}

	iw, err := mw.Create("stream.bin")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, ok := iw.(*multiVolumeCompressedFileWriter); !ok {
		t.Fatalf("writer type = %T, want *multiVolumeCompressedFileWriter", iw)
	}

	payload := bytes.Repeat([]byte{'m'}, int(maxInMemoryEntrySpoolSize)+(256<<10))
	if n, err := iw.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("Write = (%d, %v), want (%d, nil)", n, err, len(payload))
	}
	if err := iw.(io.Closer).Close(); err != nil {
		t.Fatalf("Close entry: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	rc, err := OpenMultiReader(archivePath)
	if err != nil {
		t.Fatalf("OpenMultiReader: %v", err)
	}
	defer rc.Close()
	if got, want := len(rc.File), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	if got := mustReadFileEntry(t, rc.File[0]); !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch")
	}
}

func TestMultiVolumeWriterCompressedStreamingVolumeTooSmall(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "compressed-too-small.arj")
	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{
		VolumeSize: 64,
	})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}

	iw, err := mw.Create("stream.bin")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := iw.Write([]byte("x")); !errors.Is(err, errVolumeTooSmall) {
		t.Fatalf("Write error = %v, want %v", err, errVolumeTooSmall)
	}
}

func TestMultiVolumeWriterCompressedStreamingIgnoresMethod14InputLimit(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "method14-limit.arj")
	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{
		VolumeSize: 32 * 1024,
		BufferLimits: WriteBufferLimits{
			MaxEntryBufferSize:         1024,
			MaxMethod14InputBufferSize: 8,
		},
	})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}

	iw, err := mw.CreateHeader(&FileHeader{Name: "m1.bin", Method: Method1})
	if err != nil {
		t.Fatalf("CreateHeader: %v", err)
	}
	if n, err := iw.Write([]byte("0123456789")); err != nil || n != 10 {
		t.Fatalf("Write = (%d, %v), want (10, nil)", n, err)
	}

	if err := iw.(io.Closer).Close(); err != nil {
		t.Fatalf("Close error = %v, want nil", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := OpenMultiReader(archivePath)
	if err != nil {
		t.Fatalf("OpenMultiReader: %v", err)
	}
	defer r.Close()
	if got, want := len(r.File), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	if got := mustReadFileEntry(t, r.File[0]); string(got) != "0123456789" {
		t.Fatalf("payload = %q, want %q", got, "0123456789")
	}
}

func TestWriterRegisterCompressorNilDisablesFallback(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.RegisterCompressor(Method1, nil)

	_, err := w.CreateHeader(&FileHeader{Name: "disabled.bin", Method: Method1})
	if !errors.Is(err, ErrAlgorithm) {
		t.Fatalf("CreateHeader error = %v, want %v", err, ErrAlgorithm)
	}
}

func TestMultiVolumeWriterRegisterCompressorNilDisablesFallback(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "disabled.arj")
	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 1024})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}
	mw.RegisterCompressor(Method1, nil)

	_, err = mw.CreateHeader(&FileHeader{Name: "disabled.bin", Method: Method1})
	if !errors.Is(err, ErrAlgorithm) {
		t.Fatalf("CreateHeader error = %v, want %v", err, ErrAlgorithm)
	}
}

func TestWriterRegisterCompressorConcurrentLookup(t *testing.T) {
	const customMethod uint16 = 250

	w := NewWriter(io.Discard)
	customComp := func(out io.Writer) (io.WriteCloser, error) {
		return nopWriteCloser{Writer: out}, nil
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 5000; i++ {
			if i%2 == 0 {
				w.RegisterCompressor(customMethod, customComp)
				continue
			}
			w.RegisterCompressor(customMethod, nil)
		}
	}()

	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 5000; i++ {
			_ = w.compressor(customMethod)
			_ = w.compressor(Method1)
		}
	}()

	close(start)
	wg.Wait()

	w.RegisterCompressor(customMethod, customComp)
	if got := w.compressor(customMethod); got == nil {
		t.Fatalf("custom compressor missing after registration")
	}
}

func TestWriterSharedConfigConcurrentReadWrite(t *testing.T) {
	w := NewWriter(io.Discard)

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	errCh := make(chan error, 1)
	reportErr := func(err error) {
		select {
		case errCh <- err:
		default:
		}
	}

	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 2000; i++ {
			name := fmt.Sprintf("race-%d.arj", i)
			comment := fmt.Sprintf("comment-%d", i)
			if err := w.SetArchiveName(name); err != nil {
				reportErr(fmt.Errorf("SetArchiveName: %w", err))
				return
			}
			if err := w.SetComment(comment); err != nil {
				reportErr(fmt.Errorf("SetComment: %w", err))
				return
			}
			if err := w.SetArchiveHeader(&ArchiveHeader{
				FirstHeaderSize: arjMinFirstHeaderSize,
				Name:            name,
				Comment:         comment,
			}); err != nil {
				reportErr(fmt.Errorf("SetArchiveHeader: %w", err))
				return
			}
			w.SetBufferLimits(WriteBufferLimits{
				MaxEntryBufferSize:         uint64(64 + (i % 8)),
				MaxMethod14InputBufferSize: uint64(96 + (i % 8)),
			})
		}
	}()

	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 2000; i++ {
			_ = w.mainHeaderForWrite()
			_ = w.writeBufferLimits()
			_ = w.compressor(Method1)
		}
	}()

	close(start)
	wg.Wait()

	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
}

func TestMultiVolumeWriterRegisterCompressorConcurrentLookup(t *testing.T) {
	const customMethod uint16 = 251

	archivePath := filepath.Join(t.TempDir(), "race-register.arj")
	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 1024})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}

	customComp := func(out io.Writer) (io.WriteCloser, error) {
		return nopWriteCloser{Writer: out}, nil
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 5000; i++ {
			if i%2 == 0 {
				mw.RegisterCompressor(customMethod, customComp)
				continue
			}
			mw.RegisterCompressor(customMethod, nil)
		}
	}()

	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 5000; i++ {
			_ = mw.compressor(customMethod)
			_ = mw.compressor(Method1)
		}
	}()

	close(start)
	wg.Wait()

	mw.RegisterCompressor(customMethod, customComp)
	if got := mw.compressor(customMethod); got == nil {
		t.Fatalf("custom compressor missing after registration")
	}
}

func TestMultiVolumeWriterSharedConfigConcurrentReadWrite(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "race-config.arj")
	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 1024})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	errCh := make(chan error, 1)
	reportErr := func(err error) {
		select {
		case errCh <- err:
		default:
		}
	}

	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 2000; i++ {
			name := fmt.Sprintf("multi-%d.arj", i)
			comment := fmt.Sprintf("comment-%d", i)
			if err := mw.SetArchiveName(name); err != nil {
				reportErr(fmt.Errorf("SetArchiveName: %w", err))
				return
			}
			if err := mw.SetComment(comment); err != nil {
				reportErr(fmt.Errorf("SetComment: %w", err))
				return
			}
			if err := mw.SetArchiveHeader(&ArchiveHeader{
				FirstHeaderSize: arjMinFirstHeaderSize,
				Name:            name,
				Comment:         comment,
			}); err != nil {
				reportErr(fmt.Errorf("SetArchiveHeader: %w", err))
				return
			}
			mw.SetBufferLimits(WriteBufferLimits{
				MaxEntryBufferSize:         uint64(64 + (i % 8)),
				MaxMethod14InputBufferSize: uint64(96 + (i % 8)),
			})
		}
	}()

	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 2000; i++ {
			_ = mw.mainHeaderForVolume()
			_ = mw.writeBufferLimits()
			_ = mw.compressor(Method1)
		}
	}()

	close(start)
	wg.Wait()

	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
}

func TestWriterDefaultMainHeaderTimestampSnapshot(t *testing.T) {
	w := NewWriter(io.Discard)
	first := w.mainHeaderForWrite()
	if first.Created.IsZero() || first.Modified.IsZero() {
		t.Fatalf("default archive timestamps must be non-zero: created=%v modified=%v", first.Created, first.Modified)
	}

	for i := 0; i < 64; i++ {
		got := w.mainHeaderForWrite()
		if !got.Created.Equal(first.Created) {
			t.Fatalf("Created changed between snapshots: got %v want %v", got.Created, first.Created)
		}
		if !got.Modified.Equal(first.Modified) {
			t.Fatalf("Modified changed between snapshots: got %v want %v", got.Modified, first.Modified)
		}
	}
}

func TestMultiVolumeWriterDefaultMainHeaderTimestampSnapshot(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "multi-default-time.arj")
	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 1024})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}

	first := mw.mainHeaderForVolume()
	if first.Created.IsZero() || first.Modified.IsZero() {
		t.Fatalf("default archive timestamps must be non-zero: created=%v modified=%v", first.Created, first.Modified)
	}

	for i := 0; i < 64; i++ {
		got := mw.mainHeaderForVolume()
		if !got.Created.Equal(first.Created) {
			t.Fatalf("Created changed between snapshots: got %v want %v", got.Created, first.Created)
		}
		if !got.Modified.Equal(first.Modified) {
			t.Fatalf("Modified changed between snapshots: got %v want %v", got.Modified, first.Modified)
		}
	}
}

func TestMultiVolumeWriterMaxCompressedChunkNoMonotonicAssumption(t *testing.T) {
	const customMethod uint16 = 251

	mw := &MultiVolumeWriter{}
	mw.RegisterCompressor(customMethod, func(out io.Writer) (io.WriteCloser, error) {
		return &nonMonotonicChunkCompressor{
			out:         out,
			defaultSize: 10,
			sizeByN:     map[int]int{8: 4},
		}, nil
	})

	plain := bytes.Repeat([]byte("a"), 8)
	n, comp, err := mw.maxCompressedChunk(customMethod, plain, 6)
	if err != nil {
		t.Fatalf("maxCompressedChunk: %v", err)
	}
	if got, want := n, len(plain); got != want {
		t.Fatalf("chunk len = %d, want %d", got, want)
	}
	if got, want := len(comp), 4; got != want {
		t.Fatalf("compressed len = %d, want %d", got, want)
	}
}

func TestMultiVolumeWriterMaxCompressedChunkNoMonotonicAssumptionFindsLargerPrefix(t *testing.T) {
	const customMethod uint16 = 252

	mw := &MultiVolumeWriter{}
	mw.RegisterCompressor(customMethod, func(out io.Writer) (io.WriteCloser, error) {
		return &nonMonotonicChunkCompressor{
			out:         out,
			defaultSize: 10,
			sizeByN:     map[int]int{7: 4},
		}, nil
	})

	plain := bytes.Repeat([]byte("a"), 8)
	n, comp, err := mw.maxCompressedChunk(customMethod, plain, 6)
	if err != nil {
		t.Fatalf("maxCompressedChunk: %v", err)
	}
	if got, want := n, 7; got != want {
		t.Fatalf("chunk len = %d, want %d", got, want)
	}
	if got, want := len(comp), 4; got != want {
		t.Fatalf("compressed len = %d, want %d", got, want)
	}
}

func TestMultiVolumeWriterMaxCompressedChunkAvoidsDescendingFullScan(t *testing.T) {
	const customMethod uint16 = 253

	var calls uint32
	mw := &MultiVolumeWriter{}
	mw.RegisterCompressor(customMethod, func(out io.Writer) (io.WriteCloser, error) {
		atomic.AddUint32(&calls, 1)
		return &linearChunkCompressor{out: out}, nil
	})

	plain := bytes.Repeat([]byte("a"), 4096)
	n, comp, err := mw.maxCompressedChunk(customMethod, plain, 512)
	if err != nil {
		t.Fatalf("maxCompressedChunk: %v", err)
	}
	if got, want := n, 511; got != want {
		t.Fatalf("chunk len = %d, want %d", got, want)
	}
	if got := len(comp); got > 512 {
		t.Fatalf("compressed len = %d, want <= %d", got, 512)
	}
	if got, max := atomic.LoadUint32(&calls), uint32(32); got > max {
		t.Fatalf("compress calls = %d, want <= %d", got, max)
	}
}

func TestRegisterCompressorDuplicatePanics(t *testing.T) {
	lockGlobalRegistryTests(t)

	method := nextUnregisteredMethod(t)
	RegisterCompressor(method, func(out io.Writer) (io.WriteCloser, error) {
		return nopWriteCloser{Writer: out}, nil
	})

	defer func() {
		v := recover()
		if v == nil {
			t.Fatalf("RegisterCompressor duplicate did not panic")
		}
		if msg := fmt.Sprint(v); !strings.Contains(msg, "compressor already registered") {
			t.Fatalf("panic = %q, want substring %q", msg, "compressor already registered")
		}
	}()

	RegisterCompressor(method, func(out io.Writer) (io.WriteCloser, error) {
		return nopWriteCloser{Writer: out}, nil
	})
}

func TestRegisterDecompressorDuplicatePanics(t *testing.T) {
	lockGlobalRegistryTests(t)

	method := nextUnregisteredMethod(t)
	RegisterDecompressor(method, io.NopCloser)

	defer func() {
		v := recover()
		if v == nil {
			t.Fatalf("RegisterDecompressor duplicate did not panic")
		}
		if msg := fmt.Sprint(v); !strings.Contains(msg, "decompressor already registered") {
			t.Fatalf("panic = %q, want substring %q", msg, "decompressor already registered")
		}
	}()

	RegisterDecompressor(method, io.NopCloser)
}

type nonMonotonicChunkCompressor struct {
	out         io.Writer
	n           int
	defaultSize int
	sizeByN     map[int]int
}

func (w *nonMonotonicChunkCompressor) Write(p []byte) (int, error) {
	w.n += len(p)
	return len(p), nil
}

func (w *nonMonotonicChunkCompressor) Close() error {
	size := w.defaultSize
	if size == 0 {
		size = 10
	}
	if override, ok := w.sizeByN[w.n]; ok {
		size = override
	}
	if size <= 0 {
		return nil
	}
	_, err := w.out.Write(bytes.Repeat([]byte{'z'}, size))
	return err
}

type linearChunkCompressor struct {
	out io.Writer
	n   int
}

func (w *linearChunkCompressor) Write(p []byte) (int, error) {
	w.n += len(p)
	return len(p), nil
}

func (w *linearChunkCompressor) Close() error {
	size := w.n + 1
	if size <= 0 {
		return nil
	}
	_, err := w.out.Write(bytes.Repeat([]byte{'q'}, size))
	return err
}

type failOnWriteCloser struct {
	err error
}

func (w *failOnWriteCloser) Write([]byte) (int, error) {
	return 0, w.err
}

func (w *failOnWriteCloser) Close() error {
	return nil
}

var registerMethodSeed uint32 = 0xf000
var registerGlobalRegistryMu sync.Mutex

func lockGlobalRegistryTests(t *testing.T) {
	t.Helper()
	registerGlobalRegistryMu.Lock()
	t.Cleanup(registerGlobalRegistryMu.Unlock)
}

func nextUnregisteredMethod(t *testing.T) uint16 {
	t.Helper()
	for i := 0; i < 0x0fff; i++ {
		n := atomic.AddUint32(&registerMethodSeed, 1)
		method := uint16(0xf000 + (n % 0x0fff))
		if compressor(method) == nil && decompressor(method) == nil {
			return method
		}
	}
	t.Fatalf("failed to find unregistered method id")
	return 0
}
