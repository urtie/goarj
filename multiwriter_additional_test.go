package arj

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"
)

func TestMultiVolumeWriterCreateRawRoundTrip(t *testing.T) {
	payload := []byte("multi-create-raw")
	archivePath := filepath.Join(t.TempDir(), "multi-create-raw.arj")

	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 16 << 10})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}
	fw, err := mw.CreateRaw(&FileHeader{
		Name:               "raw-create.bin",
		Method:             Store,
		CRC32:              crc32.ChecksumIEEE(payload),
		CompressedSize64:   uint64(len(payload)),
		UncompressedSize64: uint64(len(payload)),
	})
	if err != nil {
		t.Fatalf("CreateRaw: %v", err)
	}
	if _, err := fw.Write(payload); err != nil {
		t.Fatalf("Write raw payload: %v", err)
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
		t.Fatalf("payload = %q, want %q", got, payload)
	}
}

func TestMultiVolumeWriterCreateRawRejectsCompressedSizeMismatch(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "raw-compressed-size-mismatch.arj")
	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 16 << 10})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}

	payload := []byte("raw-size-mismatch")
	fw, err := mw.CreateRaw(&FileHeader{
		Name:               "bad-raw.bin",
		Method:             Store,
		CompressedSize64:   uint64(len(payload) - 1),
		UncompressedSize64: uint64(len(payload)),
		CRC32:              crc32.ChecksumIEEE(payload),
	})
	if err != nil {
		t.Fatalf("CreateRaw: %v", err)
	}
	if _, err := fw.Write(payload); err != nil {
		t.Fatalf("Write raw payload: %v", err)
	}
	if err := fw.(io.Closer).Close(); !errors.Is(err, errRawPayloadSizeMismatch) {
		t.Fatalf("Close raw writer error = %v, want %v", err, errRawPayloadSizeMismatch)
	}

	tail, err := mw.Create("tail.txt")
	if err != nil {
		t.Fatalf("Create tail: %v", err)
	}
	if _, err := tail.Write([]byte("tail")); err != nil {
		t.Fatalf("Write tail: %v", err)
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
	if got, want := r.File[0].Name, "tail.txt"; got != want {
		t.Fatalf("entry name = %q, want %q", got, want)
	}
}

func TestMultiVolumeWriterCreateRawRejectsStoreUncompressedSizeMismatch(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "raw-store-size-mismatch.arj")
	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 16 << 10})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}

	payload := []byte("store-size-mismatch")
	fw, err := mw.CreateRaw(&FileHeader{
		Name:               "bad-store-raw.bin",
		Method:             Store,
		CompressedSize64:   uint64(len(payload)),
		UncompressedSize64: uint64(len(payload) + 1),
		CRC32:              crc32.ChecksumIEEE(payload),
	})
	if err != nil {
		t.Fatalf("CreateRaw: %v", err)
	}
	if _, err := fw.Write(payload); err != nil {
		t.Fatalf("Write raw payload: %v", err)
	}
	if err := fw.(io.Closer).Close(); !errors.Is(err, errRawStoreSizeMismatch) {
		t.Fatalf("Close raw writer error = %v, want %v", err, errRawStoreSizeMismatch)
	}

	tail, err := mw.Create("tail.txt")
	if err != nil {
		t.Fatalf("Create tail: %v", err)
	}
	if _, err := tail.Write([]byte("tail")); err != nil {
		t.Fatalf("Write tail: %v", err)
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
	if got, want := r.File[0].Name, "tail.txt"; got != want {
		t.Fatalf("entry name = %q, want %q", got, want)
	}
}

func TestMultiVolumeWriterCopyRoundTrip(t *testing.T) {
	payload := []byte("multi-copy-payload")
	srcArchive := buildSingleRawArchive(t, &FileHeader{
		Name:               "src.bin",
		Method:             Store,
		CRC32:              crc32.ChecksumIEEE(payload),
		CompressedSize64:   uint64(len(payload)),
		UncompressedSize64: uint64(len(payload)),
	}, payload)

	srcReader, err := NewReader(bytes.NewReader(srcArchive), int64(len(srcArchive)))
	if err != nil {
		t.Fatalf("NewReader(source): %v", err)
	}

	archivePath := filepath.Join(t.TempDir(), "multi-copy.arj")
	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 16 << 10})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}
	if err := mw.Copy(srcReader.File[0]); err != nil {
		t.Fatalf("Copy: %v", err)
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
		t.Fatalf("payload = %q, want %q", got, payload)
	}
}

func TestMultiVolumeWriterAddFS(t *testing.T) {
	mod := time.Date(2024, time.August, 9, 10, 11, 12, 0, time.UTC)
	fsys := fstest.MapFS{
		"root.txt": {Data: []byte("root"), Mode: 0o644, ModTime: mod},
		"sub":      {Mode: fs.ModeDir | 0o755, ModTime: mod},
		"sub/leaf.txt": {
			Data:    []byte("leaf"),
			Mode:    0o600,
			ModTime: mod,
		},
	}

	archivePath := filepath.Join(t.TempDir(), "multi-addfs.arj")
	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 16 << 10})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}
	if err := mw.AddFS(fsys); err != nil {
		t.Fatalf("AddFS: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := OpenMultiReader(archivePath)
	if err != nil {
		t.Fatalf("OpenMultiReader: %v", err)
	}
	defer r.Close()
	methodByName := make(map[string]uint16, len(r.File))
	for _, f := range r.File {
		methodByName[f.Name] = f.Method
	}
	if got, want := methodByName["root.txt"], uint16(Method1); got != want {
		t.Fatalf("root.txt method = %d, want %d", got, want)
	}
	if got, want := methodByName["sub/leaf.txt"], uint16(Method1); got != want {
		t.Fatalf("sub/leaf.txt method = %d, want %d", got, want)
	}
	if got, want := methodByName["sub/"], uint16(Store); got != want {
		t.Fatalf("sub/ method = %d, want %d", got, want)
	}

	root, err := r.Open("root.txt")
	if err != nil {
		t.Fatalf("Open(root.txt): %v", err)
	}
	rootData, err := io.ReadAll(root)
	_ = root.Close()
	if err != nil {
		t.Fatalf("ReadAll(root.txt): %v", err)
	}
	if got, want := string(rootData), "root"; got != want {
		t.Fatalf("root.txt payload = %q, want %q", got, want)
	}

	leaf, err := r.Open("sub/leaf.txt")
	if err != nil {
		t.Fatalf("Open(sub/leaf.txt): %v", err)
	}
	leafData, err := io.ReadAll(leaf)
	_ = leaf.Close()
	if err != nil {
		t.Fatalf("ReadAll(sub/leaf.txt): %v", err)
	}
	if got, want := string(leafData), "leaf"; got != want {
		t.Fatalf("sub/leaf.txt payload = %q, want %q", got, want)
	}

	var subDir *File
	for _, f := range r.File {
		if f.Name == "sub/" {
			subDir = f
			break
		}
	}
	if subDir == nil {
		t.Fatalf("missing explicit directory entry sub/")
	}
	if !subDir.isDir() {
		t.Fatalf("sub/ isDir = false, want true")
	}
	if subDir.UncompressedSize64 != 0 || subDir.CompressedSize64 != 0 || subDir.CRC32 != 0 {
		t.Fatalf("sub/ sizes/crc = (%d,%d,%#08x), want (0,0,0)", subDir.UncompressedSize64, subDir.CompressedSize64, subDir.CRC32)
	}
	dirRC, err := subDir.Open()
	if err != nil {
		t.Fatalf("Open(sub/): %v", err)
	}
	dirPayload, err := io.ReadAll(dirRC)
	if err != nil {
		t.Fatalf("ReadAll(sub/): %v", err)
	}
	if err := dirRC.Close(); err != nil {
		t.Fatalf("Close(sub/): %v", err)
	}
	if len(dirPayload) != 0 {
		t.Fatalf("sub/ payload len = %d, want 0", len(dirPayload))
	}
}

func TestMultiVolumeWriterCopyCompressedSizeMismatchAbortsStagedEntry(t *testing.T) {
	payload := []byte("copy-size-mismatch")
	srcArchive := buildSingleRawArchive(t, &FileHeader{
		Name:               "src.bin",
		Method:             Store,
		CRC32:              crc32.ChecksumIEEE(payload),
		CompressedSize64:   uint64(len(payload)),
		UncompressedSize64: uint64(len(payload)),
	}, payload)

	srcReader, err := NewReader(bytes.NewReader(srcArchive), int64(len(srcArchive)))
	if err != nil {
		t.Fatalf("NewReader(source): %v", err)
	}
	src := srcReader.File[0]
	src.CompressedSize64++
	src.segments = []fileSegment{{
		dataOffset:       src.dataOffset,
		method:           src.Method,
		flags:            src.Flags,
		extFlags:         src.ExtFlags,
		passwordModifier: src.PasswordModifier,
		compressedSize:   uint64(len(payload)),
		uncompressedSize: src.UncompressedSize64,
		crc32:            src.CRC32,
	}}

	archivePath := filepath.Join(t.TempDir(), "copy-mismatch.arj")
	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 16 << 10})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}

	if err := mw.Copy(src); !errors.Is(err, errRawCopySizeMismatch) {
		t.Fatalf("Copy error = %v, want %v", err, errRawCopySizeMismatch)
	}

	fw, err := mw.Create("tail.txt")
	if err != nil {
		t.Fatalf("Create tail: %v", err)
	}
	if _, err := fw.Write([]byte("tail")); err != nil {
		t.Fatalf("Write tail: %v", err)
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
	if got, want := r.File[0].Name, "tail.txt"; got != want {
		t.Fatalf("entry name = %q, want %q", got, want)
	}
	if got := mustReadFileEntry(t, r.File[0]); string(got) != "tail" {
		t.Fatalf("tail payload = %q, want %q", got, "tail")
	}
}

func TestMultiVolumeWriterAddFSOpenFailureAbortsStagedEntry(t *testing.T) {
	wantErr := errors.New("test: addfs open failure")
	fsys := openFailMapFS{
		MapFS: fstest.MapFS{
			"bad.txt": {Data: []byte("bad"), Mode: 0o644},
		},
		failName: "bad.txt",
		failErr:  wantErr,
	}

	archivePath := filepath.Join(t.TempDir(), "addfs-open-fail.arj")
	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 16 << 10})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}

	if err := mw.AddFS(fsys); !errors.Is(err, wantErr) {
		t.Fatalf("AddFS error = %v, want %v", err, wantErr)
	}

	fw, err := mw.Create("tail.txt")
	if err != nil {
		t.Fatalf("Create tail: %v", err)
	}
	if _, err := fw.Write([]byte("tail")); err != nil {
		t.Fatalf("Write tail: %v", err)
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
	if got, want := r.File[0].Name, "tail.txt"; got != want {
		t.Fatalf("entry name = %q, want %q", got, want)
	}
}

func TestMultiVolumeWriterAddFSCopyFailureAbortsStagedEntry(t *testing.T) {
	wantErr := errors.New("test: addfs copy failure")
	fsys := readFailMapFS{
		MapFS: fstest.MapFS{
			"bad.txt": {Data: []byte("bad-data"), Mode: 0o644},
		},
		failName: "bad.txt",
		failErr:  wantErr,
	}

	archivePath := filepath.Join(t.TempDir(), "addfs-copy-fail.arj")
	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 16 << 10})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}

	if err := mw.AddFS(fsys); !errors.Is(err, wantErr) {
		t.Fatalf("AddFS error = %v, want %v", err, wantErr)
	}

	if _, err := mw.Create("tail.txt"); !errors.Is(err, wantErr) {
		t.Fatalf("Create tail error = %v, want wrapped %v", err, wantErr)
	}
	if err := mw.Close(); !errors.Is(err, wantErr) {
		t.Fatalf("Close writer error = %v, want wrapped %v", err, wantErr)
	}
}

func TestMultiVolumeWriterCreateRawRejectsOversizeForSingleVolume(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "raw-too-large.arj")
	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 96})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}

	payload := bytes.Repeat([]byte("x"), 64)
	fw, err := mw.CreateRaw(&FileHeader{
		Name:               "raw.bin",
		Method:             Store,
		CRC32:              crc32.ChecksumIEEE(payload),
		CompressedSize64:   uint64(len(payload)),
		UncompressedSize64: uint64(len(payload)),
	})
	if err != nil {
		t.Fatalf("CreateRaw: %v", err)
	}
	if _, err := fw.Write(payload); err != nil {
		t.Fatalf("Write raw payload: %v", err)
	}
	closer, ok := fw.(io.Closer)
	if !ok {
		t.Fatalf("CreateRaw writer type %T is not io.Closer", fw)
	}
	if err := closer.Close(); !errors.Is(err, ErrRawEntryTooLargeForVolume) {
		t.Fatalf("Close raw writer error = %v, want %v", err, ErrRawEntryTooLargeForVolume)
	}

	if err := mw.Close(); err != nil {
		t.Fatalf("Close multi writer after rejection: %v", err)
	}
	if got, want := len(mw.Parts()), 1; got != want {
		t.Fatalf("parts len = %d, want %d", got, want)
	}

	r, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()
	if got, want := len(r.File), 0; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
}

func TestMultiVolumeWriterCompressorSnapshotPerEntry(t *testing.T) {
	const method uint16 = 251
	archivePath := filepath.Join(t.TempDir(), "snapshot-compressor.arj")
	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 16 << 10})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}

	mw.RegisterCompressor(method, taggedSnapshotCompressor('A'))
	fw1, err := mw.CreateHeader(&FileHeader{Name: "first.bin", Method: method})
	if err != nil {
		t.Fatalf("CreateHeader(first): %v", err)
	}

	mw.RegisterCompressor(method, taggedSnapshotCompressor('B'))
	if _, err := fw1.Write([]byte("hello")); err != nil {
		t.Fatalf("Write(first): %v", err)
	}
	if err := fw1.(io.Closer).Close(); err != nil {
		t.Fatalf("Close(first): %v", err)
	}

	fw2, err := mw.CreateHeader(&FileHeader{Name: "second.bin", Method: method})
	if err != nil {
		t.Fatalf("CreateHeader(second): %v", err)
	}
	if _, err := fw2.Write([]byte("bye")); err != nil {
		t.Fatalf("Write(second): %v", err)
	}
	if err := fw2.(io.Closer).Close(); err != nil {
		t.Fatalf("Close(second): %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("Close multi writer: %v", err)
	}

	r, err := OpenMultiReader(archivePath)
	if err != nil {
		t.Fatalf("OpenMultiReader: %v", err)
	}
	defer r.Close()
	if got, want := len(r.File), 2; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}

	raw1 := mustReadRawFileEntry(t, r.File[0])
	raw2 := mustReadRawFileEntry(t, r.File[1])
	if len(raw1) == 0 || raw1[0] != 'A' {
		t.Fatalf("first raw prefix = %q, want %q", raw1, []byte{'A'})
	}
	if len(raw2) == 0 || raw2[0] != 'B' {
		t.Fatalf("second raw prefix = %q, want %q", raw2, []byte{'B'})
	}
}

func TestMultiVolumeWriterCompressedStreamingUnaffectedByLimitChanges(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "snapshot-limits.arj")
	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 16 << 10})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}

	mw.SetBufferLimits(WriteBufferLimits{MaxPlainEntryBufferSize: 4})
	fw1, err := mw.Create("first.bin")
	if err != nil {
		t.Fatalf("Create(first): %v", err)
	}

	mw.SetBufferLimits(WriteBufferLimits{MaxPlainEntryBufferSize: 16})
	n, err := fw1.Write([]byte("abcdef"))
	if got, want := n, 6; got != want {
		t.Fatalf("first write bytes = %d, want %d", got, want)
	}
	if err != nil {
		t.Fatalf("first write error = %v, want nil", err)
	}
	if err := fw1.(io.Closer).Close(); err != nil {
		t.Fatalf("Close(first): %v", err)
	}

	fw2, err := mw.Create("second.bin")
	if err != nil {
		t.Fatalf("Create(second): %v", err)
	}
	n, err = fw2.Write([]byte("abcdef"))
	if got, want := n, 6; got != want {
		t.Fatalf("second write bytes = %d, want %d", got, want)
	}
	if err != nil {
		t.Fatalf("second write error = %v, want nil", err)
	}
	if err := fw2.(io.Closer).Close(); err != nil {
		t.Fatalf("Close(second): %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("Close multi writer: %v", err)
	}

	r, err := OpenMultiReader(archivePath)
	if err != nil {
		t.Fatalf("OpenMultiReader: %v", err)
	}
	defer r.Close()
	if got, want := len(r.File), 2; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	if got, want := r.File[0].Name, "first.bin"; got != want {
		t.Fatalf("entry[0] name = %q, want %q", got, want)
	}
	if got, want := r.File[1].Name, "second.bin"; got != want {
		t.Fatalf("entry name = %q, want %q", got, want)
	}
	if got := mustReadFileEntry(t, r.File[0]); string(got) != "abcdef" {
		t.Fatalf("first payload = %q, want %q", got, "abcdef")
	}
	if got := mustReadFileEntry(t, r.File[1]); string(got) != "abcdef" {
		t.Fatalf("second payload = %q, want %q", got, "abcdef")
	}
}

func TestMultiVolumeWriterArchiveSetterGating(t *testing.T) {
	t.Run("rejected create does not gate setters", func(t *testing.T) {
		archivePath := filepath.Join(t.TempDir(), "setter-rejected.arj")
		mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 16 << 10})
		if err != nil {
			t.Fatalf("NewMultiVolumeWriter: %v", err)
		}

		_, err = mw.CreateHeader(&FileHeader{Name: "bad.bin", Method: 0x1ff})
		if !errors.Is(err, ErrAlgorithm) {
			t.Fatalf("CreateHeader error = %v, want %v", err, ErrAlgorithm)
		}
		if err := mw.SetArchiveName("after-reject.arj"); err != nil {
			t.Fatalf("SetArchiveName after rejection: %v", err)
		}
		if err := mw.SetComment("after-reject-comment"); err != nil {
			t.Fatalf("SetComment after rejection: %v", err)
		}
		if err := mw.SetArchiveHeader(&ArchiveHeader{
			FirstHeaderSize: arjMinFirstHeaderSize,
			Name:            "after-reject.arj",
			Comment:         "after-reject-comment",
		}); err != nil {
			t.Fatalf("SetArchiveHeader after rejection: %v", err)
		}

		fw, err := mw.Create("ok.txt")
		if err != nil {
			t.Fatalf("Create(ok): %v", err)
		}
		if _, err := fw.Write([]byte("ok")); err != nil {
			t.Fatalf("Write(ok): %v", err)
		}
		if err := mw.Close(); err != nil {
			t.Fatalf("Close writer: %v", err)
		}
	})

	t.Run("setters gate when archive output starts", func(t *testing.T) {
		archivePath := filepath.Join(t.TempDir(), "setter-gated.arj")
		mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 16 << 10})
		if err != nil {
			t.Fatalf("NewMultiVolumeWriter: %v", err)
		}

		fw, err := mw.Create("gated.txt")
		if err != nil {
			t.Fatalf("Create(gated): %v", err)
		}
		if err := mw.SetArchiveName("allowed-before-output.arj"); err != nil {
			t.Fatalf("SetArchiveName before output: %v", err)
		}
		if err := mw.SetComment("allowed-before-output-comment"); err != nil {
			t.Fatalf("SetComment before output: %v", err)
		}
		if err := mw.SetArchiveHeader(&ArchiveHeader{
			FirstHeaderSize: arjMinFirstHeaderSize,
			Name:            "allowed-before-output.arj",
			Comment:         "allowed-before-output-comment",
		}); err != nil {
			t.Fatalf("SetArchiveHeader before output: %v", err)
		}

		if _, err := fw.Write([]byte("payload")); err != nil {
			t.Fatalf("Write(gated): %v", err)
		}
		if err := fw.(io.Closer).Close(); err != nil {
			t.Fatalf("Close(gated): %v", err)
		}
		if err := mw.SetArchiveName("blocked.arj"); err == nil || !strings.Contains(err.Error(), "called after archive output started") {
			t.Fatalf("SetArchiveName error = %v, want after archive output started", err)
		}
		if err := mw.SetComment("blocked-comment"); err == nil || !strings.Contains(err.Error(), "called after archive output started") {
			t.Fatalf("SetComment error = %v, want after archive output started", err)
		}
		if err := mw.SetArchiveHeader(&ArchiveHeader{FirstHeaderSize: arjMinFirstHeaderSize}); err == nil || !strings.Contains(err.Error(), "called after archive output started") {
			t.Fatalf("SetArchiveHeader error = %v, want after archive output started", err)
		}
		if err := mw.Close(); err != nil {
			t.Fatalf("Close writer: %v", err)
		}
	})

	t.Run("createRaw gates setters only after close commits output", func(t *testing.T) {
		archivePath := filepath.Join(t.TempDir(), "setter-gated-raw.arj")
		mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 16 << 10})
		if err != nil {
			t.Fatalf("NewMultiVolumeWriter: %v", err)
		}

		payload := []byte("raw-payload")
		fw, err := mw.CreateRaw(&FileHeader{
			Name:               "raw.bin",
			Method:             Store,
			CRC32:              crc32.ChecksumIEEE(payload),
			CompressedSize64:   uint64(len(payload)),
			UncompressedSize64: uint64(len(payload)),
		})
		if err != nil {
			t.Fatalf("CreateRaw: %v", err)
		}
		if _, err := fw.Write(payload); err != nil {
			t.Fatalf("Write raw: %v", err)
		}
		if err := mw.SetArchiveName("raw-allowed-before-close.arj"); err != nil {
			t.Fatalf("SetArchiveName before raw close: %v", err)
		}
		if err := fw.(io.Closer).Close(); err != nil {
			t.Fatalf("Close raw: %v", err)
		}
		if err := mw.SetArchiveName("raw-blocked.arj"); err == nil || !strings.Contains(err.Error(), "called after archive output started") {
			t.Fatalf("SetArchiveName after raw close error = %v, want after archive output started", err)
		}
		if err := mw.Close(); err != nil {
			t.Fatalf("Close writer: %v", err)
		}
	})
}

func TestMultiVolumeWriterEmptyCompressedEntryRoundTrip(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "empty-compressed.arj")
	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 32 << 10})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}

	methods := []uint16{Method1, Method2, Method3, Method4}
	for _, method := range methods {
		fw, err := mw.CreateHeader(&FileHeader{
			Name:   fmt.Sprintf("m%d-empty.bin", method),
			Method: method,
		})
		if err != nil {
			t.Fatalf("CreateHeader(method=%d): %v", method, err)
		}
		if err := fw.(io.Closer).Close(); err != nil {
			t.Fatalf("Close empty entry(method=%d): %v", method, err)
		}
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := OpenMultiReader(archivePath)
	if err != nil {
		t.Fatalf("OpenMultiReader: %v", err)
	}
	defer r.Close()
	if got, want := len(r.File), len(methods); got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}

	for i, method := range methods {
		if got, want := r.File[i].Method, method; got != want {
			t.Fatalf("file[%d] method = %d, want %d", i, got, want)
		}
		rc, err := r.File[i].Open()
		if err != nil {
			t.Fatalf("file[%d].Open: %v", i, err)
		}
		got, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			t.Fatalf("file[%d].ReadAll: %v", i, err)
		}
		if len(got) != 0 {
			t.Fatalf("file[%d] payload len = %d, want 0", i, len(got))
		}
	}
}

func TestPatchMainVolumeFlagMalformedAndSuccess(t *testing.T) {
	t.Run("malformed signature", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "bad-signature.bin")
		if err := os.WriteFile(path, []byte{0x00, 0x00, 0x00, 0x00}, 0o600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatalf("OpenFile: %v", err)
		}
		defer f.Close()

		if err := patchMainVolumeFlag(f, true); !errors.Is(err, ErrFormat) {
			t.Fatalf("patchMainVolumeFlag error = %v, want %v", err, ErrFormat)
		}
	})

	t.Run("malformed basic size", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "bad-size.bin")
		if err := os.WriteFile(path, []byte{arjHeaderID1, arjHeaderID2, 1, 0}, 0o600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatalf("OpenFile: %v", err)
		}
		defer f.Close()

		if err := patchMainVolumeFlag(f, true); !errors.Is(err, ErrFormat) {
			t.Fatalf("patchMainVolumeFlag error = %v, want %v", err, ErrFormat)
		}
	})

	t.Run("toggle success", func(t *testing.T) {
		archive := buildSingleStoreArchive(t, "flag.bin", []byte("payload"))
		path := filepath.Join(t.TempDir(), "toggle.arj")
		if err := os.WriteFile(path, archive, 0o600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatalf("OpenFile: %v", err)
		}
		defer f.Close()

		if err := patchMainVolumeFlag(f, true); err != nil {
			t.Fatalf("patchMainVolumeFlag(set=true): %v", err)
		}
		assertMainHeaderVolumeFlag(t, path, true)

		if err := patchMainVolumeFlag(f, false); err != nil {
			t.Fatalf("patchMainVolumeFlag(set=false): %v", err)
		}
		assertMainHeaderVolumeFlag(t, path, false)
	})
}

func TestMultiVolumeWriterFlushAroundActiveWrites(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "flush-active.arj")
	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 16 << 10})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}

	fw, err := mw.Create("flush.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := fw.Write([]byte("flush-payload")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if err := mw.Flush(); err != nil {
		t.Fatalf("Flush while entry active: %v", err)
	}
	if got := len(mw.Parts()); got < 1 {
		t.Fatalf("parts len during active write = %d, want >= 1", got)
	}

	if err := fw.(io.Closer).Close(); err != nil {
		t.Fatalf("Close entry: %v", err)
	}
	if err := mw.Flush(); err != nil {
		t.Fatalf("Flush after entry close: %v", err)
	}
	if got, want := len(mw.Parts()), 1; got != want {
		t.Fatalf("parts len after entry close = %d, want %d", got, want)
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
	if got := mustReadFileEntry(t, r.File[0]); string(got) != "flush-payload" {
		t.Fatalf("payload = %q, want %q", got, "flush-payload")
	}
}

func TestMultiVolumeWriterMaxCompressedChunkBoundedFallbackProbeCount(t *testing.T) {
	const (
		plainLen = 20000
		maxComp  = 10
	)

	plain := newEntryBuffer(uint64(plainLen), bufferScopeMultiEntryPlain)
	defer plain.Close()
	if _, err := plain.Write(bytes.Repeat([]byte{'x'}, plainLen)); err != nil {
		t.Fatalf("plain.Write: %v", err)
	}

	probeCalls := 0
	comp := func(out io.Writer) (io.WriteCloser, error) {
		probeCalls++
		return &sawtoothProbeCompressor{
			out:        out,
			fullLength: plainLen,
		}, nil
	}

	w := &MultiVolumeWriter{}
	n, compressed, err := w.maxCompressedChunkBufferedWithCompressor(
		Method1,
		plain,
		0,
		plainLen,
		maxComp,
		comp,
		DefaultMaxMethod14InputBufferSize,
	)
	if err != nil {
		t.Fatalf("maxCompressedChunkBufferedWithCompressor: %v", err)
	}
	if n == 0 {
		t.Fatalf("selected chunk = %d, want > 0", n)
	}
	if int64(len(compressed)) > maxComp {
		t.Fatalf("compressed len = %d, want <= %d", len(compressed), maxComp)
	}
	if probeCalls > 2000 {
		t.Fatalf("probe calls = %d, want <= 2000", probeCalls)
	}
}

func TestMultiVolumeWriterMaxCompressedChunkBoundedFallbackSparseFit(t *testing.T) {
	const (
		customMethod uint16 = 255
		plainLen            = 5000
		maxComp      int64  = 10
		fitN                = 2500
	)

	mw := &MultiVolumeWriter{}
	mw.RegisterCompressor(customMethod, func(out io.Writer) (io.WriteCloser, error) {
		return &sparseFitProbeCompressor{
			out:        out,
			defaultLen: 20,
			sizeByN: map[int]int{
				fitN: 6,
			},
		}, nil
	})

	plain := bytes.Repeat([]byte{'x'}, plainLen)
	n, compressed, err := mw.maxCompressedChunk(customMethod, plain, maxComp)
	if err != nil {
		t.Fatalf("maxCompressedChunk: %v", err)
	}
	if got, want := n, fitN; got != want {
		t.Fatalf("selected chunk = %d, want %d", got, want)
	}
	if got, want := len(compressed), 6; got != want {
		t.Fatalf("compressed len = %d, want %d", got, want)
	}
}

func TestMultiVolumeWriterSelectSegmentDataCompressedReturnsProbeCRC(t *testing.T) {
	payload := bytes.Repeat([]byte("compressed-crc-probe-"), 128)
	plain := newEntryBuffer(uint64(len(payload)), bufferScopeMultiEntryPlain)
	defer plain.Close()
	if _, err := plain.Write(payload); err != nil {
		t.Fatalf("plain.Write: %v", err)
	}

	entry := &multiVolumeFileWriter{
		h: &FileHeader{
			Name:            "probe.bin",
			Method:          251,
			FirstHeaderSize: arjMinFirstHeaderSize,
		},
		plain:              plain,
		compressor:         taggedSnapshotCompressor('P'),
		method14InputLimit: DefaultMaxMethod14InputBufferSize,
	}

	w := &MultiVolumeWriter{}
	n, comp, crc, crcKnown, err := w.selectSegmentData(entry, 0, false, 1<<20)
	if err != nil {
		t.Fatalf("selectSegmentData: %v", err)
	}
	if n <= 0 {
		t.Fatalf("selected chunk = %d, want > 0", n)
	}
	if len(comp) == 0 {
		t.Fatalf("compressed payload len = %d, want > 0", len(comp))
	}
	if !crcKnown {
		t.Fatalf("crcKnown = %t, want true", crcKnown)
	}
	if got, want := crc, crc32.ChecksumIEEE(payload[:n]); got != want {
		t.Fatalf("chunk CRC = %#08x, want %#08x", got, want)
	}
}

func TestMultiVolumeWriterSelectSegmentDataStoreLeavesCRCUnknown(t *testing.T) {
	payload := bytes.Repeat([]byte("store-crc-fallback-"), 64)
	plain := newEntryBuffer(uint64(len(payload)), bufferScopeMultiEntryPlain)
	defer plain.Close()
	if _, err := plain.Write(payload); err != nil {
		t.Fatalf("plain.Write: %v", err)
	}

	entry := &multiVolumeFileWriter{
		h: &FileHeader{
			Name:            "store.bin",
			Method:          Store,
			FirstHeaderSize: arjMinFirstHeaderSize,
		},
		plain: plain,
	}

	w := &MultiVolumeWriter{}
	n, comp, crc, crcKnown, err := w.selectSegmentData(entry, 0, false, 1<<20)
	if err != nil {
		t.Fatalf("selectSegmentData: %v", err)
	}
	if n <= 0 {
		t.Fatalf("selected chunk = %d, want > 0", n)
	}
	if comp != nil {
		t.Fatalf("compressed payload = %v, want nil", comp)
	}
	if crcKnown {
		t.Fatalf("crcKnown = %t, want false", crcKnown)
	}
	if crc != 0 {
		t.Fatalf("chunk CRC = %#08x, want 0", crc)
	}
}

func assertMainHeaderVolumeFlag(t *testing.T, path string, wantSet bool) {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", path, err)
	}
	r, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("NewReader(%s): %v", path, err)
	}
	gotSet := r.ArchiveHeader.Flags&FlagVolume != 0
	if gotSet != wantSet {
		t.Fatalf("main FlagVolume = %t, want %t", gotSet, wantSet)
	}
}

func mustReadRawFileEntry(t *testing.T, f *File) []byte {
	t.Helper()

	rc, err := f.OpenRaw()
	if err != nil {
		t.Fatalf("OpenRaw(%s): %v", f.Name, err)
	}
	if closer, ok := rc.(io.Closer); ok {
		defer closer.Close()
	}
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll(raw %s): %v", f.Name, err)
	}
	return data
}

type taggedCompressorWriter struct {
	out io.Writer
	tag byte
	n   int
}

func (w *taggedCompressorWriter) Write(p []byte) (int, error) {
	w.n += len(p)
	return len(p), nil
}

func (w *taggedCompressorWriter) Close() error {
	_, err := w.out.Write([]byte{w.tag, byte(w.n)})
	return err
}

func taggedSnapshotCompressor(tag byte) Compressor {
	return func(out io.Writer) (io.WriteCloser, error) {
		return &taggedCompressorWriter{out: out, tag: tag}, nil
	}
}

type sawtoothProbeCompressor struct {
	out        io.Writer
	fullLength int
	written    int
}

func (w *sawtoothProbeCompressor) Write(p []byte) (int, error) {
	w.written += len(p)
	return len(p), nil
}

func (w *sawtoothProbeCompressor) Close() error {
	outLen := 20
	if w.written != w.fullLength && w.written%16 == 0 {
		outLen = 5
	}
	_, err := w.out.Write(make([]byte, outLen))
	return err
}

type sparseFitProbeCompressor struct {
	out        io.Writer
	written    int
	defaultLen int
	sizeByN    map[int]int
}

func (w *sparseFitProbeCompressor) Write(p []byte) (int, error) {
	w.written += len(p)
	return len(p), nil
}

func (w *sparseFitProbeCompressor) Close() error {
	outLen := w.defaultLen
	if override, ok := w.sizeByN[w.written]; ok {
		outLen = override
	}
	if outLen <= 0 {
		return nil
	}
	_, err := w.out.Write(make([]byte, outLen))
	return err
}

type openFailMapFS struct {
	fstest.MapFS
	failName string
	failErr  error
}

func (f openFailMapFS) Open(name string) (fs.File, error) {
	if name == f.failName {
		return nil, f.failErr
	}
	return f.MapFS.Open(name)
}

type readFailMapFS struct {
	fstest.MapFS
	failName string
	failErr  error
}

func (f readFailMapFS) Open(name string) (fs.File, error) {
	base, err := f.MapFS.Open(name)
	if err != nil {
		return nil, err
	}
	if name != f.failName {
		return base, nil
	}
	return &failAfterFirstReadFile{
		File: base,
		err:  f.failErr,
	}, nil
}

type failAfterFirstReadFile struct {
	fs.File
	err    error
	failed bool
}

func (f *failAfterFirstReadFile) Read(p []byte) (int, error) {
	if f.failed {
		return 0, f.err
	}
	f.failed = true
	n, _ := f.File.Read(p)
	if n > 0 {
		return n, f.err
	}
	return 0, f.err
}
