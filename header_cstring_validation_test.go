package arj

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"testing"
)

func TestNewReaderRejectsMainHeaderMissingArchiveNameTerminator(t *testing.T) {
	archive := buildArchiveForCStringValidation(t)
	corrupt := mutateMainHeaderCStringTerminator(t, archive, 0)

	_, err := NewReader(bytes.NewReader(corrupt), int64(len(corrupt)))
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("NewReader error = %v, want %v", err, ErrFormat)
	}
}

func TestNewReaderRejectsMainHeaderMissingCommentTerminator(t *testing.T) {
	archive := buildArchiveForCStringValidation(t)
	corrupt := mutateMainHeaderCStringTerminator(t, archive, 1)

	_, err := NewReader(bytes.NewReader(corrupt), int64(len(corrupt)))
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("NewReader error = %v, want %v", err, ErrFormat)
	}
}

func TestNewReaderRejectsLocalHeaderMissingFileNameTerminator(t *testing.T) {
	archive := buildArchiveForCStringValidation(t)
	corrupt := mutateFirstLocalHeaderCStringTerminator(t, archive, 0)

	_, err := NewReader(bytes.NewReader(corrupt), int64(len(corrupt)))
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("NewReader error = %v, want %v", err, ErrFormat)
	}
}

func TestNewReaderRejectsLocalHeaderMissingCommentTerminator(t *testing.T) {
	archive := buildArchiveForCStringValidation(t)
	corrupt := mutateFirstLocalHeaderCStringTerminator(t, archive, 1)

	_, err := NewReader(bytes.NewReader(corrupt), int64(len(corrupt)))
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("NewReader error = %v, want %v", err, ErrFormat)
	}
}

func buildArchiveForCStringValidation(t *testing.T) []byte {
	t.Helper()

	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.SetArchiveName("legacy-main.arj"); err != nil {
		t.Fatalf("SetArchiveName: %v", err)
	}
	if err := w.SetComment("legacy-main-comment"); err != nil {
		t.Fatalf("SetComment(main): %v", err)
	}
	fw, err := w.CreateHeader(&FileHeader{
		Name:    "legacy-file.txt",
		Comment: "legacy-file-comment",
		Method:  Store,
	})
	if err != nil {
		t.Fatalf("CreateHeader: %v", err)
	}
	if _, err := io.WriteString(fw, "payload"); err != nil {
		t.Fatalf("Write payload: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	return append([]byte(nil), buf.Bytes()...)
}

func mutateMainHeaderCStringTerminator(t *testing.T, archive []byte, index int) []byte {
	t.Helper()

	out := append([]byte(nil), archive...)
	off, err := findMainHeaderOffset(bytes.NewReader(out), int64(len(out)))
	if err != nil {
		t.Fatalf("findMainHeaderOffset: %v", err)
	}
	start := int(off)
	if start+4 > len(out) {
		t.Fatalf("main header prefix out of range at %d", start)
	}
	basicSize := int(binary.LittleEndian.Uint16(out[start+2 : start+4]))
	basicStart := start + 4
	basicEnd := basicStart + basicSize
	if basicSize < arjMinFirstHeaderSize || basicEnd+4 > len(out) {
		t.Fatalf("main header basic block out of range")
	}

	basic := append([]byte(nil), out[basicStart:basicEnd]...)
	mutateCStringTerminator(t, basic, index)
	copy(out[basicStart:basicEnd], basic)
	binary.LittleEndian.PutUint32(out[basicEnd:basicEnd+4], crc32.ChecksumIEEE(basic))
	return out
}

func mutateFirstLocalHeaderCStringTerminator(t *testing.T, archive []byte, index int) []byte {
	t.Helper()

	out := append([]byte(nil), archive...)
	localOff := skipHeaderBlock(t, out, 0)
	if localOff+4 > len(out) {
		t.Fatalf("local header prefix out of range at %d", localOff)
	}
	basicSize := int(binary.LittleEndian.Uint16(out[localOff+2 : localOff+4]))
	basicStart := localOff + 4
	basicEnd := basicStart + basicSize
	if basicSize < arjMinFirstHeaderSize || basicEnd+4 > len(out) {
		t.Fatalf("local header basic block out of range")
	}

	basic := append([]byte(nil), out[basicStart:basicEnd]...)
	mutateCStringTerminator(t, basic, index)
	copy(out[basicStart:basicEnd], basic)
	binary.LittleEndian.PutUint32(out[basicEnd:basicEnd+4], crc32.ChecksumIEEE(basic))
	return out
}

func mutateCStringTerminator(t *testing.T, basic []byte, index int) {
	t.Helper()

	if len(basic) < arjMinFirstHeaderSize {
		t.Fatalf("basic header len = %d, want >= %d", len(basic), arjMinFirstHeaderSize)
	}
	firstSize := int(basic[0])
	if firstSize < arjMinFirstHeaderSize || firstSize > len(basic) {
		t.Fatalf("first header size = %d, want [%d,%d]", firstSize, arjMinFirstHeaderSize, len(basic))
	}

	rest := basic[firstSize:]
	nameEnd := bytes.IndexByte(rest, 0)
	if nameEnd < 0 {
		t.Fatal("missing name terminator in source basic header")
	}
	if index == 0 {
		rest[nameEnd] = 'X'
		return
	}

	rest = rest[nameEnd+1:]
	commentEnd := bytes.IndexByte(rest, 0)
	if commentEnd < 0 {
		t.Fatal("missing comment terminator in source basic header")
	}
	if index == 1 {
		rest[commentEnd] = 'X'
		return
	}
	t.Fatalf("invalid C-string terminator index %d", index)
}
