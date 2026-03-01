package arj

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"reflect"
	"testing"
	"time"
)

func TestSpecHardeningExtendedHeaderRoundTrip(t *testing.T) {
	wantExt := [][]byte{
		[]byte("extra-one"),
		{0x00, 0x7f, 0x80, 0xff},
	}

	src := writeSingleFileArchive(t, &FileHeader{
		Name:                 "ext.bin",
		Method:               Store,
		ArchiverVersion:      19,
		MinVersion:           7,
		FilespecPos:          9,
		HostData:             0x1234,
		LocalExtendedHeaders: wantExt,
	}, "payload-ext")

	r1, err := NewReader(bytes.NewReader(src), int64(len(src)))
	if err != nil {
		t.Fatalf("NewReader src: %v", err)
	}
	if got, want := len(r1.File), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	if !reflect.DeepEqual(r1.File[0].LocalExtendedHeaders, wantExt) {
		t.Fatalf("src ext headers = %v, want %v", r1.File[0].LocalExtendedHeaders, wantExt)
	}

	dst := copySingleFileArchive(t, r1.File[0])
	r2, err := NewReader(bytes.NewReader(dst), int64(len(dst)))
	if err != nil {
		t.Fatalf("NewReader dst: %v", err)
	}
	if !reflect.DeepEqual(r2.File[0].LocalExtendedHeaders, wantExt) {
		t.Fatalf("dst ext headers = %v, want %v", r2.File[0].LocalExtendedHeaders, wantExt)
	}
	assertCopySemanticsEqual(t, &r2.File[0].FileHeader, &r1.File[0].FileHeader)

	rc, err := r2.File[0].Open()
	if err != nil {
		t.Fatalf("Open copied file: %v", err)
	}
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll copied file: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close copied file: %v", err)
	}
	if got, want := string(data), "payload-ext"; got != want {
		t.Fatalf("copied payload = %q, want %q", got, want)
	}
}

func TestSpecHardeningLocalFixedFieldRoundTrip(t *testing.T) {
	h := &FileHeader{
		Name:            "fixed.bin",
		Method:          Store,
		HostOS:          9,
		Flags:           0x24,
		FirstHeaderSize: arjMinFirstHeaderSize + 4,
		ArchiverVersion: 27,
		MinVersion:      8,
		FilespecPos:     11,
		HostData:        0xbeef,
	}
	h.firstHeaderExtra = []byte{0xde, 0xad, 0xbe, 0xef}
	h.SetMode(0o640)
	h.SetModTime(time.Date(2025, 1, 2, 3, 4, 6, 0, time.UTC))

	src := writeSingleFileArchive(t, h, "fixed-payload")
	r1, err := NewReader(bytes.NewReader(src), int64(len(src)))
	if err != nil {
		t.Fatalf("NewReader src: %v", err)
	}
	if got, want := len(r1.File), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	parsed := &r1.File[0].FileHeader
	assertLocalFixedFieldsEqual(t, parsed, h)

	dst := copySingleFileArchive(t, r1.File[0])
	r2, err := NewReader(bytes.NewReader(dst), int64(len(dst)))
	if err != nil {
		t.Fatalf("NewReader dst: %v", err)
	}
	roundTrip := &r2.File[0].FileHeader
	assertLocalFixedFieldsEqual(t, roundTrip, parsed)
	assertCopySemanticsEqual(t, roundTrip, parsed)
}

func TestSpecHardeningLocalFixedFieldRoundTripRawCopy(t *testing.T) {
	payload := []byte("raw-copy-payload")
	h := &FileHeader{
		Name:               "raw-copy.bin",
		Method:             Store,
		HostOS:             7,
		Flags:              0x28,
		CRC32:              crc32.ChecksumIEEE(payload),
		CompressedSize64:   uint64(len(payload)),
		UncompressedSize64: uint64(len(payload)),
	}
	h.SetMode(0o640)
	h.SetModTime(time.Date(2025, 1, 2, 3, 4, 6, 0, time.UTC))

	var src bytes.Buffer
	sw := NewWriter(&src)
	fw, err := sw.CreateRaw(h)
	if err != nil {
		t.Fatalf("CreateRaw source: %v", err)
	}
	if _, err := fw.Write(payload); err != nil {
		t.Fatalf("Write raw source: %v", err)
	}
	if err := sw.Close(); err != nil {
		t.Fatalf("Close source writer: %v", err)
	}

	sr, err := NewReader(bytes.NewReader(src.Bytes()), int64(src.Len()))
	if err != nil {
		t.Fatalf("NewReader source: %v", err)
	}
	srcHeader := &sr.File[0].FileHeader
	if srcHeader.FirstHeaderSize != arjMinFirstHeaderSize {
		t.Fatalf("FirstHeaderSize = %d, want %d", srcHeader.FirstHeaderSize, arjMinFirstHeaderSize)
	}
	if srcHeader.ArchiverVersion != arjVersionCurrent {
		t.Fatalf("ArchiverVersion = %d, want %d", srcHeader.ArchiverVersion, arjVersionCurrent)
	}
	if srcHeader.MinVersion != arjVersionNeeded {
		t.Fatalf("MinVersion = %d, want %d", srcHeader.MinVersion, arjVersionNeeded)
	}
	if srcHeader.FilespecPos != 0 {
		t.Fatalf("FilespecPos = %d, want 0", srcHeader.FilespecPos)
	}
	if srcHeader.HostData != 0 {
		t.Fatalf("HostData = %d, want 0", srcHeader.HostData)
	}

	var dst bytes.Buffer
	dw := NewWriter(&dst)
	if err := dw.Copy(sr.File[0]); err != nil {
		t.Fatalf("Copy raw entry: %v", err)
	}
	if err := dw.Close(); err != nil {
		t.Fatalf("Close destination writer: %v", err)
	}

	dr, err := NewReader(bytes.NewReader(dst.Bytes()), int64(dst.Len()))
	if err != nil {
		t.Fatalf("NewReader destination: %v", err)
	}
	dstHeader := &dr.File[0].FileHeader
	assertLocalFixedFieldsEqual(t, dstHeader, srcHeader)
	assertCopySemanticsEqual(t, dstHeader, srcHeader)

	rc, err := dr.File[0].Open()
	if err != nil {
		t.Fatalf("Open copied raw file: %v", err)
	}
	gotPayload, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll copied raw file: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close copied raw file: %v", err)
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Fatalf("copied raw payload = %q, want %q", gotPayload, payload)
	}
}

func TestSpecHardeningMalformedExtendedHeaderCRCRejects(t *testing.T) {
	src := writeSingleFileArchive(t, &FileHeader{
		Name:                 "bad-ext.bin",
		Method:               Store,
		LocalExtendedHeaders: [][]byte{{1, 2, 3, 4}},
	}, "bad")

	corrupt := corruptFirstLocalExtendedHeaderCRC(t, src)
	_, err := NewReader(bytes.NewReader(corrupt), int64(len(corrupt)))
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("NewReader error = %v, want %v", err, ErrFormat)
	}
}

func TestSpecHardeningCopyPreservesLocalHeaderMetadataAndExt(t *testing.T) {
	payload := []byte("copy-meta")
	ext := [][]byte{{0x10, 0x20}, []byte("copy-ext")}
	srcHeader := &FileHeader{
		Name:                 "copy-meta.bin",
		Method:               Store,
		CRC32:                crc32.ChecksumIEEE(payload),
		CompressedSize64:     uint64(len(payload)),
		UncompressedSize64:   uint64(len(payload)),
		HostOS:               8,
		Flags:                0x34,
		FirstHeaderSize:      arjMinFirstHeaderSize + 3,
		ArchiverVersion:      23,
		MinVersion:           6,
		FilespecPos:          5,
		HostData:             0x4242,
		LocalExtendedHeaders: ext,
	}
	srcHeader.firstHeaderExtra = []byte{0xaa, 0xbb, 0xcc}
	srcHeader.SetMode(0o640)
	srcHeader.SetModTime(time.Date(2025, 2, 3, 4, 5, 6, 0, time.UTC))

	var src bytes.Buffer
	sw := NewWriter(&src)
	wr, err := sw.CreateRaw(srcHeader)
	if err != nil {
		t.Fatalf("CreateRaw source: %v", err)
	}
	if _, err := wr.Write(payload); err != nil {
		t.Fatalf("Write source payload: %v", err)
	}
	if err := sw.Close(); err != nil {
		t.Fatalf("Close source writer: %v", err)
	}

	sr, err := NewReader(bytes.NewReader(src.Bytes()), int64(src.Len()))
	if err != nil {
		t.Fatalf("NewReader source: %v", err)
	}

	var dst bytes.Buffer
	dw := NewWriter(&dst)
	if err := dw.Copy(sr.File[0]); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if err := dw.Close(); err != nil {
		t.Fatalf("Close destination writer: %v", err)
	}

	dr, err := NewReader(bytes.NewReader(dst.Bytes()), int64(dst.Len()))
	if err != nil {
		t.Fatalf("NewReader destination: %v", err)
	}
	assertLocalFixedFieldsEqual(t, &dr.File[0].FileHeader, &sr.File[0].FileHeader)
	assertCopySemanticsEqual(t, &dr.File[0].FileHeader, &sr.File[0].FileHeader)
	if !reflect.DeepEqual(dr.File[0].LocalExtendedHeaders, ext) {
		t.Fatalf("ext headers after copy = %v, want %v", dr.File[0].LocalExtendedHeaders, ext)
	}
}

func writeSingleFileArchive(t *testing.T, h *FileHeader, payload string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf)
	fw, err := w.CreateHeader(h)
	if err != nil {
		t.Fatalf("CreateHeader: %v", err)
	}
	if _, err := io.WriteString(fw, payload); err != nil {
		t.Fatalf("WriteString: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	return append([]byte(nil), buf.Bytes()...)
}

func copySingleFileArchive(t *testing.T, src *File) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf)
	fw, err := w.CreateHeader(&src.FileHeader)
	if err != nil {
		t.Fatalf("CreateHeader dst: %v", err)
	}
	rc, err := src.Open()
	if err != nil {
		t.Fatalf("Open src: %v", err)
	}
	if _, err := io.Copy(fw, rc); err != nil {
		t.Fatalf("Copy payload: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close src reader: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close dst writer: %v", err)
	}
	return append([]byte(nil), buf.Bytes()...)
}

func assertLocalFixedFieldsEqual(t *testing.T, got, want *FileHeader) {
	t.Helper()
	if got.FirstHeaderSize != want.FirstHeaderSize {
		t.Fatalf("FirstHeaderSize = %d, want %d", got.FirstHeaderSize, want.FirstHeaderSize)
	}
	if got.ArchiverVersion != want.ArchiverVersion {
		t.Fatalf("ArchiverVersion = %d, want %d", got.ArchiverVersion, want.ArchiverVersion)
	}
	if got.MinVersion != want.MinVersion {
		t.Fatalf("MinVersion = %d, want %d", got.MinVersion, want.MinVersion)
	}
	if got.FilespecPos != want.FilespecPos {
		t.Fatalf("FilespecPos = %d, want %d", got.FilespecPos, want.FilespecPos)
	}
	if got.HostData != want.HostData {
		t.Fatalf("HostData = %d, want %d", got.HostData, want.HostData)
	}
	if got.HostOS != want.HostOS {
		t.Fatalf("HostOS = %d, want %d", got.HostOS, want.HostOS)
	}
	if got.Flags != want.Flags {
		t.Fatalf("Flags = %d, want %d", got.Flags, want.Flags)
	}
	if got.PasswordModifier != want.PasswordModifier {
		t.Fatalf("PasswordModifier = %d, want %d", got.PasswordModifier, want.PasswordModifier)
	}
	if !bytes.Equal(got.firstHeaderExtra, want.firstHeaderExtra) {
		t.Fatalf("firstHeaderExtra = %v, want %v", got.firstHeaderExtra, want.firstHeaderExtra)
	}
}

func assertCopySemanticsEqual(t *testing.T, got, want *FileHeader) {
	t.Helper()
	if got.Method != want.Method {
		t.Fatalf("Method = %d, want %d", got.Method, want.Method)
	}
	if got.CRC32 != want.CRC32 {
		t.Fatalf("CRC32 = 0x%08x, want 0x%08x", got.CRC32, want.CRC32)
	}
	if got.CompressedSize64 != want.CompressedSize64 {
		t.Fatalf("CompressedSize64 = %d, want %d", got.CompressedSize64, want.CompressedSize64)
	}
	if got.UncompressedSize64 != want.UncompressedSize64 {
		t.Fatalf("UncompressedSize64 = %d, want %d", got.UncompressedSize64, want.UncompressedSize64)
	}
	if gotMode, wantMode := got.Mode(), want.Mode(); gotMode != wantMode {
		t.Fatalf("Mode = %v, want %v", gotMode, wantMode)
	}
	if gotMod, wantMod := got.ModTime(), want.ModTime(); !gotMod.Equal(wantMod) {
		t.Fatalf("ModTime = %v, want %v", gotMod, wantMod)
	}
}

func corruptFirstLocalExtendedHeaderCRC(t *testing.T, archive []byte) []byte {
	t.Helper()
	out := append([]byte(nil), archive...)
	off := skipHeaderBlock(t, out, 0)

	if off+4 > len(out) {
		t.Fatalf("missing local header at offset %d", off)
	}
	if out[off] != arjHeaderID1 || out[off+1] != arjHeaderID2 {
		t.Fatalf("invalid local header signature at offset %d", off)
	}

	basicSize := int(binary.LittleEndian.Uint16(out[off+2 : off+4]))
	off += 4
	if basicSize <= 0 {
		t.Fatalf("local basic header size = %d, want > 0", basicSize)
	}
	if off+basicSize+4 > len(out) {
		t.Fatalf("local basic header exceeds archive bounds")
	}
	off += basicSize + 4

	if off+2 > len(out) {
		t.Fatalf("missing local extended header size")
	}
	extSize := int(binary.LittleEndian.Uint16(out[off : off+2]))
	if extSize == 0 {
		t.Fatal("expected at least one local extended header")
	}
	crcOff := off + 2 + extSize
	if crcOff+4 > len(out) {
		t.Fatalf("local extended header CRC exceeds archive bounds")
	}
	out[crcOff] ^= 0xff
	return out
}

func skipHeaderBlock(t *testing.T, data []byte, off int) int {
	t.Helper()
	if off+4 > len(data) {
		t.Fatalf("header prefix out of range at offset %d", off)
	}
	if data[off] != arjHeaderID1 || data[off+1] != arjHeaderID2 {
		t.Fatalf("invalid header signature at offset %d", off)
	}
	basicSize := int(binary.LittleEndian.Uint16(data[off+2 : off+4]))
	off += 4
	if basicSize == 0 {
		return off
	}
	if off+basicSize+4 > len(data) {
		t.Fatalf("basic header exceeds archive bounds")
	}
	off += basicSize + 4
	for {
		if off+2 > len(data) {
			t.Fatalf("extended header size out of range")
		}
		extSize := int(binary.LittleEndian.Uint16(data[off : off+2]))
		off += 2
		if extSize == 0 {
			return off
		}
		if off+extSize+4 > len(data) {
			t.Fatalf("extended header exceeds archive bounds")
		}
		off += extSize + 4
	}
}
