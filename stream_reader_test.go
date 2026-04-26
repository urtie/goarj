package arj

import (
	"bytes"
	"encoding/binary"
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

func injectFirstLocalEntryPayload(t *testing.T, archive, payload []byte) []byte {
	t.Helper()

	out := append([]byte(nil), archive...)
	size := int64(len(out))
	r := bytes.NewReader(out)

	_, _, localOff, err := readHeaderBlock(r, size, 0)
	if err != nil {
		t.Fatalf("readHeaderBlock(main): %v", err)
	}
	basic, _, localDataOff, err := readHeaderBlock(r, size, localOff)
	if err != nil {
		t.Fatalf("readHeaderBlock(local): %v", err)
	}
	if len(basic) < 24 {
		t.Fatalf("local header basic size = %d, want >= 24", len(basic))
	}
	if len(payload) > int(^uint32(0)) {
		t.Fatalf("payload length = %d exceeds uint32", len(payload))
	}

	mutBasic := append([]byte(nil), basic...)
	binary.LittleEndian.PutUint32(mutBasic[12:16], uint32(len(payload)))
	binary.LittleEndian.PutUint32(mutBasic[16:20], uint32(len(payload)))
	binary.LittleEndian.PutUint32(mutBasic[20:24], crc32.ChecksumIEEE(payload))

	basicStart := int(localOff) + 4
	copy(out[basicStart:basicStart+len(mutBasic)], mutBasic)
	basicCRC := crc32.ChecksumIEEE(mutBasic)
	basicCRCStart := basicStart + len(mutBasic)
	binary.LittleEndian.PutUint32(out[basicCRCStart:basicCRCStart+4], basicCRC)

	dataOff := int(localDataOff)
	out = append(out[:dataOff], append(append([]byte(nil), payload...), out[dataOff:]...)...)
	return out
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

func TestStreamReaderClearPasswordScrubsPassword(t *testing.T) {
	archive := buildStreamArchive(t, []streamTestEntry{
		{
			header:  FileHeader{Name: "scrub.txt", Method: Store},
			payload: []byte("payload"),
		},
	})
	sr, err := NewStreamReader(bytes.NewReader(archive))
	if err != nil {
		t.Fatalf("NewStreamReader: %v", err)
	}
	sr.SetPassword("top-secret")
	if len(sr.password) == 0 {
		t.Fatal("password unexpectedly empty after SetPassword")
	}
	sensitive := sr.password[:len(sr.password):len(sr.password)]

	sr.ClearPassword()
	assertStreamReaderPasswordScrubbed(t, sr, sensitive)

	_, rc, err := sr.Next()
	if err != nil {
		t.Fatalf("Next after ClearPassword: %v", err)
	}
	if got, err := io.ReadAll(rc); err != nil || string(got) != "payload" {
		t.Fatalf("ReadAll after ClearPassword = (%q, %v), want (%q, nil)", got, err, "payload")
	}
}

func TestStreamReaderCloseScrubsPassword(t *testing.T) {
	archive := buildStreamArchive(t, []streamTestEntry{
		{
			header:  FileHeader{Name: "scrub.txt", Method: Store},
			payload: []byte("payload"),
		},
	})
	sr, err := NewStreamReader(bytes.NewReader(archive))
	if err != nil {
		t.Fatalf("NewStreamReader: %v", err)
	}
	sr.SetPassword("top-secret")
	if len(sr.password) == 0 {
		t.Fatal("password unexpectedly empty after SetPassword")
	}
	sensitive := sr.password[:len(sr.password):len(sr.password)]

	if _, _, err := sr.Next(); err != nil {
		t.Fatalf("Next: %v", err)
	}
	if sr.current == nil {
		t.Fatal("current entry unexpectedly nil before Close")
	}
	if err := sr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	assertStreamReaderPasswordScrubbed(t, sr, sensitive)
	if sr.current != nil {
		t.Fatalf("current entry = %v, want nil after Close", sr.current)
	}
	if _, _, err := sr.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("Next after Close error = %v, want %v", err, io.EOF)
	}
}

func TestStreamReaderCloseAbortsOpenEntryWithoutDraining(t *testing.T) {
	archive := buildStreamArchive(t, []streamTestEntry{
		{
			header:  FileHeader{Name: "corrupt-tail.txt", Method: Store},
			payload: []byte("payload"),
		},
	})
	dataOff := firstStreamLocalDataOffset(t, archive)
	corrupt := append([]byte(nil), archive...)
	corrupt[dataOff+1] ^= 0xff

	sr, err := NewStreamReader(bytes.NewReader(corrupt))
	if err != nil {
		t.Fatalf("NewStreamReader: %v", err)
	}
	_, rc, err := sr.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	buf := make([]byte, 1)
	if n, err := rc.Read(buf); n != 1 || err != nil {
		t.Fatalf("Read first byte = (%d, %v), want (1, nil)", n, err)
	}
	if err := sr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func firstStreamLocalDataOffset(t *testing.T, archive []byte) int {
	t.Helper()

	size := int64(len(archive))
	r := bytes.NewReader(archive)
	_, _, localOff, err := readHeaderBlock(r, size, 0)
	if err != nil {
		t.Fatalf("readHeaderBlock(main): %v", err)
	}
	_, _, localDataOff, err := readHeaderBlock(r, size, localOff)
	if err != nil {
		t.Fatalf("readHeaderBlock(local): %v", err)
	}
	return int(localDataOff)
}

func assertStreamReaderPasswordScrubbed(t *testing.T, sr *StreamReader, sensitive []byte) {
	t.Helper()
	if sr.password != nil {
		t.Fatalf("password slice = %v, want nil", sr.password)
	}
	for i, b := range sensitive {
		if b != 0 {
			t.Fatalf("scrubbed byte %d = %d, want 0", i, b)
		}
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

func TestNewStreamReaderWithOptionsMaxHeaderScanBytes(t *testing.T) {
	archive := buildStreamArchive(t, []streamTestEntry{
		{
			header:  FileHeader{Name: "file.txt", Method: Store},
			payload: []byte("ok"),
		},
	})
	prefix := bytes.Repeat([]byte{0x7f}, 128)
	stream := append(append([]byte(nil), prefix...), archive...)

	_, err := NewStreamReaderWithOptions(bytes.NewReader(stream), StreamReaderOptions{
		MaxHeaderScanBytes: int64(len(prefix) - 1),
	})
	if !errors.Is(err, ErrStreamHeaderScanLimitExceeded) {
		t.Fatalf("NewStreamReaderWithOptions error = %v, want %v", err, ErrStreamHeaderScanLimitExceeded)
	}

	sr, err := NewStreamReaderWithOptions(bytes.NewReader(stream), StreamReaderOptions{
		MaxHeaderScanBytes: int64(len(prefix)),
	})
	if err != nil {
		t.Fatalf("NewStreamReaderWithOptions: %v", err)
	}
	if got, want := sr.BaseOffset(), int64(len(prefix)); got != want {
		t.Fatalf("BaseOffset = %d, want %d", got, want)
	}
}

func TestNewStreamReaderWithOptionsRejectsNegativeMaxHeaderScanBytes(t *testing.T) {
	_, err := NewStreamReaderWithOptions(bytes.NewReader(nil), StreamReaderOptions{
		MaxHeaderScanBytes: -1,
	})
	if !errors.Is(err, ErrInvalidStreamHeaderScanLimit) {
		t.Fatalf("NewStreamReaderWithOptions error = %v, want %v", err, ErrInvalidStreamHeaderScanLimit)
	}
}

func TestNewStreamReaderSkipsInvalidSignatureCandidates(t *testing.T) {
	archive := buildStreamArchive(t, []streamTestEntry{
		{
			header:  FileHeader{Name: "file.txt", Method: Store},
			payload: []byte("ok"),
		},
	})
	prefix := []byte{
		0x00, 0x11,
		arjHeaderID1, arjHeaderID2, 0x01, 0x00, // invalid basic header size
		0x33, 0x44,
		arjHeaderID1, arjHeaderID2, 0x00, 0x00, // invalid end-of-headers block
		0x55, 0x66,
	}
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
	if err := rc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if string(got) != "ok" {
		t.Fatalf("payload = %q, want %q", got, "ok")
	}
}

func TestNewStreamReaderSkipsCorruptMainHeaderCandidate(t *testing.T) {
	archive := buildStreamArchive(t, []streamTestEntry{
		{
			header:  FileHeader{Name: "file.txt", Method: Store},
			payload: []byte("ok"),
		},
	})

	const decoyFirstHeaderSize = arjMinFirstHeaderSize
	decoyBasic := make([]byte, decoyFirstHeaderSize+2)
	decoyBasic[0] = decoyFirstHeaderSize
	decoyBasic[6] = arjFileTypeMain
	decoyBasic[decoyFirstHeaderSize] = 0
	decoyBasic[decoyFirstHeaderSize+1] = 0
	decoySize := len(decoyBasic)
	decoy := make([]byte, 0, 2+2+decoySize+4)
	decoy = append(decoy, arjHeaderID1, arjHeaderID2, byte(decoySize), byte(decoySize>>8))
	decoy = append(decoy, decoyBasic...)
	// CRC intentionally incorrect: this must be rejected by the scanner.
	decoy = append(decoy, 0x00, 0x00, 0x00, 0x00)

	stream := append(append([]byte(nil), decoy...), archive...)

	sr, err := NewStreamReader(bytes.NewReader(stream))
	if err != nil {
		t.Fatalf("NewStreamReader: %v", err)
	}
	if got, want := sr.BaseOffset(), int64(len(decoy)); got != want {
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
	if err := rc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if string(got) != "ok" {
		t.Fatalf("payload = %q, want %q", got, "ok")
	}
}

func TestStreamReaderCustomDecompressorReturningNilYieldsErrAlgorithm(t *testing.T) {
	archive := buildStreamArchive(t, []streamTestEntry{
		{
			header:  FileHeader{Name: "nil-decompressor.bin", Method: Store},
			payload: []byte("payload"),
		},
	})

	sr, err := NewStreamReader(bytes.NewReader(archive))
	if err != nil {
		t.Fatalf("NewStreamReader: %v", err)
	}
	sr.RegisterDecompressor(Store, func(io.Reader) io.ReadCloser { return nil })

	_, rc, err := sr.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if rc == nil {
		t.Fatal("Next reader = nil, want non-nil")
	}
	defer rc.Close()

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("ReadAll panic = %v, want nil panic", recovered)
		}
	}()

	_, err = io.ReadAll(rc)
	if !errors.Is(err, ErrAlgorithm) {
		t.Fatalf("ReadAll error = %v, want %v", err, ErrAlgorithm)
	}
}

func TestStreamReaderOpenErrorIsStickyAcrossReads(t *testing.T) {
	payload := []byte("unsupported")
	hdr := &FileHeader{
		Name:               "unsupported.bin",
		Method:             250, // deliberately unregistered decompressor
		CRC32:              crc32.ChecksumIEEE(payload),
		CompressedSize64:   uint64(len(payload)),
		UncompressedSize64: uint64(len(payload)),
	}
	archive := buildSingleRawArchive(t, hdr, payload)

	sr, err := NewStreamReader(bytes.NewReader(archive))
	if err != nil {
		t.Fatalf("NewStreamReader: %v", err)
	}
	_, rc, err := sr.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if rc == nil {
		t.Fatal("Next reader = nil, want non-nil")
	}
	defer rc.Close()

	buf := make([]byte, 1)
	if n, err := rc.Read(buf); n != 0 || !errors.Is(err, ErrAlgorithm) {
		t.Fatalf("first Read = (%d, %v), want (0, %v)", n, err, ErrAlgorithm)
	}
	if n, err := rc.Read(buf); n != 0 || !errors.Is(err, ErrAlgorithm) {
		t.Fatalf("second Read = (%d, %v), want (0, %v)", n, err, ErrAlgorithm)
	}
}

func TestStreamReaderMethod14DecodeLimitsSetAfterNextApplyOnFirstRead(t *testing.T) {
	payload := bytes.Repeat([]byte("method14-limit-check-"), 32)
	archive := buildStreamArchive(t, []streamTestEntry{
		{
			header:  FileHeader{Name: "limit.bin", Method: Method1},
			payload: payload,
		},
	})

	sr, err := NewStreamReader(bytes.NewReader(archive))
	if err != nil {
		t.Fatalf("NewStreamReader: %v", err)
	}

	_, rc, err := sr.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if rc == nil {
		t.Fatal("Next reader = nil, want non-nil")
	}
	defer rc.Close()

	sr.SetMethod14DecodeLimits(Method14DecodeLimits{
		MaxCompressedSize:   1,
		MaxUncompressedSize: 1,
	})

	_, err = io.ReadAll(rc)
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("ReadAll error = %v, want %v", err, ErrFormat)
	}
}

func TestNewStreamReaderRejectsTruncatedTailSignatureCandidate(t *testing.T) {
	stream := []byte{0x11, 0x22, arjHeaderID1, arjHeaderID2}
	_, err := NewStreamReader(bytes.NewReader(stream))
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("NewStreamReader error = %v, want %v", err, ErrFormat)
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

func TestStreamReaderNextRejectsCorruptLocalExtendedHeaderCRC(t *testing.T) {
	archive := buildStreamArchive(t, []streamTestEntry{
		{
			header: FileHeader{
				Name:                 "crc.bin",
				Method:               Store,
				LocalExtendedHeaders: [][]byte{{0x01, 0x02, 0x03, 0x04}},
			},
			payload: []byte("payload"),
		},
	})
	corrupt := corruptFirstLocalExtendedHeaderCRC(t, archive)

	sr, err := NewStreamReader(bytes.NewReader(corrupt))
	if err != nil {
		t.Fatalf("NewStreamReader: %v", err)
	}

	if _, _, err := sr.Next(); !errors.Is(err, ErrFormat) {
		t.Fatalf("Next error = %v, want %v", err, ErrFormat)
	}
}

func TestStreamReaderNextRejectsTruncatedLocalExtendedHeader(t *testing.T) {
	archive := buildStreamArchive(t, []streamTestEntry{
		{
			header: FileHeader{
				Name:                 "truncated.bin",
				Method:               Store,
				LocalExtendedHeaders: [][]byte{{0x10, 0x20, 0x30, 0x40}},
			},
			payload: []byte("payload"),
		},
	})
	truncated := append([]byte(nil), archive...)
	localOff := skipHeaderBlock(t, truncated, 0)

	if localOff+4 > len(truncated) {
		t.Fatalf("missing local header at offset %d", localOff)
	}
	basicSize := int(binary.LittleEndian.Uint16(truncated[localOff+2 : localOff+4]))
	extStart := localOff + 4 + basicSize + 4
	if extStart+2 > len(truncated) {
		t.Fatalf("missing local extended-header size at offset %d", extStart)
	}
	extSize := int(binary.LittleEndian.Uint16(truncated[extStart : extStart+2]))
	if extSize == 0 {
		t.Fatal("expected local extended header")
	}

	// Keep only part of the first extended-header CRC to force malformed input.
	cut := extStart + 2 + extSize + 1
	if cut >= len(truncated) {
		t.Fatalf("invalid truncate cut offset %d for archive size %d", cut, len(truncated))
	}
	truncated = truncated[:cut]

	sr, err := NewStreamReader(bytes.NewReader(truncated))
	if err != nil {
		t.Fatalf("NewStreamReader: %v", err)
	}

	if _, _, err := sr.Next(); !errors.Is(err, ErrFormat) {
		t.Fatalf("Next error = %v, want %v", err, ErrFormat)
	}
}

func TestStreamReaderWithOptionsMaxExtendedHeaderCount(t *testing.T) {
	archive := buildStreamArchive(t, []streamTestEntry{
		{
			header: FileHeader{
				Name:                 "count-limit.bin",
				Method:               Store,
				LocalExtendedHeaders: [][]byte{[]byte("one"), []byte("two")},
			},
			payload: []byte("payload"),
		},
	})

	sr, err := NewStreamReaderWithOptions(
		bytes.NewReader(archive),
		StreamReaderOptions{
			ParserLimits: ParserLimits{
				MaxExtendedHeaders: 1,
			},
		},
	)
	if err != nil {
		t.Fatalf("NewStreamReaderWithOptions: %v", err)
	}

	_, _, err = sr.Next()
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("Next error = %v, want %v", err, ErrFormat)
	}
	if err == nil || !strings.Contains(err.Error(), "max extended headers exceeded") {
		t.Fatalf("Next error = %v, want max extended headers exceeded", err)
	}
}

func TestStreamReaderWithOptionsMaxExtendedHeaderBytes(t *testing.T) {
	archive := buildStreamArchive(t, []streamTestEntry{
		{
			header: FileHeader{
				Name:                 "bytes-limit.bin",
				Method:               Store,
				LocalExtendedHeaders: [][]byte{[]byte("abcd"), []byte("efgh")},
			},
			payload: []byte("payload"),
		},
	})

	sr, err := NewStreamReaderWithOptions(
		bytes.NewReader(archive),
		StreamReaderOptions{
			ParserLimits: ParserLimits{
				MaxExtendedHeaders:     4,
				MaxExtendedHeaderBytes: 7,
			},
		},
	)
	if err != nil {
		t.Fatalf("NewStreamReaderWithOptions: %v", err)
	}

	_, _, err = sr.Next()
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("Next error = %v, want %v", err, ErrFormat)
	}
	if err == nil || !strings.Contains(err.Error(), "max extended header bytes exceeded") {
		t.Fatalf("Next error = %v, want max extended header bytes exceeded", err)
	}
}

func TestStreamReaderNextRejectsMalformedSecondHeaderSignature(t *testing.T) {
	archive := buildStreamArchive(t, []streamTestEntry{
		{
			header:  FileHeader{Name: "first.txt", Method: Store},
			payload: []byte("first payload"),
		},
		{
			header:  FileHeader{Name: "second.txt", Method: Store},
			payload: []byte("second payload"),
		},
	})
	corrupt := append([]byte(nil), archive...)
	secondOff := secondLocalHeaderOffset(t, corrupt)
	if secondOff+2 > len(corrupt) {
		t.Fatalf("second header prefix out of range at %d", secondOff)
	}
	corrupt[secondOff] ^= 0xff

	sr, err := NewStreamReader(bytes.NewReader(corrupt))
	if err != nil {
		t.Fatalf("NewStreamReader: %v", err)
	}

	h, rc, err := sr.Next()
	if err != nil {
		t.Fatalf("Next(first): %v", err)
	}
	if got, want := h.Name, "first.txt"; got != want {
		t.Fatalf("first name = %q, want %q", got, want)
	}
	gotFirst, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll(first): %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close(first): %v", err)
	}
	if got, want := string(gotFirst), "first payload"; got != want {
		t.Fatalf("first payload = %q, want %q", got, want)
	}

	if _, _, err := sr.Next(); !errors.Is(err, ErrFormat) {
		t.Fatalf("Next(second) error = %v, want %v", err, ErrFormat)
	}
}

func TestStreamReaderNextRejectsMalformedSecondHeaderCRC(t *testing.T) {
	archive := buildStreamArchive(t, []streamTestEntry{
		{
			header:  FileHeader{Name: "first.txt", Method: Store},
			payload: []byte("first payload"),
		},
		{
			header:  FileHeader{Name: "second.txt", Method: Store},
			payload: []byte("second payload"),
		},
	})
	corrupt := append([]byte(nil), archive...)
	secondOff := secondLocalHeaderOffset(t, corrupt)
	if secondOff+4 > len(corrupt) {
		t.Fatalf("second header prefix out of range at %d", secondOff)
	}
	basicSize := int(binary.LittleEndian.Uint16(corrupt[secondOff+2 : secondOff+4]))
	crcOff := secondOff + 4 + basicSize
	if basicSize < arjMinFirstHeaderSize || crcOff+4 > len(corrupt) {
		t.Fatalf("second header basic/CRC out of range")
	}
	corrupt[crcOff] ^= 0xff

	sr, err := NewStreamReader(bytes.NewReader(corrupt))
	if err != nil {
		t.Fatalf("NewStreamReader: %v", err)
	}

	h, rc, err := sr.Next()
	if err != nil {
		t.Fatalf("Next(first): %v", err)
	}
	if got, want := h.Name, "first.txt"; got != want {
		t.Fatalf("first name = %q, want %q", got, want)
	}
	gotFirst, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll(first): %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close(first): %v", err)
	}
	if got, want := string(gotFirst), "first payload"; got != want {
		t.Fatalf("first payload = %q, want %q", got, want)
	}

	if _, _, err := sr.Next(); !errors.Is(err, ErrFormat) {
		t.Fatalf("Next(second) error = %v, want %v", err, ErrFormat)
	}
}

func secondLocalHeaderOffset(t *testing.T, archive []byte) int {
	t.Helper()

	size := int64(len(archive))
	r := bytes.NewReader(archive)
	_, _, firstOff, err := readHeaderBlock(r, size, 0)
	if err != nil {
		t.Fatalf("readHeaderBlock(main): %v", err)
	}
	firstBasic, firstExt, firstDataOff, err := readHeaderBlock(r, size, firstOff)
	if err != nil {
		t.Fatalf("readHeaderBlock(first local): %v", err)
	}
	firstFile, err := parseLocalFileHeaderOwned(firstBasic, firstExt, nil)
	if err != nil {
		t.Fatalf("parseLocalFileHeaderOwned(first local): %v", err)
	}

	secondOff := firstDataOff + int64(firstFile.CompressedSize64)
	if secondOff > size {
		t.Fatalf("second header offset = %d, archive size = %d", secondOff, size)
	}
	return int(secondOff)
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

func TestExtractAllStreamDirectoryPayloadKeepsAlignment(t *testing.T) {
	baseArchive := buildStreamArchive(t, []streamTestEntry{
		{
			header: FileHeader{Name: "docs/", Method: Store, fileType: arjFileTypeDirectory},
		},
		{
			header:  FileHeader{Name: "docs/readme.txt", Method: Store},
			payload: []byte("stream extract"),
		},
	})
	archive := injectFirstLocalEntryPayload(t, baseArchive, []byte("dir-payload"))

	r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if got, want := len(r.File), 2; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	if !r.File[0].isDir() {
		t.Fatalf("first entry isDir = false, want true")
	}
	if r.File[0].CompressedSize64 == 0 {
		t.Fatalf("directory compressed size = 0, want > 0 for alignment regression coverage")
	}

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
