package arj

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExtractOptionsZeroValuesUseDefaults(t *testing.T) {
	got := normalizeExtractOptions(ExtractOptions{})
	if got.MaxFiles != DefaultExtractMaxFiles {
		t.Fatalf("MaxFiles = %d, want %d", got.MaxFiles, DefaultExtractMaxFiles)
	}
	if got.MaxTotalBytes != DefaultExtractMaxTotalBytes {
		t.Fatalf("MaxTotalBytes = %d, want %d", got.MaxTotalBytes, DefaultExtractMaxTotalBytes)
	}
	if got.MaxFileBytes != DefaultExtractMaxFileBytes {
		t.Fatalf("MaxFileBytes = %d, want %d", got.MaxFileBytes, DefaultExtractMaxFileBytes)
	}
	if got.Strict {
		t.Fatalf("Strict = true, want false")
	}
}

func TestUnlimitedExtractOptionsSentinelBehavior(t *testing.T) {
	opts := UnlimitedExtractOptions()
	if opts.MaxFiles != ExtractUnlimitedFiles {
		t.Fatalf("MaxFiles = %d, want %d", opts.MaxFiles, ExtractUnlimitedFiles)
	}
	if opts.MaxTotalBytes != ExtractUnlimitedBytes {
		t.Fatalf("MaxTotalBytes = %d, want %d", opts.MaxTotalBytes, ExtractUnlimitedBytes)
	}
	if opts.MaxFileBytes != ExtractUnlimitedBytes {
		t.Fatalf("MaxFileBytes = %d, want %d", opts.MaxFileBytes, ExtractUnlimitedBytes)
	}
	if err := validateExtractOptions(opts); err != nil {
		t.Fatalf("validateExtractOptions(UnlimitedExtractOptions()) = %v, want nil", err)
	}

	normalized := normalizeExtractOptions(opts)
	if normalized.MaxFiles != 0 {
		t.Fatalf("normalized MaxFiles = %d, want 0", normalized.MaxFiles)
	}
	if normalized.MaxTotalBytes != 0 {
		t.Fatalf("normalized MaxTotalBytes = %d, want 0", normalized.MaxTotalBytes)
	}
	if normalized.MaxFileBytes != 0 {
		t.Fatalf("normalized MaxFileBytes = %d, want 0", normalized.MaxFileBytes)
	}
}

func TestExtractOptionsRejectInvalidNegativeValues(t *testing.T) {
	tests := []struct {
		name       string
		opts       ExtractOptions
		wantSubstr string
	}{
		{
			name:       "max files invalid",
			opts:       ExtractOptions{MaxFiles: -2},
			wantSubstr: "MaxFiles must be >= 0 or ExtractUnlimitedFiles",
		},
		{
			name:       "max total bytes invalid",
			opts:       ExtractOptions{MaxTotalBytes: -2},
			wantSubstr: "MaxTotalBytes must be >= 0 or ExtractUnlimitedBytes",
		},
		{
			name:       "max file bytes invalid",
			opts:       ExtractOptions{MaxFileBytes: -2},
			wantSubstr: "MaxFileBytes must be >= 0 or ExtractUnlimitedBytes",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateExtractOptions(tc.opts)
			if err == nil {
				t.Fatalf("validateExtractOptions(%+v) = nil, want error", tc.opts)
			}
			if !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Fatalf("validateExtractOptions(%+v) error = %q, want substring %q", tc.opts, err, tc.wantSubstr)
			}
		})
	}
}

func TestExtractAllPreOpenQuotaCheckBeforeDecompressorPath(t *testing.T) {
	payload := []byte("pre-open-quota")
	hdr := &FileHeader{
		Name:               "oversized.bin",
		Method:             250, // deliberately unregistered decompressor
		CRC32:              crc32.ChecksumIEEE(payload),
		CompressedSize64:   uint64(len(payload)),
		UncompressedSize64: uint64(len(payload)),
	}
	archive := buildSingleRawArchive(t, hdr, payload)

	r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	out := filepath.Join(t.TempDir(), "out")

	err = r.ExtractAllWithOptions(out, ExtractOptions{MaxFileBytes: int64(len(payload) - 1)})
	if err == nil {
		t.Fatalf("ExtractAllWithOptions error = nil, want quota error")
	}
	if !strings.Contains(err.Error(), "max file bytes exceeded") {
		t.Fatalf("ExtractAllWithOptions error = %v, want max file bytes exceeded", err)
	}
	if errors.Is(err, ErrAlgorithm) {
		t.Fatalf("ExtractAllWithOptions error unexpectedly matched %v", ErrAlgorithm)
	}

	if _, statErr := os.Stat(filepath.Join(out, "oversized.bin")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("oversized output exists or stat failed: %v", statErr)
	}
}

func TestExtractAllPropagatesEntryCloseErrorAfterSuccessfulCopy(t *testing.T) {
	payload := []byte("close-error-payload")
	archive := writeSingleFileArchive(t, &FileHeader{
		Name:   "close-error.bin",
		Method: Store,
	}, string(payload))

	r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	wantCloseErr := errors.New("decompressor close failed")
	r.RegisterDecompressor(Store, func(in io.Reader) io.ReadCloser {
		return &closeErrReadCloser{
			Reader:   in,
			closeErr: wantCloseErr,
		}
	})

	out := filepath.Join(t.TempDir(), "out")
	err = r.ExtractAll(out)
	if !errors.Is(err, wantCloseErr) {
		t.Fatalf("ExtractAll error = %v, want %v", err, wantCloseErr)
	}
}

func TestNewReaderWithOptionsMaxEntriesBoundaryInclusive(t *testing.T) {
	buildArchive := func(entryCount int) []byte {
		t.Helper()
		var archive bytes.Buffer
		w := NewWriter(&archive)
		for i := 0; i < entryCount; i++ {
			fw, err := w.Create(fmt.Sprintf("f-%d.txt", i))
			if err != nil {
				t.Fatalf("Create(%d): %v", i, err)
			}
			if _, err := fw.Write([]byte("x")); err != nil {
				t.Fatalf("Write(%d): %v", i, err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close writer: %v", err)
		}
		return append([]byte(nil), archive.Bytes()...)
	}

	const limit = 1
	atLimit := buildArchive(limit)
	r, err := NewReaderWithOptions(
		bytes.NewReader(atLimit),
		int64(len(atLimit)),
		ReaderOptions{ParserLimits: ParserLimits{MaxEntries: limit}},
	)
	if err != nil {
		t.Fatalf("NewReaderWithOptions at limit: %v", err)
	}
	if got := len(r.File); got != limit {
		t.Fatalf("entry count at limit = %d, want %d", got, limit)
	}

	overLimit := buildArchive(limit + 1)
	_, err = NewReaderWithOptions(
		bytes.NewReader(overLimit),
		int64(len(overLimit)),
		ReaderOptions{ParserLimits: ParserLimits{MaxEntries: limit}},
	)
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("NewReaderWithOptions over limit error = %v, want %v", err, ErrFormat)
	}
}

func TestNewReaderWithOptionsRejectsNilReaderAt(t *testing.T) {
	_, err := NewReaderWithOptions(nil, 2, ReaderOptions{})
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("NewReaderWithOptions(nil, ...) error = %v, want %v", err, ErrFormat)
	}
}

func TestNewReaderWithOptionsMaxExtendedHeaderCountLimit(t *testing.T) {
	archive := writeSingleFileArchive(t, &FileHeader{
		Name:                 "ext-count.bin",
		Method:               Store,
		LocalExtendedHeaders: [][]byte{[]byte("one"), []byte("two")},
	}, "payload")

	_, err := NewReaderWithOptions(
		bytes.NewReader(archive),
		int64(len(archive)),
		ReaderOptions{ParserLimits: ParserLimits{MaxExtendedHeaders: 1}},
	)
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("NewReaderWithOptions error = %v, want %v", err, ErrFormat)
	}
	if err == nil {
		t.Fatalf("NewReaderWithOptions error = nil, want non-nil")
	}
}

func TestNewReaderWithOptionsMaxExtendedHeaderBytesLimit(t *testing.T) {
	archive := writeSingleFileArchive(t, &FileHeader{
		Name:                 "ext-bytes.bin",
		Method:               Store,
		LocalExtendedHeaders: [][]byte{[]byte("abcd"), []byte("efgh")},
	}, "payload")

	_, err := NewReaderWithOptions(
		bytes.NewReader(archive),
		int64(len(archive)),
		ReaderOptions{
			ParserLimits: ParserLimits{
				MaxExtendedHeaders:     4,
				MaxExtendedHeaderBytes: 7,
			},
		},
	)
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("NewReaderWithOptions error = %v, want %v", err, ErrFormat)
	}
	if err == nil {
		t.Fatalf("NewReaderWithOptions error = nil, want non-nil")
	}
}

func TestFileOpenRejectsInvalidOpenState(t *testing.T) {
	var zero File
	if _, err := zero.Open(); !errors.Is(err, ErrFormat) {
		t.Fatalf("zero File.Open error = %v, want %v", err, ErrFormat)
	}
	if _, err := zero.OpenWithPassword("pw"); !errors.Is(err, ErrFormat) {
		t.Fatalf("zero File.OpenWithPassword error = %v, want %v", err, ErrFormat)
	}
	if _, err := zero.OpenRaw(); !errors.Is(err, ErrFormat) {
		t.Fatalf("zero File.OpenRaw error = %v, want %v", err, ErrFormat)
	}
	if _, err := zero.DataOffset(); !errors.Is(err, ErrFormat) {
		t.Fatalf("zero File.DataOffset error = %v, want %v", err, ErrFormat)
	}

	manual := &File{FileHeader: FileHeader{Name: "manual.txt"}}
	if _, err := manual.Open(); !errors.Is(err, ErrFormat) {
		t.Fatalf("manual File.Open error = %v, want %v", err, ErrFormat)
	}
	if _, err := manual.OpenWithPassword("pw"); !errors.Is(err, ErrFormat) {
		t.Fatalf("manual File.OpenWithPassword error = %v, want %v", err, ErrFormat)
	}
	if _, err := manual.OpenRaw(); !errors.Is(err, ErrFormat) {
		t.Fatalf("manual File.OpenRaw error = %v, want %v", err, ErrFormat)
	}
	if _, err := manual.DataOffset(); !errors.Is(err, ErrFormat) {
		t.Fatalf("manual File.DataOffset error = %v, want %v", err, ErrFormat)
	}

	var nilFile *File
	if _, err := nilFile.Open(); !errors.Is(err, ErrFormat) {
		t.Fatalf("nil File.Open error = %v, want %v", err, ErrFormat)
	}
	if _, err := nilFile.OpenRaw(); !errors.Is(err, ErrFormat) {
		t.Fatalf("nil File.OpenRaw error = %v, want %v", err, ErrFormat)
	}
	if _, err := nilFile.DataOffset(); !errors.Is(err, ErrFormat) {
		t.Fatalf("nil File.DataOffset error = %v, want %v", err, ErrFormat)
	}
}

func TestReaderOptionValidationSentinels(t *testing.T) {
	tests := []struct {
		name string
		size int64
		opts ReaderOptions
		want error
	}{
		{
			name: "negative size",
			size: -1,
			want: ErrInvalidReaderSize,
		},
		{
			name: "max entries",
			size: 0,
			opts: ReaderOptions{ParserLimits: ParserLimits{MaxEntries: -1}},
			want: ErrInvalidParserMaxEntries,
		},
		{
			name: "max extended headers",
			size: 0,
			opts: ReaderOptions{ParserLimits: ParserLimits{MaxExtendedHeaders: -1}},
			want: ErrInvalidParserMaxExtendedHeaders,
		},
		{
			name: "max extended header bytes",
			size: 0,
			opts: ReaderOptions{ParserLimits: ParserLimits{MaxExtendedHeaderBytes: -1}},
			want: ErrInvalidParserMaxExtendedHeaderBytes,
		},
		{
			name: "main header probe budget",
			size: 0,
			opts: ReaderOptions{MainHeaderProbeBudget: -1},
			want: ErrInvalidMainHeaderProbeBudget,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewReaderWithOptions(bytes.NewReader(nil), tc.size, tc.opts)
			if !errors.Is(err, tc.want) {
				t.Fatalf("NewReaderWithOptions error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestReadCloserCloseScrubsReaderPassword(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "scrub.arj")
	archive := buildSingleStoreArchive(t, "scrub.txt", []byte("payload"))
	if err := os.WriteFile(path, archive, 0o600); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}

	rc, err := OpenReader(path)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	rc.SetPassword("top-secret")
	if len(rc.password) == 0 {
		t.Fatal("password unexpectedly empty after SetPassword")
	}
	sensitive := rc.password[:len(rc.password):len(rc.password)]

	if err := rc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if rc.password != nil {
		t.Fatalf("password slice = %v, want nil", rc.password)
	}
	for i, b := range sensitive {
		if b != 0 {
			t.Fatalf("scrubbed byte %d = %d, want 0", i, b)
		}
	}
}

func TestOffsetBoundsHelpersRejectOverflow(t *testing.T) {
	if !fitsRange(10, 6, 4) {
		t.Fatalf("fitsRange valid range = false, want true")
	}
	if fitsRange(math.MaxInt64, math.MaxInt64-1, 4) {
		t.Fatalf("fitsRange overflow range = true, want false")
	}
	if fitsRange(10, 11, 0) {
		t.Fatalf("fitsRange out-of-range offset = true, want false")
	}

	if next, ok := advanceOffsetWithinSize(8, 2, 10); !ok || next != 10 {
		t.Fatalf("advanceOffsetWithinSize valid = (%d, %v), want (10, true)", next, ok)
	}
	if _, ok := advanceOffsetWithinSize(math.MaxInt64-1, 4, math.MaxInt64); ok {
		t.Fatalf("advanceOffsetWithinSize overflow unexpectedly succeeded")
	}
	if _, ok := advanceOffsetWithinSize(11, 1, 10); ok {
		t.Fatalf("advanceOffsetWithinSize out-of-range unexpectedly succeeded")
	}
}

func TestMultiSegmentReadCloserPropagatesSegmentCloseError(t *testing.T) {
	wantErr := errors.New("segment close failed")
	rc := &multiSegmentReadCloser{
		segments: []fileSegment{{}},
		openPart: func(fileSegment) (io.ReadCloser, error) {
			return &eofOnceCloseErrReader{payload: []byte("x"), closeErr: wantErr}, nil
		},
	}

	buf := make([]byte, 8)
	n, err := rc.Read(buf)
	if got, want := n, 1; got != want {
		t.Fatalf("Read bytes = %d, want %d", got, want)
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("Read error = %v, want %v", err, wantErr)
	}

	n, err = rc.Read(buf)
	if got, want := n, 0; got != want {
		t.Fatalf("second Read bytes = %d, want %d", got, want)
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("second Read error = %v, want %v", err, wantErr)
	}

	if err := rc.Close(); !errors.Is(err, wantErr) {
		t.Fatalf("Close error = %v, want %v", err, wantErr)
	}
}

func TestChecksumReaderDetectsTruncationReadCountMismatch(t *testing.T) {
	archive := buildSingleStoreArchive(t, "short.bin", []byte("abc"))
	mutated := mutateFirstLocalHeaderUncompressedSize(t, archive, 4)

	r, err := NewReader(bytes.NewReader(mutated), int64(len(mutated)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if got, want := len(r.File), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}

	rc, err := r.File[0].Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("ReadAll error = %v, want %v", err, ErrFormat)
	}
	if string(got) != "abc" {
		t.Fatalf("ReadAll payload = %q, want %q", got, "abc")
	}
}

func TestChecksumReaderCloseVerifiesExactSizeRead(t *testing.T) {
	archive := buildSingleStoreArchive(t, "corrupt.bin", []byte("abcd"))
	r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("NewReader original: %v", err)
	}
	off, err := r.File[0].DataOffset()
	if err != nil {
		t.Fatalf("DataOffset: %v", err)
	}

	corrupt := append([]byte(nil), archive...)
	corrupt[off] ^= 0xff

	r, err = NewReader(bytes.NewReader(corrupt), int64(len(corrupt)))
	if err != nil {
		t.Fatalf("NewReader corrupt: %v", err)
	}
	rc, err := r.File[0].Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	buf := make([]byte, r.File[0].UncompressedSize64)
	if n, err := io.ReadFull(rc, buf); err != nil || n != len(buf) {
		t.Fatalf("ReadFull = (%d, %v), want (%d, nil)", n, err, len(buf))
	}
	if err := rc.Close(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("Close error = %v, want %v", err, ErrChecksum)
	}
}

func TestChecksumReaderCloseDetectsOverlongExactSizeRead(t *testing.T) {
	payload := []byte("abcd")
	archive := buildSingleStoreArchive(t, "overlong.bin", payload)
	mutated := mutateFirstLocalHeaderUncompressedSizeAndCRC(t, archive, 3, crc32.ChecksumIEEE(payload[:3]))

	r, err := NewReader(bytes.NewReader(mutated), int64(len(mutated)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	rc, err := r.File[0].Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	buf := make([]byte, r.File[0].UncompressedSize64)
	if n, err := io.ReadFull(rc, buf); err != nil || n != len(buf) {
		t.Fatalf("ReadFull = (%d, %v), want (%d, nil)", n, err, len(buf))
	}
	if string(buf) != string(payload[:3]) {
		t.Fatalf("ReadFull payload = %q, want %q", buf, payload[:3])
	}
	if err := rc.Close(); !errors.Is(err, ErrFormat) {
		t.Fatalf("Close error = %v, want %v", err, ErrFormat)
	}
}

func mutateFirstLocalHeaderUncompressedSizeAndCRC(t *testing.T, archive []byte, uncompressedSize, crc uint32) []byte {
	t.Helper()

	out := append([]byte(nil), archive...)
	basicStart, basicEnd := firstLocalHeaderBasicBounds(t, out)
	binary.LittleEndian.PutUint32(out[basicStart+16:basicStart+20], uncompressedSize)
	binary.LittleEndian.PutUint32(out[basicStart+20:basicStart+24], crc)
	binary.LittleEndian.PutUint32(out[basicEnd:basicEnd+4], crc32.ChecksumIEEE(out[basicStart:basicEnd]))
	return out
}

func buildSingleRawArchive(t *testing.T, hdr *FileHeader, payload []byte) []byte {
	t.Helper()

	var buf bytes.Buffer
	w := NewWriter(&buf)
	fw, err := w.CreateRaw(hdr)
	if err != nil {
		t.Fatalf("CreateRaw: %v", err)
	}
	if _, err := fw.Write(payload); err != nil {
		t.Fatalf("Write raw payload: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	return append([]byte(nil), buf.Bytes()...)
}

type eofOnceCloseErrReader struct {
	payload  []byte
	readOnce bool
	closeErr error
}

func (r *eofOnceCloseErrReader) Read(p []byte) (int, error) {
	if r.readOnce {
		return 0, io.EOF
	}
	r.readOnce = true
	return copy(p, r.payload), io.EOF
}

func (r *eofOnceCloseErrReader) Close() error {
	return r.closeErr
}

type closeErrReadCloser struct {
	io.Reader
	closeErr error
}

func (r *closeErrReadCloser) Close() error {
	return r.closeErr
}
