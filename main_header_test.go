package arj

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"
)

func TestMainHeaderFixedFieldsRoundTrip(t *testing.T) {
	want := ArchiveHeader{
		FirstHeaderSize:      arjMinFirstHeaderSize + 4,
		ArchiverVersion:      23,
		MinVersion:           7,
		HostOS:               9,
		Flags:                0xa0,
		SecurityVersion:      2,
		FileType:             arjFileTypeMain,
		Reserved:             0x5c,
		Created:              time.Date(2025, 1, 2, 3, 4, 6, 0, time.UTC),
		Modified:             time.Date(2025, 1, 3, 4, 5, 8, 0, time.UTC),
		ArchiveSize:          0x11223344,
		SecurityEnvelopePos:  0x55667788,
		FilespecPos:          13,
		SecurityEnvelopeSize: 21,
		HostData:             0xbeef,
		Name:                 "fixed-main.arj",
		Comment:              "fixed-main-comment",
		FirstHeaderExtra:     []byte{0xde, 0xad, 0xbe, 0xef},
	}

	src := writeArchiveWithMainHeader(t, &want)
	r1, err := NewReader(bytes.NewReader(src), int64(len(src)))
	if err != nil {
		t.Fatalf("NewReader source: %v", err)
	}
	assertMainHeaderFixedFields(t, r1.ArchiveHeader, want)

	var dst bytes.Buffer
	w2 := NewWriter(&dst)
	if err := w2.SetArchiveHeader(&r1.ArchiveHeader); err != nil {
		t.Fatalf("SetArchiveHeader destination: %v", err)
	}
	fw, err := w2.Create("file.txt")
	if err != nil {
		t.Fatalf("Create destination: %v", err)
	}
	if _, err := io.WriteString(fw, "payload"); err != nil {
		t.Fatalf("Write destination payload: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("Close destination writer: %v", err)
	}

	r2, err := NewReader(bytes.NewReader(dst.Bytes()), int64(dst.Len()))
	if err != nil {
		t.Fatalf("NewReader destination: %v", err)
	}
	assertMainHeaderFixedFields(t, r2.ArchiveHeader, want)
}

func TestMainHeaderExtendedHeadersRoundTrip(t *testing.T) {
	wantExt := [][]byte{
		[]byte("main-ext-one"),
		{0x00, 0x7f, 0x80, 0xff},
	}
	hdr := ArchiveHeader{
		FirstHeaderSize:      arjMinFirstHeaderSize,
		ArchiverVersion:      19,
		MinVersion:           5,
		HostOS:               7,
		Flags:                0x04,
		SecurityVersion:      1,
		FileType:             arjFileTypeMain,
		Created:              time.Date(2025, 2, 2, 3, 4, 6, 0, time.UTC),
		Modified:             time.Date(2025, 2, 3, 4, 5, 8, 0, time.UTC),
		ArchiveSize:          0x01020304,
		SecurityEnvelopePos:  0x05060708,
		FilespecPos:          3,
		SecurityEnvelopeSize: 9,
		HostData:             0x4242,
		Name:                 "main-ext.arj",
		Comment:              "main-ext-comment",
		MainExtendedHeaders:  cloneMainExtendedHeaders(wantExt),
	}

	src := writeArchiveWithMainHeader(t, &hdr)
	r1, err := NewReader(bytes.NewReader(src), int64(len(src)))
	if err != nil {
		t.Fatalf("NewReader source: %v", err)
	}
	if !reflect.DeepEqual(r1.ArchiveHeader.MainExtendedHeaders, wantExt) {
		t.Fatalf("source main ext headers = %v, want %v", r1.ArchiveHeader.MainExtendedHeaders, wantExt)
	}
	_, rawExt1 := readMainHeaderBlock(t, src)
	if !reflect.DeepEqual(rawExt1, wantExt) {
		t.Fatalf("source raw main ext headers = %v, want %v", rawExt1, wantExt)
	}

	var dst bytes.Buffer
	w2 := NewWriter(&dst)
	if err := w2.SetArchiveHeader(&r1.ArchiveHeader); err != nil {
		t.Fatalf("SetArchiveHeader destination: %v", err)
	}
	fw, err := w2.Create("file.txt")
	if err != nil {
		t.Fatalf("Create destination: %v", err)
	}
	if _, err := io.WriteString(fw, "payload"); err != nil {
		t.Fatalf("Write destination payload: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("Close destination writer: %v", err)
	}

	r2, err := NewReader(bytes.NewReader(dst.Bytes()), int64(dst.Len()))
	if err != nil {
		t.Fatalf("NewReader destination: %v", err)
	}
	if !reflect.DeepEqual(r2.ArchiveHeader.MainExtendedHeaders, wantExt) {
		t.Fatalf("destination main ext headers = %v, want %v", r2.ArchiveHeader.MainExtendedHeaders, wantExt)
	}
	_, rawExt2 := readMainHeaderBlock(t, dst.Bytes())
	if !reflect.DeepEqual(rawExt2, wantExt) {
		t.Fatalf("destination raw main ext headers = %v, want %v", rawExt2, wantExt)
	}
}

func TestMainHeaderMalformedExtendedHeaderCRCRejects(t *testing.T) {
	hdr := ArchiveHeader{
		FirstHeaderSize:      arjMinFirstHeaderSize,
		ArchiverVersion:      24,
		MinVersion:           5,
		HostOS:               6,
		Flags:                0x30,
		SecurityVersion:      1,
		FileType:             arjFileTypeMain,
		Created:              time.Date(2025, 3, 2, 3, 4, 6, 0, time.UTC),
		Modified:             time.Date(2025, 3, 3, 4, 5, 8, 0, time.UTC),
		ArchiveSize:          0x11111111,
		SecurityEnvelopePos:  0x22222222,
		FilespecPos:          4,
		SecurityEnvelopeSize: 7,
		HostData:             0x1111,
		Name:                 "bad-main-ext.arj",
		Comment:              "bad-main-ext-comment",
		MainExtendedHeaders:  [][]byte{{1, 2, 3, 4}},
	}

	src := writeArchiveWithMainHeader(t, &hdr)
	corrupt := corruptFirstMainExtendedHeaderCRC(t, src)
	_, err := NewReader(bytes.NewReader(corrupt), int64(len(corrupt)))
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("NewReader error = %v, want %v", err, ErrFormat)
	}
}

func TestFindMainHeaderOffsetProbeBudgetExceeded(t *testing.T) {
	real := buildSingleStoreArchive(t, "budget.bin", []byte("payload"))
	prefix := signaturePlausibleMainHeaderNoise(64 << 10)

	container := append(append([]byte(nil), prefix...), real...)
	_, err := findMainHeaderOffsetWithBudget(
		bytes.NewReader(container),
		int64(len(container)),
		&mainHeaderProbeBudget{remaining: 128},
	)
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("findMainHeaderOffsetWithBudget error = %v, want %v", err, ErrFormat)
	}
}

func TestFindMainHeaderOffsetProbeBudgetIgnoresScanBytesWithoutCandidates(t *testing.T) {
	noise := bytes.Repeat([]byte{0x11}, 256<<10)
	rdr := &countingReaderAt{data: noise}
	const budget = int64(1024)

	_, err := findMainHeaderOffsetWithBudget(
		rdr,
		int64(len(noise)),
		&mainHeaderProbeBudget{remaining: budget},
	)
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("findMainHeaderOffsetWithBudget error = %v, want %v", err, ErrFormat)
	}
	if got, want := rdr.totalRead, int64(len(noise))+4; got != want {
		t.Fatalf("bytes read = %d, want %d", got, want)
	}
}

func TestFindMainHeaderOffsetFastPathRegularArchive(t *testing.T) {
	archive := buildSingleStoreArchive(t, "fast-path.bin", bytes.Repeat([]byte{'x'}, 4<<20))
	rdr := &countingReaderAt{data: archive}

	got, err := findMainHeaderOffsetWithBudget(
		rdr,
		int64(len(archive)),
		&mainHeaderProbeBudget{remaining: int64(len(archive)) * 2},
	)
	if err != nil {
		t.Fatalf("findMainHeaderOffsetWithBudget: %v", err)
	}
	if got != 0 {
		t.Fatalf("findMainHeaderOffsetWithBudget = %d, want 0", got)
	}
	if maxRead := int64(len(archive)) / 4; rdr.totalRead >= maxRead {
		t.Fatalf("bytes read = %d, want < %d", rdr.totalRead, maxRead)
	}
}

func TestFindMainHeaderOffsetPrefilterSkipsImpossibleCandidates(t *testing.T) {
	real := buildSingleStoreArchive(t, "budget-prefilter.bin", []byte("payload"))
	prefix := signatureDenseOffsetNoise(256 << 10)

	container := append(append([]byte(nil), prefix...), real...)
	want := int64(len(prefix))
	budget := int64(len(container)) + (8 << 10)
	got, err := findMainHeaderOffsetWithBudget(
		bytes.NewReader(container),
		int64(len(container)),
		&mainHeaderProbeBudget{remaining: budget},
	)
	if err != nil {
		t.Fatalf("findMainHeaderOffsetWithBudget: %v", err)
	}
	if got != want {
		t.Fatalf("findMainHeaderOffsetWithBudget = %d, want %d", got, want)
	}
}

func TestFindMainHeaderOffsetProbeBudgetAllowsValidEmbeddedArchive(t *testing.T) {
	real := buildSingleStoreArchive(t, "budget-ok.bin", []byte("payload"))
	prefix := signatureDenseOffsetNoise(64 << 10)

	container := append(append([]byte(nil), prefix...), real...)
	want := int64(len(prefix))
	got, err := findMainHeaderOffsetWithBudget(
		bytes.NewReader(container),
		int64(len(container)),
		&mainHeaderProbeBudget{remaining: int64(len(container)) * 64},
	)
	if err != nil {
		t.Fatalf("findMainHeaderOffsetWithBudget: %v", err)
	}
	if got != want {
		t.Fatalf("findMainHeaderOffsetWithBudget = %d, want %d", got, want)
	}
}

func TestNewMainHeaderProbeBudgetAppliesDefaultCap(t *testing.T) {
	size := DefaultMainHeaderProbeBudgetMax/mainHeaderProbeBudgetMultiplier + 1
	budget := newMainHeaderProbeBudget(size, 0)
	if budget.remaining != DefaultMainHeaderProbeBudgetMax {
		t.Fatalf("newMainHeaderProbeBudget remaining = %d, want %d", budget.remaining, DefaultMainHeaderProbeBudgetMax)
	}
}

func TestNewMainHeaderProbeBudgetUsesConfiguredValue(t *testing.T) {
	const configured = int64(4096)
	budget := newMainHeaderProbeBudget(1<<40, configured)
	if budget.remaining != configured {
		t.Fatalf("newMainHeaderProbeBudget configured remaining = %d, want %d", budget.remaining, configured)
	}
}

func TestNewReaderWithOptionsMainHeaderProbeBudget(t *testing.T) {
	real := buildSingleStoreArchive(t, "opts-budget.bin", []byte("payload"))
	prefix := signaturePlausibleMainHeaderNoise(32 << 10)

	container := append(append([]byte(nil), prefix...), real...)
	size := int64(len(container))

	_, err := NewReaderWithOptions(bytes.NewReader(container), size, ReaderOptions{
		MainHeaderProbeBudget: 128,
	})
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("NewReaderWithOptions low budget error = %v, want %v", err, ErrFormat)
	}

	r, err := NewReaderWithOptions(bytes.NewReader(container), size, ReaderOptions{
		MainHeaderProbeBudget: 1 << 20,
	})
	if err != nil {
		t.Fatalf("NewReaderWithOptions high budget: %v", err)
	}
	if got, want := r.BaseOffset(), int64(len(prefix)); got != want {
		t.Fatalf("BaseOffset = %d, want %d", got, want)
	}
}

func TestMainHeaderLegacyCommentAndArchiveName(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	const archiveName = "legacy-name.arj"
	const archiveComment = "legacy-comment"

	if err := w.SetArchiveName(archiveName); err != nil {
		t.Fatalf("SetArchiveName: %v", err)
	}
	if err := w.SetComment(archiveComment); err != nil {
		t.Fatalf("SetComment: %v", err)
	}
	fw, err := w.Create("legacy.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := io.WriteString(fw, "legacy-payload"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if got := r.Comment; got != archiveComment {
		t.Fatalf("reader comment = %q, want %q", got, archiveComment)
	}
	if got := r.ArchiveName; got != archiveName {
		t.Fatalf("reader archive name = %q, want %q", got, archiveName)
	}
	if got := r.ArchiveHeader.Comment; got != archiveComment {
		t.Fatalf("archive header comment = %q, want %q", got, archiveComment)
	}
	if got := r.ArchiveHeader.Name; got != archiveName {
		t.Fatalf("archive header name = %q, want %q", got, archiveName)
	}
}

func TestWriterSettersRemainUsableAfterRejectedCreateHeader(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	_, err := w.CreateHeader(&FileHeader{Name: "bad.bin", Method: 0x1ff})
	if !errors.Is(err, ErrAlgorithm) {
		t.Fatalf("CreateHeader error = %v, want %v", err, ErrAlgorithm)
	}
	if got := buf.Len(); got != 0 {
		t.Fatalf("buffer length after rejected CreateHeader = %d, want 0", got)
	}

	const archiveName = "post-reject-header.arj"
	const archiveComment = "post-reject-header-comment"
	if err := w.SetArchiveName(archiveName); err != nil {
		t.Fatalf("SetArchiveName after rejected CreateHeader: %v", err)
	}
	if err := w.SetComment(archiveComment); err != nil {
		t.Fatalf("SetComment after rejected CreateHeader: %v", err)
	}

	fw, err := w.Create("ok.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := io.WriteString(fw, "payload"); err != nil {
		t.Fatalf("Write payload: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if got := r.ArchiveName; got != archiveName {
		t.Fatalf("archive name = %q, want %q", got, archiveName)
	}
	if got := r.Comment; got != archiveComment {
		t.Fatalf("comment = %q, want %q", got, archiveComment)
	}
}

func TestWriterSettersRemainUsableAfterRejectedCreateRaw(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	_, err := w.CreateRaw(&FileHeader{Name: "bad-raw.bin", Method: 0x1ff})
	if !errors.Is(err, ErrAlgorithm) {
		t.Fatalf("CreateRaw error = %v, want %v", err, ErrAlgorithm)
	}
	if got := buf.Len(); got != 0 {
		t.Fatalf("buffer length after rejected CreateRaw = %d, want 0", got)
	}

	const archiveName = "post-reject-raw.arj"
	const archiveComment = "post-reject-raw-comment"
	if err := w.SetArchiveName(archiveName); err != nil {
		t.Fatalf("SetArchiveName after rejected CreateRaw: %v", err)
	}
	if err := w.SetComment(archiveComment); err != nil {
		t.Fatalf("SetComment after rejected CreateRaw: %v", err)
	}

	fw, err := w.Create("ok.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := io.WriteString(fw, "payload"); err != nil {
		t.Fatalf("Write payload: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if got := r.ArchiveName; got != archiveName {
		t.Fatalf("archive name = %q, want %q", got, archiveName)
	}
	if got := r.Comment; got != archiveComment {
		t.Fatalf("comment = %q, want %q", got, archiveComment)
	}
}

func writeArchiveWithMainHeader(t *testing.T, hdr *ArchiveHeader) []byte {
	t.Helper()

	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.SetArchiveHeader(hdr); err != nil {
		t.Fatalf("SetArchiveHeader: %v", err)
	}
	fw, err := w.Create("file.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := io.WriteString(fw, "payload"); err != nil {
		t.Fatalf("Write payload: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	return append([]byte(nil), buf.Bytes()...)
}

func assertMainHeaderFixedFields(t *testing.T, got, want ArchiveHeader) {
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
	if got.HostOS != want.HostOS {
		t.Fatalf("HostOS = %d, want %d", got.HostOS, want.HostOS)
	}
	if got.Flags != want.Flags {
		t.Fatalf("Flags = %d, want %d", got.Flags, want.Flags)
	}
	if got.SecurityVersion != want.SecurityVersion {
		t.Fatalf("SecurityVersion = %d, want %d", got.SecurityVersion, want.SecurityVersion)
	}
	if got.FileType != want.FileType {
		t.Fatalf("FileType = %d, want %d", got.FileType, want.FileType)
	}
	if got.Reserved != want.Reserved {
		t.Fatalf("Reserved = %d, want %d", got.Reserved, want.Reserved)
	}
	if !got.Created.Equal(want.Created) {
		t.Fatalf("Created = %v, want %v", got.Created, want.Created)
	}
	if !got.Modified.Equal(want.Modified) {
		t.Fatalf("Modified = %v, want %v", got.Modified, want.Modified)
	}
	if got.ArchiveSize != want.ArchiveSize {
		t.Fatalf("ArchiveSize = %d, want %d", got.ArchiveSize, want.ArchiveSize)
	}
	if got.SecurityEnvelopePos != want.SecurityEnvelopePos {
		t.Fatalf("SecurityEnvelopePos = %d, want %d", got.SecurityEnvelopePos, want.SecurityEnvelopePos)
	}
	if got.FilespecPos != want.FilespecPos {
		t.Fatalf("FilespecPos = %d, want %d", got.FilespecPos, want.FilespecPos)
	}
	if got.SecurityEnvelopeSize != want.SecurityEnvelopeSize {
		t.Fatalf("SecurityEnvelopeSize = %d, want %d", got.SecurityEnvelopeSize, want.SecurityEnvelopeSize)
	}
	if got.HostData != want.HostData {
		t.Fatalf("HostData = %d, want %d", got.HostData, want.HostData)
	}
	if got.Name != want.Name {
		t.Fatalf("Name = %q, want %q", got.Name, want.Name)
	}
	if got.Comment != want.Comment {
		t.Fatalf("Comment = %q, want %q", got.Comment, want.Comment)
	}
	if !bytes.Equal(got.FirstHeaderExtra, want.FirstHeaderExtra) {
		t.Fatalf("FirstHeaderExtra = %v, want %v", got.FirstHeaderExtra, want.FirstHeaderExtra)
	}
}

func readMainHeaderBlock(t *testing.T, archive []byte) ([]byte, [][]byte) {
	t.Helper()

	off, err := findMainHeaderOffset(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("findMainHeaderOffset: %v", err)
	}
	basic, ext, _, err := readHeaderBlock(bytes.NewReader(archive), int64(len(archive)), off)
	if err != nil {
		t.Fatalf("readHeaderBlock(main): %v", err)
	}
	return basic, ext
}

func corruptFirstMainExtendedHeaderCRC(t *testing.T, archive []byte) []byte {
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
	if out[start] != arjHeaderID1 || out[start+1] != arjHeaderID2 {
		t.Fatalf("invalid main header signature at %d", start)
	}
	basicSize := int(binary.LittleEndian.Uint16(out[start+2 : start+4]))
	cursor := start + 4 + basicSize + 4
	if cursor+2 > len(out) {
		t.Fatal("missing main extended-header size")
	}
	extSize := int(binary.LittleEndian.Uint16(out[cursor : cursor+2]))
	if extSize == 0 {
		t.Fatal("expected at least one main extended header")
	}
	crcOff := cursor + 2 + extSize
	if crcOff+4 > len(out) {
		t.Fatal("main extended-header CRC out of range")
	}
	out[crcOff] ^= 0xff
	return out
}

func signaturePlausibleMainHeaderNoise(size int) []byte {
	if size < 0 {
		size = 0
	}
	out := make([]byte, size)
	for i := 0; i+3 < len(out); i += 4 {
		out[i] = arjHeaderID1
		out[i+1] = arjHeaderID2
		out[i+2] = arjMinFirstHeaderSize
		out[i+3] = 0x00
	}
	if len(out)%4 == 1 {
		out[len(out)-1] = arjHeaderID1
	}
	return out
}

type countingReaderAt struct {
	data      []byte
	totalRead int64
}

func (r *countingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[off:])
	r.totalRead += int64(n)
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}
