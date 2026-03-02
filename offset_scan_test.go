package arj

import (
	"bytes"
	"io"
	"testing"
)

func TestFindMainHeaderOffsetSkipsMalformedPrefixedCandidate(t *testing.T) {
	decoy := buildDecoyMainHeaderArchive(t, "decoy-broken.arj", false)
	real := buildSingleStoreArchive(t, "real.bin", []byte("real-payload"))

	prefix := []byte("MZ-stub-prefix")
	container := append(append(append([]byte(nil), prefix...), decoy...), real...)
	want := int64(len(prefix) + len(decoy))

	got, err := findMainHeaderOffset(bytes.NewReader(container), int64(len(container)))
	if err != nil {
		t.Fatalf("findMainHeaderOffset: %v", err)
	}
	if got != want {
		t.Fatalf("findMainHeaderOffset = %d, want %d", got, want)
	}

	r, err := NewReader(bytes.NewReader(container), int64(len(container)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if got := r.BaseOffset(); got != want {
		t.Fatalf("BaseOffset = %d, want %d", got, want)
	}
	if got, want := len(r.File), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
}

func TestFindMainHeaderOffsetPrefersCandidateWithFiles(t *testing.T) {
	decoy := buildDecoyMainHeaderArchive(t, "decoy-empty.arj", true)
	real := buildSingleStoreArchive(t, "real.bin", []byte("real-payload"))

	prefix := []byte("SFX-prefix")
	container := append(append(append([]byte(nil), prefix...), decoy...), real...)
	want := int64(len(prefix) + len(decoy))

	got, err := findMainHeaderOffset(bytes.NewReader(container), int64(len(container)))
	if err != nil {
		t.Fatalf("findMainHeaderOffset: %v", err)
	}
	if got != want {
		t.Fatalf("findMainHeaderOffset = %d, want %d", got, want)
	}

	r, err := NewReader(bytes.NewReader(container), int64(len(container)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if got := r.BaseOffset(); got != want {
		t.Fatalf("BaseOffset = %d, want %d", got, want)
	}
	if got, want := len(r.File), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
}

func TestFindMainHeaderOffsetAtZeroDecoyStillPrefersRealCandidate(t *testing.T) {
	decoy := buildDecoyMainHeaderArchive(t, "decoy-empty.arj", true)
	real := buildSingleStoreArchive(t, "real.bin", []byte("real-payload"))
	container := append(append([]byte(nil), decoy...), real...)
	want := int64(len(decoy))

	got, err := findMainHeaderOffset(bytes.NewReader(container), int64(len(container)))
	if err != nil {
		t.Fatalf("findMainHeaderOffset: %v", err)
	}
	if got != want {
		t.Fatalf("findMainHeaderOffset = %d, want %d", got, want)
	}

	r, err := NewReader(bytes.NewReader(container), int64(len(container)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if got := r.BaseOffset(); got != want {
		t.Fatalf("BaseOffset = %d, want %d", got, want)
	}
	if got, want := len(r.File), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
}

func TestReaderOffsetsWithPrefixAndAppendedData(t *testing.T) {
	real := buildSingleStoreArchive(t, "payload.txt", []byte("payload-data"))
	decoy := buildDecoyMainHeaderArchive(t, "decoy-empty.arj", true)

	realReader, err := NewReader(bytes.NewReader(real), int64(len(real)))
	if err != nil {
		t.Fatalf("NewReader(real): %v", err)
	}
	realDataOffset, err := realReader.File[0].DataOffset()
	if err != nil {
		t.Fatalf("DataOffset(real): %v", err)
	}

	prefix := append([]byte("EXE-STUB"), decoy...)
	suffix := []byte("TRAILING-APPENDED-BYTES")
	container := append(append(append([]byte(nil), prefix...), real...), suffix...)

	r, err := NewReader(bytes.NewReader(container), int64(len(container)))
	if err != nil {
		t.Fatalf("NewReader(container): %v", err)
	}
	if got, want := r.BaseOffset(), int64(len(prefix)); got != want {
		t.Fatalf("BaseOffset = %d, want %d", got, want)
	}
	if got, want := len(r.File), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}

	gotDataOffset, err := r.File[0].DataOffset()
	if err != nil {
		t.Fatalf("DataOffset(container): %v", err)
	}
	if want := int64(len(prefix)) + realDataOffset; gotDataOffset != want {
		t.Fatalf("DataOffset(container) = %d, want %d", gotDataOffset, want)
	}

	rc, err := r.File[0].Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got, want := string(data), "payload-data"; got != want {
		t.Fatalf("payload = %q, want %q", got, want)
	}
}

func TestFindMainHeaderOffsetWithSignatureDensePrefix(t *testing.T) {
	payload := bytes.Repeat([]byte("dense-scan-payload"), 2048)
	real := buildSingleStoreArchive(t, "payload.bin", payload)
	decoy := buildDecoyMainHeaderArchive(t, "decoy-empty.arj", true)
	prefix := signatureDenseOffsetNoise(512 << 10)

	container := make([]byte, 0, len(prefix)+len(decoy)+len(real))
	container = append(container, prefix...)
	container = append(container, decoy...)
	container = append(container, real...)
	size := int64(len(container))
	wantOff := int64(len(prefix) + len(decoy))

	gotOff, err := findMainHeaderOffset(bytes.NewReader(container), size)
	if err != nil {
		t.Fatalf("findMainHeaderOffset: %v", err)
	}
	if gotOff != wantOff {
		t.Fatalf("findMainHeaderOffset = %d, want %d", gotOff, wantOff)
	}

	r, err := NewReader(bytes.NewReader(container), size)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if got := r.BaseOffset(); got != wantOff {
		t.Fatalf("BaseOffset = %d, want %d", got, wantOff)
	}
	if got, want := len(r.File), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}

	rc, err := r.File[0].Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	gotPayload, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Fatalf("payload mismatch")
	}
}

func TestFindMainHeaderOffsetHandlesSignatureSplitAcrossScanChunkBoundary(t *testing.T) {
	real := buildSingleStoreArchive(t, "chunk-boundary.bin", []byte("boundary payload"))
	if len(real) < 2 || real[0] != arjHeaderID1 || real[1] != arjHeaderID2 {
		t.Fatalf("invalid real archive signature prefix")
	}

	prefix := bytes.Repeat([]byte{0x11}, mainHeaderScanChunkSize-1)
	prefix[len(prefix)-1] = arjHeaderID1
	container := append(append([]byte(nil), prefix...), real[1:]...)
	want := int64(len(prefix) - 1)

	got, err := findMainHeaderOffset(bytes.NewReader(container), int64(len(container)))
	if err != nil {
		t.Fatalf("findMainHeaderOffset: %v", err)
	}
	if got != want {
		t.Fatalf("findMainHeaderOffset = %d, want %d", got, want)
	}

	r, err := NewReader(bytes.NewReader(container), int64(len(container)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if gotOff := r.BaseOffset(); gotOff != want {
		t.Fatalf("BaseOffset = %d, want %d", gotOff, want)
	}
	if gotFiles, wantFiles := len(r.File), 1; gotFiles != wantFiles {
		t.Fatalf("file count = %d, want %d", gotFiles, wantFiles)
	}
}

func buildDecoyMainHeaderArchive(t *testing.T, name string, withTerminator bool) []byte {
	t.Helper()

	basic := make([]byte, arjMinFirstHeaderSize)
	basic[0] = arjMinFirstHeaderSize
	basic[1] = arjVersionCurrent
	basic[2] = arjVersionNeeded
	basic[3] = currentHostOS()
	basic[6] = arjFileTypeMain

	full := make([]byte, 0, len(basic)+len(name)+2)
	full = append(full, basic...)
	full = append(full, name...)
	full = append(full, 0)
	full = append(full, 0)

	var buf bytes.Buffer
	if err := writeHeaderBlock(&buf, full); err != nil {
		t.Fatalf("writeHeaderBlock(decoy): %v", err)
	}
	if withTerminator {
		if _, err := buf.Write([]byte{arjHeaderID1, arjHeaderID2, 0, 0}); err != nil {
			t.Fatalf("write decoy terminator: %v", err)
		}
	} else {
		if _, err := buf.Write([]byte("NOPE")); err != nil {
			t.Fatalf("write malformed decoy trailer: %v", err)
		}
	}
	return buf.Bytes()
}

func signatureDenseOffsetNoise(size int) []byte {
	if size < 0 {
		size = 0
	}
	out := make([]byte, size)
	for i := 0; i+3 < len(out); i += 4 {
		out[i] = arjHeaderID1
		out[i+1] = arjHeaderID2
		out[i+2] = 0xff
		out[i+3] = 0xff
	}
	if len(out)%4 == 1 {
		out[len(out)-1] = arjHeaderID1
	}
	return out
}

func buildSingleStoreArchive(t *testing.T, name string, payload []byte) []byte {
	t.Helper()

	var buf bytes.Buffer
	w := NewWriter(&buf)
	fw, err := w.CreateHeader(&FileHeader{
		Name:   name,
		Method: Store,
	})
	if err != nil {
		t.Fatalf("CreateHeader(%s): %v", name, err)
	}
	if _, err := fw.Write(payload); err != nil {
		t.Fatalf("Write(%s): %v", name, err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	return append([]byte(nil), buf.Bytes()...)
}
