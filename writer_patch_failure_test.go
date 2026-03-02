package arj

import (
	"errors"
	"io"
	"testing"
)

var (
	errInjectedPatchWrite = errors.New("injected patch write failure")
	errInjectedPatchSeek  = errors.New("injected patch seek failure")
)

func TestPatchLocalFileHeaderRejectsNilWriterAndNegativeOffset(t *testing.T) {
	h := &FileHeader{Name: "patch.bin", Method: Store}

	if err := patchLocalFileHeader(nil, 0, h); err == nil {
		t.Fatal("patchLocalFileHeader(nil, ...) error = nil, want error")
	}

	ws := &patchFailureWriteSeeker{}
	if err := patchLocalFileHeader(ws, -1, h); err == nil {
		t.Fatal("patchLocalFileHeader negative offset error = nil, want error")
	}
}

func TestPatchLocalFileHeaderReturnsPatchWriteFailure(t *testing.T) {
	ws := &patchFailureWriteSeeker{failWriteAfterPatchSeek: true}
	h := &FileHeader{Name: "patch-write.bin", Method: Store}

	err := patchLocalFileHeader(ws, 0, h)
	if !errors.Is(err, errInjectedPatchWrite) {
		t.Fatalf("patchLocalFileHeader error = %v, want %v", err, errInjectedPatchWrite)
	}
}

func TestPatchLocalFileHeaderReturnsSeekRestoreFailure(t *testing.T) {
	ws := &patchFailureWriteSeeker{
		failSeekOnCall: 3, // current, patch-start, restore-start
		pos:            17,
	}
	h := &FileHeader{Name: "patch-seek.bin", Method: Store}

	err := patchLocalFileHeader(ws, 0, h)
	if !errors.Is(err, errInjectedPatchSeek) {
		t.Fatalf("patchLocalFileHeader error = %v, want %v", err, errInjectedPatchSeek)
	}
}

func TestWriterEntryCloseSurfacesPatchWriteFailure(t *testing.T) {
	ws := &patchFailureWriteSeeker{failWriteAfterPatchSeek: true}
	w := NewWriter(ws)

	iw, err := w.CreateHeader(&FileHeader{Name: "stream.bin", Method: Store})
	if err != nil {
		t.Fatalf("CreateHeader: %v", err)
	}
	if _, err := iw.Write([]byte("payload")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := iw.(io.Closer).Close(); !errors.Is(err, errInjectedPatchWrite) {
		t.Fatalf("Close entry error = %v, want %v", err, errInjectedPatchWrite)
	}
	if err := w.Close(); !errors.Is(err, errInjectedPatchWrite) {
		t.Fatalf("Close writer error = %v, want %v", err, errInjectedPatchWrite)
	}
}

func TestWriterEntryCloseSurfacesPatchSeekRestoreFailure(t *testing.T) {
	ws := &patchFailureWriteSeeker{failSeekOnCall: 3}
	w := NewWriter(ws)

	iw, err := w.CreateHeader(&FileHeader{Name: "stream.bin", Method: Store})
	if err != nil {
		t.Fatalf("CreateHeader: %v", err)
	}
	if _, err := iw.Write([]byte("payload")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := iw.(io.Closer).Close(); !errors.Is(err, errInjectedPatchSeek) {
		t.Fatalf("Close entry error = %v, want %v", err, errInjectedPatchSeek)
	}
	if err := w.Close(); !errors.Is(err, errInjectedPatchSeek) {
		t.Fatalf("Close writer error = %v, want %v", err, errInjectedPatchSeek)
	}
}

type patchFailureWriteSeeker struct {
	data []byte
	pos  int64

	seekCalls int

	failWriteAfterPatchSeek bool
	failSeekOnCall          int

	seenPatchSeekStart bool
}

func (w *patchFailureWriteSeeker) Write(p []byte) (int, error) {
	if w.failWriteAfterPatchSeek && w.seenPatchSeekStart {
		return 0, errInjectedPatchWrite
	}

	end := w.pos + int64(len(p))
	if end < 0 {
		return 0, ErrFormat
	}
	if end > int64(len(w.data)) {
		grow := make([]byte, end-int64(len(w.data)))
		w.data = append(w.data, grow...)
	}
	copy(w.data[w.pos:end], p)
	w.pos = end
	return len(p), nil
}

func (w *patchFailureWriteSeeker) Seek(offset int64, whence int) (int64, error) {
	w.seekCalls++
	if w.failSeekOnCall > 0 && w.seekCalls == w.failSeekOnCall {
		return 0, errInjectedPatchSeek
	}

	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
		w.seenPatchSeekStart = true
	case io.SeekCurrent:
		next = w.pos + offset
	case io.SeekEnd:
		next = int64(len(w.data)) + offset
	default:
		return 0, ErrFormat
	}
	if next < 0 {
		return 0, ErrFormat
	}
	w.pos = next
	return w.pos, nil
}
