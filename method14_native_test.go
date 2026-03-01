package arj

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
)

func TestMethod14RoundTrip(t *testing.T) {
	methods := []uint16{Method1, Method2, Method3, Method4}
	cases := []struct {
		name    string
		payload []byte
	}{
		{name: "empty", payload: nil},
		{name: "short", payload: []byte("native method payload")},
		{name: "binary", payload: append([]byte{}, bytes.Repeat([]byte{0x00, 0xFF, 0x10, 0xEF}, 64)...)},
		{name: "text", payload: bytes.Repeat([]byte("method-14-roundtrip-"), 48)},
	}

	for _, method := range methods {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("method-%d/%s", method, tc.name), func(t *testing.T) {
				var archive bytes.Buffer
				w := NewWriter(&archive)

				fw, err := w.CreateHeader(&FileHeader{Name: "payload.bin", Method: method})
				if err != nil {
					t.Fatalf("CreateHeader method %d: %v", method, err)
				}
				if _, err := fw.Write(tc.payload); err != nil {
					t.Fatalf("Write method %d: %v", method, err)
				}
				if err := w.Close(); err != nil {
					t.Fatalf("Close writer method %d: %v", method, err)
				}

				r, err := NewReader(bytes.NewReader(archive.Bytes()), int64(archive.Len()))
				if err != nil {
					t.Fatalf("NewReader method %d: %v", method, err)
				}
				if got, want := len(r.File), 1; got != want {
					t.Fatalf("file count method %d = %d, want %d", method, got, want)
				}
				if got, want := r.File[0].Method, method; got != want {
					t.Fatalf("method header = %d, want %d", got, want)
				}

				rc, err := r.File[0].Open()
				if err != nil {
					t.Fatalf("Open method %d: %v", method, err)
				}
				got, err := io.ReadAll(rc)
				if err != nil {
					t.Fatalf("ReadAll method %d: %v", method, err)
				}
				if err := rc.Close(); err != nil {
					t.Fatalf("Close method %d: %v", method, err)
				}
				if !bytes.Equal(got, tc.payload) {
					t.Fatalf("payload mismatch method %d", method)
				}
			})
		}
	}
}

func TestMethod14RoundTripMultipleFiles(t *testing.T) {
	payloads := map[uint16][]byte{
		Method1: bytes.Repeat([]byte("m1-"), 23),
		Method2: bytes.Repeat([]byte("m2-"), 31),
		Method3: bytes.Repeat([]byte("m3-"), 19),
		Method4: bytes.Repeat([]byte("m4-"), 17),
	}
	order := []uint16{Method1, Method2, Method3, Method4}

	var archive bytes.Buffer
	w := NewWriter(&archive)
	for _, method := range order {
		name := fmt.Sprintf("m%d.bin", method)
		fw, err := w.CreateHeader(&FileHeader{Name: name, Method: method})
		if err != nil {
			t.Fatalf("CreateHeader method %d: %v", method, err)
		}
		if _, err := fw.Write(payloads[method]); err != nil {
			t.Fatalf("Write method %d: %v", method, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := NewReader(bytes.NewReader(archive.Bytes()), int64(archive.Len()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if got, want := len(r.File), len(order); got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}

	for i, method := range order {
		f := r.File[i]
		if got, want := f.Method, method; got != want {
			t.Fatalf("file %d method = %d, want %d", i, got, want)
		}
		rc, err := f.Open()
		if err != nil {
			t.Fatalf("Open file %d method %d: %v", i, method, err)
		}
		got, err := io.ReadAll(rc)
		if err != nil {
			t.Fatalf("ReadAll file %d method %d: %v", i, method, err)
		}
		if err := rc.Close(); err != nil {
			t.Fatalf("Close file %d method %d: %v", i, method, err)
		}
		if !bytes.Equal(got, payloads[method]) {
			t.Fatalf("payload mismatch file %d method %d", i, method)
		}
	}
}

func TestMethod14RepetitivePayloadCompression(t *testing.T) {
	payload := bytes.Repeat([]byte("method14-native-repetition-"), 256)
	literal4 := encodeMethod4LiteralOnly(payload)

	for _, method := range []uint16{Method1, Method2, Method3, Method4} {
		t.Run(fmt.Sprintf("method-%d", method), func(t *testing.T) {
			compressed, err := compressMethod14Payload(method, payload)
			if err != nil {
				t.Fatalf("compress method %d: %v", method, err)
			}

			roundTrip, err := decompressMethod14Payload(method, compressed, uint64(len(payload)))
			if err != nil {
				t.Fatalf("decompress method %d: %v", method, err)
			}
			if !bytes.Equal(roundTrip, payload) {
				t.Fatalf("payload mismatch method %d", method)
			}

			if method == Method4 {
				if bytes.Equal(compressed, literal4) {
					t.Fatalf("method 4 used literal-only encoding on repetitive input")
				}
				return
			}
		})
	}
}

func TestMethod123BlockSplitRoundTrip(t *testing.T) {
	payload := make([]byte, 100000)
	var x uint32 = 1
	for i := range payload {
		x = x*1664525 + 1013904223
		payload[i] = byte(x >> 24)
	}

	for _, method := range []uint16{Method1, Method2, Method3} {
		t.Run(fmt.Sprintf("method-%d", method), func(t *testing.T) {
			compressed, err := compressMethod14Payload(method, payload)
			if err != nil {
				t.Fatalf("compress method %d: %v", method, err)
			}
			roundTrip, err := decompressMethod14Payload(method, compressed, uint64(len(payload)))
			if err != nil {
				t.Fatalf("decompress method %d: %v", method, err)
			}
			if !bytes.Equal(roundTrip, payload) {
				t.Fatalf("payload mismatch method %d", method)
			}
		})
	}
}

func TestMethod14CompressorDefaultInputLimitGuard(t *testing.T) {
	var dst bytes.Buffer
	cw, err := compressorMethod14(Method1)(&dst)
	if err != nil {
		t.Fatalf("compressorMethod14: %v", err)
	}

	mc, ok := cw.(*method14Compressor)
	if !ok {
		t.Fatalf("compressor type = %T, want *method14Compressor", cw)
	}
	if got, want := mc.limit, DefaultMaxMethod14InputBufferSize; got != want {
		t.Fatalf("default method14 input limit = %d, want %d", got, want)
	}

	// Simulate a full compressor input buffer so this test remains fast and
	// stable regardless of default limit value.
	mc.buf.size = mc.limit
	n, err := cw.Write([]byte("m"))
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
	if got, want := limitErr.Scope, bufferScopeMethod14Input; got != want {
		t.Fatalf("limit scope = %q, want %q", got, want)
	}
	if got, want := limitErr.Limit, DefaultMaxMethod14InputBufferSize; got != want {
		t.Fatalf("limit value = %d, want %d", got, want)
	}
	if got, want := limitErr.Buffered, DefaultMaxMethod14InputBufferSize; got != want {
		t.Fatalf("buffered = %d, want %d", got, want)
	}
	if got, want := limitErr.Attempted, uint64(1); got != want {
		t.Fatalf("attempted = %d, want %d", got, want)
	}
}

func TestWriterMethod14InputBufferLimitOverride(t *testing.T) {
	var archive bytes.Buffer
	w := NewWriter(&archive)
	w.SetBufferLimits(WriteBufferLimits{
		MaxEntryBufferSize:         1 << 20,
		MaxMethod14InputBufferSize: 8,
	})

	iw, err := w.CreateHeader(&FileHeader{Name: "m1.bin", Method: Method1})
	if err != nil {
		t.Fatalf("CreateHeader: %v", err)
	}

	n, err := iw.Write([]byte("0123456789"))
	if got, want := n, 8; got != want {
		t.Fatalf("Write bytes = %d, want %d", got, want)
	}
	if !errors.Is(err, ErrBufferLimitExceeded) {
		t.Fatalf("Write error = %v, want %v", err, ErrBufferLimitExceeded)
	}

	var limitErr *BufferLimitError
	if !errors.As(err, &limitErr) {
		t.Fatalf("Write error type = %T, want *BufferLimitError", err)
	}
	if got, want := limitErr.Scope, bufferScopeMethod14Input; got != want {
		t.Fatalf("limit scope = %q, want %q", got, want)
	}
	if got, want := limitErr.Limit, uint64(8); got != want {
		t.Fatalf("limit value = %d, want %d", got, want)
	}
}

func TestMethod14CompressorCloseFailsOnShortWrite(t *testing.T) {
	dst := &nilErrShortWriter{maxBytes: 1}

	cw, err := compressorMethod14(Method1)(dst)
	if err != nil {
		t.Fatalf("compressorMethod14: %v", err)
	}

	payload := bytes.Repeat([]byte("short-write-check-"), 16)
	if _, err := cw.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if err := cw.Close(); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("Close error = %v, want %v", err, io.ErrShortWrite)
	}
}

func TestARJBitWriterMatchesReference(t *testing.T) {
	type bitWrite struct {
		n int
		x uint16
	}

	var ops []bitWrite
	ops = append(ops,
		bitWrite{n: 0, x: 0},
		bitWrite{n: 1, x: 1},
		bitWrite{n: 16, x: 0xABCD},
		bitWrite{n: 7, x: 0x55},
	)

	var seed uint32 = 1
	next := func() uint32 {
		seed = seed*1664525 + 1013904223
		return seed
	}
	for i := 0; i < 4096; i++ {
		n := int(next() % 17)
		x := uint16(next())
		ops = append(ops, bitWrite{n: n, x: x})
	}

	var gotW arjBitWriter
	var wantW referenceARJBitWriter
	for _, op := range ops {
		gotW.putBits(op.n, op.x)
		wantW.putBits(op.n, op.x)
	}

	got := gotW.finishWithShutdownPadding()
	want := wantW.finishWithShutdownPadding()
	if !bytes.Equal(got, want) {
		t.Fatalf("bitstream mismatch: got %d bytes, want %d bytes", len(got), len(want))
	}
}

func TestMethod14AdvanceBitPosBoundaries(t *testing.T) {
	tests := []struct {
		name      string
		bitPos    uint64
		totalBits uint64
		n         int
		wantPos   uint64
		wantOK    bool
	}{
		{name: "advance within range", bitPos: 0, totalBits: 32, n: 8, wantPos: 8, wantOK: true},
		{name: "advance to end", bitPos: 24, totalBits: 32, n: 8, wantPos: 32, wantOK: true},
		{name: "advance past end rejected", bitPos: 31, totalBits: 32, n: 2, wantOK: false},
		{name: "invalid width rejected", bitPos: 0, totalBits: 32, n: methodCodeBit + 1, wantOK: false},
		{name: "large counters handled safely", bitPos: 1 << 40, totalBits: (1 << 40) + 16, n: 16, wantPos: (1 << 40) + 16, wantOK: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotPos, ok := method14AdvanceBitPos(tc.bitPos, tc.totalBits, tc.n)
			if ok != tc.wantOK {
				t.Fatalf("method14AdvanceBitPos(%d,%d,%d) ok = %t, want %t", tc.bitPos, tc.totalBits, tc.n, ok, tc.wantOK)
			}
			if ok && gotPos != tc.wantPos {
				t.Fatalf("method14AdvanceBitPos(%d,%d,%d) pos = %d, want %d", tc.bitPos, tc.totalBits, tc.n, gotPos, tc.wantPos)
			}
		})
	}
}

func TestMethod123RejectsZeroBlockSize(t *testing.T) {
	for _, method := range []uint16{Method1, Method2, Method3} {
		t.Run(fmt.Sprintf("method-%d", method), func(t *testing.T) {
			_, err := decompressMethod14Payload(method, []byte{0x00, 0x00}, 1)
			if !errors.Is(err, ErrFormat) {
				t.Fatalf("decompress method %d zero block error = %v, want %v", method, err, ErrFormat)
			}
		})
	}
}

func TestMethod4RejectsImpossibleInitialMatchLength(t *testing.T) {
	var bw arjBitWriter
	// Encode a match token with decoded length 3 (n=1) and distance 1 (ptr=0).
	// For origSize=1 this must be rejected as malformed.
	enc4Pass1(&bw, 1)
	enc4Pass2(&bw, 0)

	compressed := bw.finishWithShutdownPadding()
	_, err := decompressMethod14Payload(Method4, compressed, 1)
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("decompress method 4 impossible match error = %v, want %v", err, ErrFormat)
	}
}

func TestMethod4RejectsTruncatedLiteral(t *testing.T) {
	_, err := decompressMethod14Payload(Method4, []byte{0x00}, 1)
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("decompress method 4 truncated literal error = %v, want %v", err, ErrFormat)
	}
}

func TestDecodeMethod4RangeBoundaries(t *testing.T) {
	t.Run("pointer_max", func(t *testing.T) {
		var bw arjBitWriter
		enc4Pass2(&bw, method4MaxPtr)

		br := newARJBitReader(bw.finishWithShutdownPadding())
		got, err := decodeMethod4Ptr(br)
		if err != nil {
			t.Fatalf("decodeMethod4Ptr error: %v", err)
		}
		if got != method4MaxPtr {
			t.Fatalf("decodeMethod4Ptr = %d, want %d", got, method4MaxPtr)
		}
	})

	t.Run("length_max", func(t *testing.T) {
		var bw arjBitWriter
		const maxEncLen = method4MaxMatch - (methodThresh - 1)
		enc4Pass1(&bw, maxEncLen)

		br := newARJBitReader(bw.finishWithShutdownPadding())
		got, err := decodeMethod4Len(br)
		if err != nil {
			t.Fatalf("decodeMethod4Len error: %v", err)
		}
		if got != maxEncLen {
			t.Fatalf("decodeMethod4Len = %d, want %d", got, maxEncLen)
		}
	})
}

func TestMakeDecodeTableRejectsOversubscribedLengths(t *testing.T) {
	bitLen := []uint8{1, 1, 1}
	table := make([]uint16, 2)
	left := make([]uint16, 8)
	right := make([]uint16, 8)

	err := makeDecodeTable(3, bitLen, 1, table, len(table), left, right)
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("makeDecodeTable oversubscribed error = %v, want %v", err, ErrFormat)
	}
}

type nilErrShortWriter struct {
	maxBytes int
}

func (w *nilErrShortWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if w.maxBytes <= 0 || w.maxBytes >= len(p) {
		return len(p), nil
	}
	return w.maxBytes, nil
}

type referenceARJBitWriter struct {
	out      []byte
	bitBuf   byte
	bitCount int
}

func (bw *referenceARJBitWriter) putBits(n int, x uint16) {
	for i := n - 1; i >= 0; i-- {
		bit := byte((x >> i) & 1)
		bw.bitBuf = (bw.bitBuf << 1) | bit
		bw.bitCount++
		if bw.bitCount == 8 {
			bw.out = append(bw.out, bw.bitBuf)
			bw.bitBuf = 0
			bw.bitCount = 0
		}
	}
}

func (bw *referenceARJBitWriter) finishWithShutdownPadding() []byte {
	bw.putBits(7, 0)
	return bw.out
}
