package arj

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"testing"
)

func TestMethod14TruncatedPayloadRejected(t *testing.T) {
	payload := bytes.Repeat([]byte("method14-truncated-payload-"), 128)
	for _, method := range []uint16{Method1, Method2, Method3, Method4} {
		t.Run(fmt.Sprintf("method-%d", method), func(t *testing.T) {
			compressed, err := compressMethod14Payload(method, payload)
			if err != nil {
				t.Fatalf("compress method %d: %v", method, err)
			}
			if len(compressed) < 2 {
				t.Fatalf("compressed payload too small: %d", len(compressed))
			}

			truncated := compressed[:len(compressed)-1]
			_, err = decompressMethod14Payload(method, truncated, uint64(len(payload)))
			if !errors.Is(err, ErrFormat) {
				t.Fatalf("decompress truncated method %d error = %v, want %v", method, err, ErrFormat)
			}
		})
	}
}

func TestMethod14OpenRejectsHugeAdvertisedUncompressedSize(t *testing.T) {
	payload := bytes.Repeat([]byte("method14-size-limit-"), 128)
	archive := buildMethod14Archive(t, Method1, payload)
	mutated := mutateFirstLocalHeaderUncompressedSize(t, archive, ^uint32(0))

	r, err := NewReader(bytes.NewReader(mutated), int64(len(mutated)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if got, want := len(r.File), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}

	_, err = r.File[0].Open()
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("Open error = %v, want %v", err, ErrFormat)
	}
}

func TestMethod14DecodeLimitsConfigurable(t *testing.T) {
	payload := bytes.Repeat([]byte("method14-configurable-limits-"), 128)
	archive := buildMethod14Archive(t, Method4, payload)

	t.Run("compressed", func(t *testing.T) {
		r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
		if err != nil {
			t.Fatalf("NewReader: %v", err)
		}
		f := r.File[0]
		if f.CompressedSize64 <= 1 {
			t.Fatalf("compressed size too small for limit test: %d", f.CompressedSize64)
		}

		r.SetMethod14DecodeLimits(Method14DecodeLimits{
			MaxCompressedSize:   f.CompressedSize64 - 1,
			MaxUncompressedSize: f.UncompressedSize64,
		})

		_, err = f.Open()
		if !errors.Is(err, ErrFormat) {
			t.Fatalf("Open error = %v, want %v", err, ErrFormat)
		}
	})

	t.Run("uncompressed", func(t *testing.T) {
		r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
		if err != nil {
			t.Fatalf("NewReader: %v", err)
		}
		f := r.File[0]
		if f.UncompressedSize64 <= 1 {
			t.Fatalf("uncompressed size too small for limit test: %d", f.UncompressedSize64)
		}

		r.SetMethod14DecodeLimits(Method14DecodeLimits{
			MaxCompressedSize:   f.CompressedSize64,
			MaxUncompressedSize: f.UncompressedSize64 - 1,
		})

		_, err = f.Open()
		if !errors.Is(err, ErrFormat) {
			t.Fatalf("Open error = %v, want %v", err, ErrFormat)
		}
	})
}

func TestMethod14DecodeHardCapCannotBeRaisedByLimits(t *testing.T) {
	limits := Method14DecodeLimits{
		MaxCompressedSize:   ^uint64(0),
		MaxUncompressedSize: ^uint64(0),
	}
	_, err := decompressMethod14PayloadWithLimits(
		Method1,
		[]byte{0x00},
		method14MaxDecodeWorkingSetBytes,
		limits,
	)
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("decompress with raised limits over hard cap error = %v, want %v", err, ErrFormat)
	}
}

func TestMethod14ReaderDecodeHardCapCannotBeRaised(t *testing.T) {
	payload := bytes.Repeat([]byte("method14-hard-cap-reader-"), 64)
	archive := buildMethod14Archive(t, Method1, payload)

	oversized := method14MaxDecodeWorkingSetBytes + 1
	if oversized > uint64(^uint32(0)) {
		t.Fatalf("test setup overflow: oversized=%d exceeds uint32", oversized)
	}
	mutated := mutateFirstLocalHeaderUncompressedSize(t, archive, uint32(oversized))

	r, err := NewReader(bytes.NewReader(mutated), int64(len(mutated)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	r.SetMethod14DecodeLimits(Method14DecodeLimits{
		MaxCompressedSize:   ^uint64(0),
		MaxUncompressedSize: ^uint64(0),
	})
	rc, err := r.File[0].Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_, err = io.ReadAll(rc)
	closeErr := rc.Close()
	if err == nil {
		err = closeErr
	}
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("Read with raised limits over hard cap error = %v, want %v", err, ErrFormat)
	}
}

func TestValidateMethod14DecodeBufferSizesIntBoundaries(t *testing.T) {
	maxInt := method14TestArchMaxInt()
	limits := Method14DecodeLimits{
		MaxCompressedSize:   ^uint64(0),
		MaxUncompressedSize: ^uint64(0),
	}

	tests := []struct {
		name             string
		compressedSize   uint64
		uncompressedSize uint64
		wantErr          bool
	}{
		{
			name:             "compressed_at_int_max_allowed",
			compressedSize:   maxInt,
			uncompressedSize: 0,
		},
		{
			name:             "sum_at_int_max_allowed",
			compressedSize:   maxInt - 1,
			uncompressedSize: 1,
		},
		{
			name:             "compressed_over_int_max_rejected",
			compressedSize:   maxInt + 1,
			uncompressedSize: 0,
			wantErr:          true,
		},
		{
			name:             "uncompressed_over_int_max_rejected",
			compressedSize:   0,
			uncompressedSize: maxInt + 1,
			wantErr:          true,
		},
		{
			name:             "combined_buffers_overflow_rejected",
			compressedSize:   maxInt,
			uncompressedSize: 1,
			wantErr:          true,
		},
		{
			name:             "combined_buffers_overflow_rejected_inverse",
			compressedSize:   1,
			uncompressedSize: maxInt,
			wantErr:          true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateMethod14DecodeBufferSizes(limits, tc.compressedSize, tc.uncompressedSize)
			if tc.wantErr {
				if !errors.Is(err, ErrFormat) {
					t.Fatalf("validateMethod14DecodeBufferSizes(%d,%d) error = %v, want %v", tc.compressedSize, tc.uncompressedSize, err, ErrFormat)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateMethod14DecodeBufferSizes(%d,%d) error = %v, want nil", tc.compressedSize, tc.uncompressedSize, err)
			}
		})
	}
}

func TestValidateMethod14DecodeBufferSizesForArchIntBoundaries(t *testing.T) {
	const maxInt32 = uint64(^uint32(0) >> 1)
	limits := Method14DecodeLimits{
		MaxCompressedSize:   ^uint64(0),
		MaxUncompressedSize: ^uint64(0),
	}

	tests := []struct {
		name             string
		compressedSize   uint64
		uncompressedSize uint64
		wantErr          bool
	}{
		{
			name:             "compressed_at_int32_max_allowed",
			compressedSize:   maxInt32,
			uncompressedSize: 0,
		},
		{
			name:             "sum_at_int32_max_allowed",
			compressedSize:   maxInt32 - 1,
			uncompressedSize: 1,
		},
		{
			name:             "compressed_over_int32_max_rejected",
			compressedSize:   maxInt32 + 1,
			uncompressedSize: 0,
			wantErr:          true,
		},
		{
			name:             "sum_over_int32_max_rejected",
			compressedSize:   maxInt32,
			uncompressedSize: 1,
			wantErr:          true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateMethod14DecodeBufferSizesForArch(limits, tc.compressedSize, tc.uncompressedSize, maxInt32)
			if tc.wantErr {
				if !errors.Is(err, ErrFormat) {
					t.Fatalf("validateMethod14DecodeBufferSizesForArch(%d,%d) error = %v, want %v", tc.compressedSize, tc.uncompressedSize, err, ErrFormat)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateMethod14DecodeBufferSizesForArch(%d,%d) error = %v, want nil", tc.compressedSize, tc.uncompressedSize, err)
			}
		})
	}
}

func TestValidateMethod14DecodeWorkingSetBoundaries(t *testing.T) {
	tests := []struct {
		name             string
		compressedSize   uint64
		uncompressedSize uint64
		wantErr          bool
	}{
		{
			name:             "at_cap_allowed",
			compressedSize:   DefaultMethod14MaxCompressedSize,
			uncompressedSize: DefaultMethod14MaxUncompressedSize,
		},
		{
			name:             "sum_over_cap_rejected",
			compressedSize:   DefaultMethod14MaxCompressedSize,
			uncompressedSize: DefaultMethod14MaxUncompressedSize + 1,
			wantErr:          true,
		},
		{
			name:             "compressed_over_cap_rejected",
			compressedSize:   method14MaxDecodeWorkingSetBytes + 1,
			uncompressedSize: 0,
			wantErr:          true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateMethod14DecodeWorkingSet(tc.compressedSize, tc.uncompressedSize)
			if tc.wantErr {
				if !errors.Is(err, ErrFormat) {
					t.Fatalf("validateMethod14DecodeWorkingSet(%d,%d) error = %v, want %v", tc.compressedSize, tc.uncompressedSize, err, ErrFormat)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateMethod14DecodeWorkingSet(%d,%d) error = %v, want nil", tc.compressedSize, tc.uncompressedSize, err)
			}
		})
	}
}

func TestMethod14DecompressRejectsCombinedBufferOverflowNearIntMax(t *testing.T) {
	maxInt := method14TestArchMaxInt()
	limits := Method14DecodeLimits{
		MaxCompressedSize:   ^uint64(0),
		MaxUncompressedSize: ^uint64(0),
	}

	_, err := decompressMethod14PayloadWithLimits(Method1, []byte{0x00}, maxInt, limits)
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("decompressMethod14PayloadWithLimits overflow error = %v, want %v", err, ErrFormat)
	}
}

func method14TestArchMaxInt() uint64 {
	return uint64(int(^uint(0) >> 1))
}

func buildMethod14Archive(t *testing.T, method uint16, payload []byte) []byte {
	t.Helper()

	var archive bytes.Buffer
	w := NewWriter(&archive)

	fw, err := w.CreateHeader(&FileHeader{Name: "payload.bin", Method: method})
	if err != nil {
		t.Fatalf("CreateHeader method %d: %v", method, err)
	}
	if _, err := fw.Write(payload); err != nil {
		t.Fatalf("Write method %d: %v", method, err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	return append([]byte(nil), archive.Bytes()...)
}

func mutateFirstLocalHeaderUncompressedSize(t *testing.T, archive []byte, uncompressedSize uint32) []byte {
	t.Helper()

	out := append([]byte(nil), archive...)
	basicStart, basicEnd := firstLocalHeaderBasicBounds(t, out)
	binary.LittleEndian.PutUint32(out[basicStart+16:basicStart+20], uncompressedSize)
	binary.LittleEndian.PutUint32(out[basicEnd:basicEnd+4], crc32.ChecksumIEEE(out[basicStart:basicEnd]))
	return out
}

func firstLocalHeaderBasicBounds(t *testing.T, archive []byte) (int, int) {
	t.Helper()

	mainOff, err := findMainHeaderOffset(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("findMainHeaderOffset: %v", err)
	}
	_, _, localOff, err := readHeaderBlock(bytes.NewReader(archive), int64(len(archive)), mainOff)
	if err != nil {
		t.Fatalf("readHeaderBlock main: %v", err)
	}

	i := int(localOff)
	if i < 0 || i+4 > len(archive) {
		t.Fatalf("local header offset out of range: %d", localOff)
	}
	if archive[i] != arjHeaderID1 || archive[i+1] != arjHeaderID2 {
		t.Fatalf("local header signature mismatch at offset %d", localOff)
	}

	basicSize := int(binary.LittleEndian.Uint16(archive[i+2 : i+4]))
	basicStart := i + 4
	basicEnd := basicStart + basicSize
	if basicSize < arjMinFirstHeaderSize || basicEnd+4 > len(archive) {
		t.Fatalf("local header size out of range: %d", basicSize)
	}
	return basicStart, basicEnd
}
