package arj

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

const (
	fuzzParserMaxInputBytes  = 1 << 18
	fuzzParserMaxEntryRead   = 1 << 16
	fuzzParserMaxFilesToOpen = 4
	fuzzParserMaxHeaderWalk  = 8
)

func FuzzParserArbitraryBytes(f *testing.F) {
	seeds := [][]byte{
		nil,
		{0x00},
		{arjHeaderID1},
		{arjHeaderID1, arjHeaderID2, 0x00, 0x00},
		bytes.Repeat([]byte{arjHeaderID1, arjHeaderID2, 0xff, 0xff}, 16),
	}
	seeds = append(seeds, fuzzParserDerivedSeeds()...)
	for _, seed := range seeds {
		f.Add(append([]byte(nil), seed...))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > fuzzParserMaxInputBytes {
			return
		}

		blob := append([]byte(nil), data...)
		fuzzExerciseParserSurfaces(t, blob)

		r, err := NewReader(bytes.NewReader(blob), int64(len(blob)))
		if err != nil {
			if !errors.Is(err, ErrFormat) {
				t.Fatalf("NewReader(%d bytes) error = %v, want nil or %v", len(blob), err, ErrFormat)
			}
			return
		}

		r.SetMethod14DecodeLimits(Method14DecodeLimits{
			MaxCompressedSize:   fuzzParserMaxEntryRead,
			MaxUncompressedSize: fuzzParserMaxEntryRead,
		})

		maxOpen := len(r.File)
		if maxOpen > fuzzParserMaxFilesToOpen {
			maxOpen = fuzzParserMaxFilesToOpen
		}
		for i := 0; i < maxOpen; i++ {
			rc, err := r.File[i].Open()
			if err != nil {
				continue
			}
			_, _ = io.Copy(io.Discard, io.LimitReader(rc, fuzzParserMaxEntryRead))
			_ = rc.Close()
		}
	})
}

func fuzzExerciseParserSurfaces(t *testing.T, data []byte) {
	t.Helper()

	size := int64(len(data))
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("parser panic for %d-byte input: %v", len(data), recovered)
		}
	}()

	_, _ = parseMainHeader(data, nil)
	_, _ = parseLocalFileHeader(data, nil, nil)

	if size == 0 {
		return
	}

	offsets := []int64{0, size / 2, size - 1, int64(data[0]) % size}
	for _, off := range offsets {
		basic, ext, next, err := readHeaderBlock(bytes.NewReader(data), size, off)
		if err != nil {
			if !errors.Is(err, ErrFormat) {
				t.Fatalf("readHeaderBlock(%d) error = %v, want nil or %v", off, err, ErrFormat)
			}
			continue
		}

		_, _ = parseMainHeader(basic, ext)
		local, localErr := parseLocalFileHeader(basic, ext, nil)
		if localErr == nil && local != nil {
			if next > size {
				t.Fatalf("readHeaderBlock next offset = %d, size = %d", next, size)
			}
			if local.CompressedSize64 <= uint64(size-next) {
				projected := next + int64(local.CompressedSize64)
				if projected < next {
					t.Fatalf("projected local data offset overflowed: next=%d compressed=%d", next, local.CompressedSize64)
				}
			}
		}
	}

	mainOff, err := findMainHeaderOffset(bytes.NewReader(data), size)
	if err != nil {
		if !errors.Is(err, ErrFormat) {
			t.Fatalf("findMainHeaderOffset error = %v, want nil or %v", err, ErrFormat)
		}
		return
	}
	if mainOff < 0 || mainOff >= size {
		t.Fatalf("findMainHeaderOffset = %d out of range for size %d", mainOff, size)
	}

	files, end, ok, probeErr := probeArchiveLayout(bytes.NewReader(data), size, mainOff)
	if probeErr != nil {
		if !errors.Is(probeErr, ErrFormat) {
			t.Fatalf("probeArchiveLayout error = %v, want nil or %v", probeErr, ErrFormat)
		}
		return
	}
	if ok && end < mainOff {
		t.Fatalf("probeArchiveLayout end = %d before main offset %d (files=%d)", end, mainOff, files)
	}

	off := mainOff
	for i := 0; i < fuzzParserMaxHeaderWalk && off < size; i++ {
		basic, ext, afterHeader, readErr := readHeaderBlock(bytes.NewReader(data), size, off)
		if readErr != nil || len(basic) == 0 {
			break
		}

		if i == 0 {
			_, _ = parseMainHeader(basic, ext)
			if afterHeader <= off {
				break
			}
			off = afterHeader
			continue
		}

		local, localErr := parseLocalFileHeader(basic, ext, nil)
		if localErr != nil || local == nil {
			break
		}
		if afterHeader > size || local.CompressedSize64 > uint64(size-afterHeader) {
			break
		}
		next := afterHeader + int64(local.CompressedSize64)
		if next <= off {
			break
		}
		off = next
	}
}

func fuzzParserSeedArchive() []byte {
	var archive bytes.Buffer
	w := NewWriter(&archive)
	fw, err := w.Create("seed.bin")
	if err != nil {
		return nil
	}
	if _, err := fw.Write([]byte("seed")); err != nil {
		return nil
	}
	if err := w.Close(); err != nil {
		return nil
	}
	return append([]byte(nil), archive.Bytes()...)
}

func fuzzParserDerivedSeeds() [][]byte {
	base := fuzzParserSeedArchive()
	if len(base) == 0 {
		return nil
	}

	out := [][]byte{
		base,
		append(bytes.Repeat([]byte("SFX"), 32), base...),
		append(append([]byte(nil), base...), bytes.Repeat([]byte{0x00}, 32)...),
	}

	if len(base) > 1 {
		out = append(out, append([]byte(nil), base[:len(base)-1]...))
	}
	if len(base) > 8 {
		out = append(out, append([]byte(nil), base[:8]...))
	}

	if mainOff, err := findMainHeaderOffset(bytes.NewReader(base), int64(len(base))); err == nil {
		if corrupted, ok := fuzzParserCorruptHeaderCRC(base, mainOff); ok {
			out = append(out, corrupted)
		}
		if resized, ok := fuzzParserMutateHeaderSize(base, mainOff, 0xffff); ok {
			out = append(out, resized)
		}

		if _, _, localOff, err := readHeaderBlock(bytes.NewReader(base), int64(len(base)), mainOff); err == nil {
			if corrupted, ok := fuzzParserCorruptHeaderCRC(base, localOff); ok {
				out = append(out, corrupted)
			}
			if resized, ok := fuzzParserMutateHeaderSize(base, localOff, 0xffff); ok {
				out = append(out, resized)
			}
		}
	}
	return out
}

func fuzzParserCorruptHeaderCRC(src []byte, off int64) ([]byte, bool) {
	out := append([]byte(nil), src...)
	i := int(off)
	if i < 0 || i+4 > len(out) {
		return nil, false
	}
	if out[i] != arjHeaderID1 || out[i+1] != arjHeaderID2 {
		return nil, false
	}
	basicSize := int(binary.LittleEndian.Uint16(out[i+2 : i+4]))
	basicStart := i + 4
	basicEnd := basicStart + basicSize
	if basicSize < arjMinFirstHeaderSize || basicEnd+4 > len(out) {
		return nil, false
	}
	crcIdx := basicEnd
	out[crcIdx] ^= 0x80
	return out, true
}

func fuzzParserMutateHeaderSize(src []byte, off int64, size uint16) ([]byte, bool) {
	out := append([]byte(nil), src...)
	i := int(off)
	if i < 0 || i+4 > len(out) {
		return nil, false
	}
	if out[i] != arjHeaderID1 || out[i+1] != arjHeaderID2 {
		return nil, false
	}
	binary.LittleEndian.PutUint16(out[i+2:i+4], size)
	return out, true
}
