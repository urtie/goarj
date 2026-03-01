package arj

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

const (
	fuzzMultiVolumeMaxPayload = 1 << 16
	fuzzOffsetScanMaxNoise    = 1 << 18
	fuzzOffsetScanMaxPayload  = 1 << 15
)

func FuzzOpenMultiReaderContinuationReconstruction(f *testing.F) {
	f.Add(uint8(0), []byte("joined-payload"))
	f.Add(uint8(1), bytes.Repeat([]byte("abc"), 64))
	f.Add(uint8(1), bytes.Repeat([]byte("abc"), 8192))
	f.Add(uint8(2), []byte("missing-continuation"))
	f.Add(uint8(3), []byte("orphan-ext"))
	f.Add(uint8(4), []byte("name-mismatch"))
	f.Add(uint8(5), []byte("broken-next-volume"))

	f.Fuzz(func(t *testing.T, scenario uint8, payload []byte) {
		if len(payload) > fuzzMultiVolumeMaxPayload {
			return
		}
		if len(payload) == 0 {
			payload = []byte{0}
		}

		tmp := t.TempDir()
		base := filepath.Join(tmp, "set")
		target := "split.bin"

		var (
			openPath    = base + ".arj"
			wantErr     bool
			wantPayload []byte
		)

		switch scenario % 6 {
		case 0:
			parts := fuzzSplitPayload(payload, 2)
			writeVolumeArchive(t, base+".arj", []volumeEntry{
				{name: "head.txt", payload: []byte("head")},
				{name: target, flags: FlagVolume, payload: parts[0]},
			})
			writeVolumeArchive(t, base+".a01", []volumeEntry{
				{name: target, flags: FlagExtFile, payload: parts[1]},
				{name: "tail.txt", payload: []byte("tail")},
			})
			wantPayload = fuzzJoinPayload(parts)
		case 1:
			parts := fuzzSplitPayload(payload, 3)
			writeVolumeArchive(t, base+".arj", []volumeEntry{
				{name: target, flags: FlagVolume, payload: parts[0]},
			})
			writeVolumeArchive(t, base+".a01", []volumeEntry{
				{name: "mid.txt", payload: []byte("mid")},
				{name: target, flags: FlagVolume | FlagExtFile, payload: parts[1]},
			})
			writeVolumeArchive(t, base+".a02", []volumeEntry{
				{name: target, flags: FlagExtFile, payload: parts[2]},
				{name: "end.txt", payload: []byte("end")},
			})
			openPath = base + ".a01"
			wantPayload = fuzzJoinPayload(parts)
		case 2:
			writeVolumeArchive(t, base+".arj", []volumeEntry{
				{name: target, flags: FlagVolume, payload: payload},
			})
			wantErr = true
		case 3:
			writeVolumeArchive(t, base+".arj", []volumeEntry{
				{name: target, flags: FlagExtFile, payload: payload},
			})
			wantErr = true
		case 4:
			parts := fuzzSplitPayload(payload, 2)
			writeVolumeArchive(t, base+".arj", []volumeEntry{
				{name: target, flags: FlagVolume, payload: parts[0]},
			})
			writeVolumeArchive(t, base+".a01", []volumeEntry{
				{name: "other.bin", flags: FlagExtFile, payload: parts[1]},
			})
			wantErr = true
		default:
			parts := fuzzSplitPayload(payload, 2)
			writeVolumeArchive(t, base+".arj", []volumeEntry{
				{name: target, flags: FlagVolume, payload: parts[0]},
			})
			broken := append([]byte("not-an-arj-volume"), fuzzNoiseWithoutSignature(payload, 64)...)
			if err := os.WriteFile(base+".a01", broken, 0o600); err != nil {
				t.Fatalf("WriteFile broken continuation: %v", err)
			}
			openPath = base + ".a01"
			wantErr = true
		}

		multi, err := OpenMultiReader(openPath)
		if wantErr {
			if !errors.Is(err, ErrFormat) {
				t.Fatalf("OpenMultiReader(%s) error = %v, want %v", openPath, err, ErrFormat)
			}
			return
		}
		if err != nil {
			t.Fatalf("OpenMultiReader(%s): %v", openPath, err)
		}
		defer multi.Close()

		var (
			split *File
			count int
		)
		for _, file := range multi.File {
			if file.Name == target {
				count++
				split = file
			}
		}
		if count != 1 || split == nil {
			t.Fatalf("split entry count = %d, want 1", count)
		}

		if split.Flags&(FlagVolume|FlagExtFile) != 0 {
			t.Fatalf("split flags = 0x%02x, want volume/ext bits cleared", split.Flags)
		}
		if got, want := split.UncompressedSize64, uint64(len(wantPayload)); got != want {
			t.Fatalf("uncompressed size = %d, want %d", got, want)
		}
		if got, want := split.CompressedSize64, uint64(len(wantPayload)); got != want {
			t.Fatalf("compressed size = %d, want %d", got, want)
		}

		got := mustReadFileEntry(t, split)
		if !bytes.Equal(got, wantPayload) {
			t.Fatalf("merged payload mismatch")
		}

		raw, err := split.OpenRaw()
		if err != nil {
			t.Fatalf("OpenRaw: %v", err)
		}
		gotRaw, err := io.ReadAll(raw)
		if err != nil {
			t.Fatalf("ReadAll raw: %v", err)
		}
		if !bytes.Equal(gotRaw, wantPayload) {
			t.Fatalf("merged raw payload mismatch")
		}
	})
}

func FuzzFindMainHeaderOffsetCandidateScanning(f *testing.F) {
	f.Add(uint8(0), []byte("MZ-stub"), []byte("tail-data"), []byte("payload"))
	f.Add(uint8(1), bytes.Repeat([]byte{0x7f}, 32), []byte("append"), []byte("x"))
	f.Add(uint8(2), []byte("no-real"), []byte("suffix"), []byte("payload"))
	f.Add(uint8(3), []byte("empty-decoy"), []byte("suffix"), []byte("payload"))
	f.Add(
		uint8(4),
		bytes.Repeat([]byte{arjHeaderID1, arjHeaderID2, 0xff, 0xff}, 4096),
		bytes.Repeat([]byte{arjHeaderID1, arjHeaderID2, 0xff, 0xff}, 1024),
		bytes.Repeat([]byte("payload"), 2048),
	)
	f.Add(
		uint8(5),
		bytes.Repeat([]byte{arjHeaderID1, arjHeaderID2, 0xff, 0xff}, 8192),
		[]byte("dense-suffix"),
		bytes.Repeat([]byte("x"), 1<<14),
	)

	f.Fuzz(func(t *testing.T, scenario uint8, prefix, suffix, payload []byte) {
		if len(prefix) > fuzzOffsetScanMaxNoise || len(suffix) > fuzzOffsetScanMaxNoise || len(payload) > fuzzOffsetScanMaxPayload {
			return
		}
		if len(payload) == 0 {
			payload = []byte{1}
		}

		if scenario%6 >= 4 {
			prefix = fuzzSignatureDenseNoise(prefix, fuzzOffsetScanMaxNoise)
			suffix = fuzzSignatureDenseNoise(suffix, fuzzOffsetScanMaxNoise)
		} else {
			prefix = fuzzNoiseWithoutSignature(prefix, fuzzOffsetScanMaxNoise)
			suffix = fuzzNoiseWithoutSignature(suffix, fuzzOffsetScanMaxNoise)
		}

		real := buildSingleStoreArchive(t, "payload.bin", payload)
		emptyDecoy := buildDecoyMainHeaderArchive(t, "decoy-empty.arj", true)
		badCRCDecoy := fuzzCorruptHeaderCRC(buildDecoyMainHeaderArchive(t, "decoy-badcrc.arj", true))
		badSizedCandidate := fuzzMalformedCandidateFromNoise(payload)

		container := append([]byte(nil), prefix...)
		var (
			emptyOff int64 = -1
			realOff  int64 = -1
			badOffs  []int64
			wantOff  int64
			wantErr  bool
		)

		switch scenario % 6 {
		case 0:
			badOffs = append(badOffs, int64(len(container)))
			container = append(container, badCRCDecoy...)
			badOffs = append(badOffs, int64(len(container)))
			container = append(container, badSizedCandidate...)
			realOff = int64(len(container))
			container = append(container, real...)
			wantOff = realOff
		case 1:
			emptyOff = int64(len(container))
			container = append(container, emptyDecoy...)
			badOffs = append(badOffs, int64(len(container)))
			container = append(container, badCRCDecoy...)
			realOff = int64(len(container))
			container = append(container, real...)
			wantOff = realOff
		case 2:
			badOffs = append(badOffs, int64(len(container)))
			container = append(container, badCRCDecoy...)
			badOffs = append(badOffs, int64(len(container)))
			container = append(container, badSizedCandidate...)
			wantErr = true
		case 3:
			emptyOff = int64(len(container))
			container = append(container, emptyDecoy...)
			badOffs = append(badOffs, int64(len(container)))
			container = append(container, badSizedCandidate...)
			wantOff = emptyOff
		case 4:
			badOffs = append(badOffs, int64(len(container)))
			container = append(container, badCRCDecoy...)
			badOffs = append(badOffs, int64(len(container)))
			container = append(container, badSizedCandidate...)
			badOffs = append(badOffs, int64(len(container)))
			container = append(container, fuzzMalformedCandidateFromNoise(prefix)...)
			realOff = int64(len(container))
			container = append(container, real...)
			wantOff = realOff
		default:
			badOffs = append(badOffs, int64(len(container)))
			container = append(container, badCRCDecoy...)
			badOffs = append(badOffs, int64(len(container)))
			container = append(container, badSizedCandidate...)
			badOffs = append(badOffs, int64(len(container)))
			container = append(container, fuzzMalformedCandidateFromNoise(suffix)...)
			wantErr = true
		}

		container = append(container, suffix...)
		size := int64(len(container))
		reader := bytes.NewReader(container)

		for _, off := range badOffs {
			files, end, ok, perr := probeArchiveLayout(reader, size, off)
			if perr == nil && ok {
				t.Fatalf("probeArchiveLayout bad candidate at %d accepted (files=%d end=%d)", off, files, end)
			}
		}

		if emptyOff >= 0 {
			files, end, ok, perr := probeArchiveLayout(reader, size, emptyOff)
			if perr != nil {
				t.Fatalf("probeArchiveLayout empty decoy: %v", perr)
			}
			if !ok || files != 0 || end <= emptyOff {
				t.Fatalf("probeArchiveLayout empty decoy = (%d,%d,%t), want files=0 and ok", files, end, ok)
			}
		}

		if realOff >= 0 {
			files, end, ok, perr := probeArchiveLayout(reader, size, realOff)
			if perr != nil {
				t.Fatalf("probeArchiveLayout real archive: %v", perr)
			}
			if !ok || files == 0 || end <= realOff {
				t.Fatalf("probeArchiveLayout real archive = (%d,%d,%t), want files>0 and ok", files, end, ok)
			}
		}

		gotOff, err := findMainHeaderOffset(reader, size)
		if wantErr {
			if !errors.Is(err, ErrFormat) {
				t.Fatalf("findMainHeaderOffset error = %v, want %v", err, ErrFormat)
			}
			if _, nerr := NewReader(bytes.NewReader(container), size); !errors.Is(nerr, ErrFormat) {
				t.Fatalf("NewReader malformed error = %v, want %v", nerr, ErrFormat)
			}
			return
		}
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
		if r.BaseOffset() != wantOff {
			t.Fatalf("BaseOffset = %d, want %d", r.BaseOffset(), wantOff)
		}
		if realOff >= 0 && len(r.File) == 0 {
			t.Fatalf("expected parsed files from real archive")
		}
		if realOff < 0 && len(r.File) != 0 {
			t.Fatalf("file count = %d, want 0", len(r.File))
		}
	})
}

func fuzzSplitPayload(payload []byte, parts int) [][]byte {
	if parts <= 0 {
		return nil
	}
	if len(payload) == 0 {
		payload = []byte{0}
	}
	out := make([][]byte, parts)
	for i := 0; i < parts; i++ {
		start := i * len(payload) / parts
		end := (i + 1) * len(payload) / parts
		out[i] = append([]byte(nil), payload[start:end]...)
	}
	return out
}

func fuzzJoinPayload(parts [][]byte) []byte {
	var out []byte
	for _, part := range parts {
		out = append(out, part...)
	}
	return out
}

func fuzzNoiseWithoutSignature(in []byte, max int) []byte {
	if max < 0 {
		max = 0
	}
	if len(in) > max {
		in = in[:max]
	}
	out := append([]byte(nil), in...)
	for i := 0; i+1 < len(out); i++ {
		if out[i] == arjHeaderID1 && out[i+1] == arjHeaderID2 {
			out[i+1] ^= 0x01
		}
	}
	return out
}

func fuzzSignatureDenseNoise(in []byte, max int) []byte {
	if max < 0 {
		max = 0
	}
	if len(in) > max {
		in = in[:max]
	}
	if len(in) == 0 {
		in = []byte{0}
	}

	out := make([]byte, len(in))
	copy(out, in)

	for i := 0; i+3 < len(out); i += 4 {
		out[i] = arjHeaderID1
		out[i+1] = arjHeaderID2
		// Force malformed basic header sizes for candidate rejection.
		out[i+2] = 0xff
		out[i+3] = 0xff
	}
	if len(out)%4 == 1 {
		out[len(out)-1] = arjHeaderID1
	}
	return out
}

func fuzzCorruptHeaderCRC(archive []byte) []byte {
	out := append([]byte(nil), archive...)
	if len(out) < 8 || out[0] != arjHeaderID1 || out[1] != arjHeaderID2 {
		return out
	}
	basicSize := int(binary.LittleEndian.Uint16(out[2:4]))
	crcOff := 4 + basicSize
	if basicSize < arjMinFirstHeaderSize || crcOff+4 > len(out) {
		out[2], out[3] = 0xff, 0xff
		return out
	}
	out[crcOff] ^= 0x01
	return out
}

func fuzzMalformedCandidateFromNoise(noise []byte) []byte {
	tail := fuzzNoiseWithoutSignature(noise, 32)
	size := uint16(arjMaxBasicHeaderSize + 1)
	out := []byte{arjHeaderID1, arjHeaderID2, byte(size), byte(size >> 8)}
	out = append(out, tail...)
	if len(out) < 6 {
		out = append(out, 'N', 'O')
	}
	return out
}
