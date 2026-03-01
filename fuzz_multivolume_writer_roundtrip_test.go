package arj

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
)

const (
	fuzzMVWriterRoundTripMaxSeedBytes = 1 << 10
	fuzzMVWriterRoundTripMaxPayload   = 1 << 15
	fuzzMVWriterRoundTripMinVolume    = 512
	fuzzMVWriterRoundTripVolumeRange  = 3584
)

var fuzzMVWriterMethods = [...]uint16{Store, Method1, Method2, Method3, Method4}

type fuzzMVWriterFileSpec struct {
	name    string
	method  uint16
	payload []byte
}

func FuzzMultiVolumeWriterRoundTrip(f *testing.F) {
	for _, method := range []uint8{0, 1, 2, 3, 4} {
		f.Add(method, uint16(0), []byte("seed"))
		f.Add(method, uint16(511), bytes.Repeat([]byte("a"), 64))
		f.Add(method, uint16(1337), bytes.Repeat([]byte{0x00, 0xff, 0x13, 0x7a}, 128))
	}

	f.Fuzz(func(t *testing.T, methodByte uint8, volumeHint uint16, seed []byte) {
		if len(seed) > fuzzMVWriterRoundTripMaxSeedBytes {
			return
		}
		if len(seed) == 0 {
			seed = []byte{0}
		}

		volumeSize := fuzzMVWriterRoundTripMinVolume + int(volumeHint)%fuzzMVWriterRoundTripVolumeRange
		methodBase := int(methodByte) % len(fuzzMVWriterMethods)
		specs := fuzzMVWriterRoundTripSpecs(seed, volumeSize, methodBase)

		tmp := t.TempDir()
		archivePath := filepath.Join(tmp, "roundtrip.arj")
		mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: int64(volumeSize)})
		if err != nil {
			t.Fatalf("NewMultiVolumeWriter: %v", err)
		}

		for _, spec := range specs {
			fw, err := mw.CreateHeader(&FileHeader{Name: spec.name, Method: spec.method})
			if err != nil {
				t.Fatalf("CreateHeader(%q): %v", spec.name, err)
			}
			if _, err := fw.Write(spec.payload); err != nil {
				t.Fatalf("Write(%q): %v", spec.name, err)
			}
		}
		if err := mw.Close(); err != nil {
			t.Fatalf("Close multi-volume writer: %v", err)
		}

		paths, err := VolumePaths(archivePath)
		if err != nil {
			t.Fatalf("VolumePaths: %v", err)
		}
		if got, min := len(paths), 2; got < min {
			t.Fatalf("volume path count = %d, want >= %d", got, min)
		}

		mr, err := OpenMultiReader(archivePath)
		if err != nil {
			t.Fatalf("OpenMultiReader: %v", err)
		}
		defer mr.Close()

		if got, want := len(mr.File), len(specs); got != want {
			t.Fatalf("file count = %d, want %d", got, want)
		}

		for i, spec := range specs {
			gotFile := mr.File[i]
			if gotFile.Name != spec.name {
				t.Fatalf("entry name(%d) = %q, want %q", i, gotFile.Name, spec.name)
			}
			if gotFile.Flags&(FlagVolume|FlagExtFile) != 0 {
				t.Fatalf("entry flags(%q) = 0x%02x, want continuation bits cleared", gotFile.Name, gotFile.Flags)
			}
			gotPayload := mustReadFileEntry(t, gotFile)
			if !bytes.Equal(gotPayload, spec.payload) {
				t.Fatalf("payload mismatch for %q", spec.name)
			}
		}
	})
}

func fuzzMVWriterRoundTripSpecs(seed []byte, volumeSize int, methodBase int) []fuzzMVWriterFileSpec {
	fileCount := 2 + int(fuzzMVWriterByte(seed, 0)%3) // [2,4]
	specs := make([]fuzzMVWriterFileSpec, 0, fileCount)

	largeSize := volumeSize*2 + 1 + int(fuzzMVWriterByte(seed, 1)%251)
	if largeSize <= volumeSize {
		largeSize = volumeSize + 1
	}
	if largeSize > fuzzMVWriterRoundTripMaxPayload {
		largeSize = fuzzMVWriterRoundTripMaxPayload
	}
	specs = append(specs, fuzzMVWriterFileSpec{
		name:    "split-00.bin",
		method:  fuzzMVWriterMethods[methodBase],
		payload: fuzzMVWriterDeterministicBytes(seed, 0, largeSize),
	})

	for i := 1; i < fileCount; i++ {
		size := 1 + int(fuzzMVWriterByte(seed, i+2))%1536
		if size > fuzzMVWriterRoundTripMaxPayload/8 {
			size = fuzzMVWriterRoundTripMaxPayload / 8
		}
		specs = append(specs, fuzzMVWriterFileSpec{
			name:    fmt.Sprintf("dir%02d/file%02d.bin", i, i),
			method:  fuzzMVWriterMethods[(methodBase+i)%len(fuzzMVWriterMethods)],
			payload: fuzzMVWriterDeterministicBytes(seed, byte(i), size),
		})
	}

	return specs
}

func fuzzMVWriterDeterministicBytes(seed []byte, salt byte, n int) []byte {
	if n <= 0 {
		return nil
	}
	if len(seed) == 0 {
		seed = []byte{0}
	}

	out := make([]byte, n)
	var state uint64 = 1469598103934665603
	for _, b := range seed {
		state ^= uint64(b)
		state *= 1099511628211
	}
	state ^= uint64(salt)<<32 | uint64(n)
	for i := range out {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		out[i] = byte(state)
	}
	return out
}

func fuzzMVWriterByte(seed []byte, idx int) byte {
	if len(seed) == 0 {
		return byte(idx*37 + 11)
	}
	return seed[idx%len(seed)]
}
