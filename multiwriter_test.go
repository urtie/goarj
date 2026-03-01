package arj

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewMultiVolumeWriterPartNaming(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantFirst string
		wantPart  string
	}{
		{name: "lowercase ext", input: "bundle.arj", wantFirst: "bundle.arj", wantPart: ".a"},
		{name: "uppercase ext", input: "bundle.ARJ", wantFirst: "bundle.ARJ", wantPart: ".A"},
		{name: "no ext", input: "bundle", wantFirst: "bundle.arj", wantPart: ".a"},
	}

	payload := bytes.Repeat([]byte("naming-check-"), 800)
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			firstPath := filepath.Join(tmp, tc.input)

			mw, err := NewMultiVolumeWriter(firstPath, MultiVolumeWriterOptions{VolumeSize: 512})
			if err != nil {
				t.Fatalf("NewMultiVolumeWriter: %v", err)
			}
			fw, err := mw.Create("payload.bin")
			if err != nil {
				t.Fatalf("Create: %v", err)
			}
			if _, err := fw.Write(payload); err != nil {
				t.Fatalf("Write: %v", err)
			}
			if err := mw.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			parts := mw.Parts()
			if len(parts) < 2 {
				t.Fatalf("parts len = %d, want >= 2", len(parts))
			}

			wantFirstPath := filepath.Join(tmp, tc.wantFirst)
			if got := parts[0]; got != wantFirstPath {
				t.Fatalf("first part path = %q, want %q", got, wantFirstPath)
			}

			stem := strings.TrimSuffix(wantFirstPath, filepath.Ext(wantFirstPath))
			for i := 1; i < len(parts); i++ {
				want := fmt.Sprintf("%s%s%02d", stem, tc.wantPart, i)
				if got := parts[i]; got != want {
					t.Fatalf("part %d path = %q, want %q", i, got, want)
				}
				if _, err := os.Stat(parts[i]); err != nil {
					t.Fatalf("Stat(%s): %v", parts[i], err)
				}
			}
		})
	}
}

func TestNewMultiVolumeWriterInvalidOptions(t *testing.T) {
	if _, err := NewMultiVolumeWriter("bad.zip", MultiVolumeWriterOptions{VolumeSize: 1024}); !errors.Is(err, ErrInvalidMultiVolumePath) {
		t.Fatalf("NewMultiVolumeWriter path error = %v, want %v", err, ErrInvalidMultiVolumePath)
	}
	if _, err := NewMultiVolumeWriter("ok.arj", MultiVolumeWriterOptions{VolumeSize: 0}); !errors.Is(err, ErrInvalidMultiVolumeSize) {
		t.Fatalf("NewMultiVolumeWriter size error = %v, want %v", err, ErrInvalidMultiVolumeSize)
	}
}

func TestMultiVolumeWriterPartNamingBeyondA99(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "triple.arj")
	payload := bytes.Repeat([]byte("triple-digit-part-check-"), 9000)

	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 768})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}
	fw, err := mw.Create("payload.bin")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := fw.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	parts := mw.Parts()
	if got, min := len(parts), 101; got < min {
		t.Fatalf("parts len = %d, want >= %d", got, min)
	}
	if got, want := parts[99], filepath.Join(tmp, "triple.a99"); got != want {
		t.Fatalf("part 99 = %q, want %q", got, want)
	}
	if got, want := parts[100], filepath.Join(tmp, "triple.a100"); got != want {
		t.Fatalf("part 100 = %q, want %q", got, want)
	}
}

func TestMultiVolumeWriterContinuationFlags(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "flags.arj")
	payload := bytes.Repeat([]byte("0123456789abcdef"), 3000)

	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 1024})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}

	fw, err := mw.Create("split.bin")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := fw.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	paths, err := VolumePaths(archivePath)
	if err != nil {
		t.Fatalf("VolumePaths: %v", err)
	}
	if len(paths) < 3 {
		t.Fatalf("volume count = %d, want >= 3", len(paths))
	}

	var total uint64
	var expectedResume uint64
	for i, path := range paths {
		rc, err := OpenReader(path)
		if err != nil {
			t.Fatalf("OpenReader(%s): %v", path, err)
		}
		if got, want := len(rc.File), 1; got != want {
			_ = rc.Close()
			t.Fatalf("%s file count = %d, want %d", path, got, want)
		}

		f := rc.File[0]
		total += f.UncompressedSize64
		hasVol := f.Flags&FlagVolume != 0
		hasExt := f.Flags&FlagExtFile != 0
		switch {
		case i == 0:
			if !hasVol || hasExt {
				_ = rc.Close()
				t.Fatalf("first segment flags = 0x%02x, want volume only", f.Flags)
			}
			if got, want := f.FirstHeaderSize, uint8(arjMinFirstHeaderSize); got != want {
				_ = rc.Close()
				t.Fatalf("first segment first header size = %d, want %d", got, want)
			}
			if len(f.firstHeaderExtra) != 0 {
				_ = rc.Close()
				t.Fatalf("first segment header extra len = %d, want 0", len(f.firstHeaderExtra))
			}
		case i == len(paths)-1:
			if hasVol || !hasExt {
				_ = rc.Close()
				t.Fatalf("last segment flags = 0x%02x, want extfile only", f.Flags)
			}
		default:
			if !hasVol || !hasExt {
				_ = rc.Close()
				t.Fatalf("middle segment flags = 0x%02x, want volume+extfile", f.Flags)
			}
		}
		if hasExt {
			if f.FirstHeaderSize < arjMinFirstHeaderSize+4 {
				_ = rc.Close()
				t.Fatalf("%s first header size = %d, want >= %d", path, f.FirstHeaderSize, arjMinFirstHeaderSize+4)
			}
			if len(f.firstHeaderExtra) < 4 {
				_ = rc.Close()
				t.Fatalf("%s first header extra len = %d, want >= 4", path, len(f.firstHeaderExtra))
			}
			if got, want := uint64(binary.LittleEndian.Uint32(f.firstHeaderExtra[:4])), expectedResume; got != want {
				_ = rc.Close()
				t.Fatalf("%s resume position = %d, want %d", path, got, want)
			}
		}
		expectedResume += f.UncompressedSize64
		if err := rc.Close(); err != nil {
			t.Fatalf("Close reader %s: %v", path, err)
		}
	}
	if got, want := total, uint64(len(payload)); got != want {
		t.Fatalf("segment total size = %d, want %d", got, want)
	}
}

func TestMultiVolumeWriterRoundTripOpenMultiReader(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "roundtrip.arj")

	rng := rand.New(rand.NewSource(1234))
	compressedPayload := make([]byte, 64*1024)
	if _, err := rng.Read(compressedPayload); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}

	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 8192})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}

	fw, err := mw.Create("head.txt")
	if err != nil {
		t.Fatalf("Create head.txt: %v", err)
	}
	if _, err := fw.Write([]byte("head")); err != nil {
		t.Fatalf("Write head.txt: %v", err)
	}

	fw, err = mw.CreateHeader(&FileHeader{Name: "compressed.bin", Method: Method1})
	if err != nil {
		t.Fatalf("CreateHeader compressed.bin: %v", err)
	}
	if _, err := fw.Write(compressedPayload); err != nil {
		t.Fatalf("Write compressed.bin: %v", err)
	}

	fw, err = mw.Create("tail.txt")
	if err != nil {
		t.Fatalf("Create tail.txt: %v", err)
	}
	if _, err := fw.Write([]byte("tail")); err != nil {
		t.Fatalf("Write tail.txt: %v", err)
	}

	if err := mw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := os.Stat(filepath.Join(tmp, "roundtrip.a01")); err != nil {
		t.Fatalf("expected continuation volume roundtrip.a01: %v", err)
	}

	mr, err := OpenMultiReader(archivePath)
	if err != nil {
		t.Fatalf("OpenMultiReader: %v", err)
	}
	defer mr.Close()

	if got, want := len(mr.File), 3; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}

	if got := mustReadFileEntry(t, mr.File[0]); string(got) != "head" {
		t.Fatalf("head payload = %q, want %q", got, "head")
	}
	if got := mustReadFileEntry(t, mr.File[1]); !bytes.Equal(got, compressedPayload) {
		t.Fatalf("compressed payload mismatch")
	}
	if got := mustReadFileEntry(t, mr.File[2]); string(got) != "tail" {
		t.Fatalf("tail payload = %q, want %q", got, "tail")
	}
}

func TestInteropARJBinaryExtractsMultiVolumeWriterArchive(t *testing.T) {
	arjPath := requireInteropARJBinary(t)

	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "interop-split.arj")
	payload := bytes.Repeat([]byte("interop-split-payload-"), 4000)

	mw, err := NewMultiVolumeWriter(archivePath, MultiVolumeWriterOptions{VolumeSize: 10 * 1024})
	if err != nil {
		t.Fatalf("NewMultiVolumeWriter: %v", err)
	}
	fw, err := mw.Create("payload.bin")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := fw.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := os.Stat(filepath.Join(tmp, "interop-split.a01")); err != nil {
		t.Fatalf("expected continuation volume interop-split.a01: %v", err)
	}

	extractDir := filepath.Join(tmp, "extract")
	if err := os.Mkdir(extractDir, 0o755); err != nil {
		t.Fatalf("Mkdir(%s): %v", extractDir, err)
	}

	runInteropARJCommand(t, arjPath, "x", "-y", "-i", "-v", archivePath, "-ht"+extractDir)

	extractedPath := filepath.Join(extractDir, "payload.bin")
	got, err := os.ReadFile(extractedPath)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", extractedPath, err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("arj extracted payload mismatch")
	}
}
