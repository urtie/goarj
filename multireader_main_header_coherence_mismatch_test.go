package arj

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOpenMultiReaderRejectsMainHeaderCoherenceMismatchMatrix(t *testing.T) {
	cases := []struct {
		name      string
		wantField string
		mutate    func(*ArchiveHeader)
	}{
		{
			name:      "main_extended_headers",
			wantField: "MainExtendedHeaders",
			mutate: func(h *ArchiveHeader) {
				h.MainExtendedHeaders[0][0] ^= 0x01
			},
		},
		{
			name:      "first_header_extra",
			wantField: "FirstHeaderExtra",
			mutate: func(h *ArchiveHeader) {
				h.FirstHeaderExtra[5] ^= 0x01
			},
		},
		{
			name:      "security_version",
			wantField: "SecurityVersion",
			mutate: func(h *ArchiveHeader) {
				h.SecurityVersion ^= 0x01
			},
		},
		{
			name:      "security_envelope_pos",
			wantField: "SecurityEnvelopePos",
			mutate: func(h *ArchiveHeader) {
				h.SecurityEnvelopePos++
			},
		},
		{
			name:      "security_envelope_size",
			wantField: "SecurityEnvelopeSize",
			mutate: func(h *ArchiveHeader) {
				h.SecurityEnvelopeSize++
			},
		},
		{
			name:      "ext_flags",
			wantField: "ExtFlags",
			mutate: func(h *ArchiveHeader) {
				h.ExtFlags ^= 0x01
			},
		},
		{
			name:      "protection_flags_via_first_header_extra",
			wantField: "FirstHeaderExtra",
			mutate: func(h *ArchiveHeader) {
				h.ProtectionFlags ^= 0x20
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			base := filepath.Join(tmp, "matrix")

			baseHeader := testMainHeaderCoherenceBase()
			writeVolumeArchiveWithHeader(t, base+".arj", baseHeader, []volumeEntry{
				{name: "joined.bin", flags: FlagVolume, payload: []byte("left-")},
			})

			secondHeader := cloneArchiveHeader(baseHeader)
			tc.mutate(&secondHeader)
			writeVolumeArchiveWithHeader(t, base+".a01", secondHeader, []volumeEntry{
				{name: "joined.bin", flags: FlagExtFile, payload: []byte("right")},
			})

			_, err := OpenMultiReader(base + ".arj")
			if !errors.Is(err, ErrFormat) {
				t.Fatalf("OpenMultiReader error = %v, want %v", err, ErrFormat)
			}
			if err == nil || !strings.Contains(err.Error(), "inconsistent multi-volume main header") {
				t.Fatalf("OpenMultiReader error = %v, want inconsistent multi-volume main header", err)
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantField) {
				t.Fatalf("OpenMultiReader error = %v, want field %q", err, tc.wantField)
			}
		})
	}
}

func testMainHeaderCoherenceBase() ArchiveHeader {
	return ArchiveHeader{
		FirstHeaderSize:      arjMinFirstHeaderSize + 6,
		ArchiverVersion:      arjVersionCurrent,
		MinVersion:           arjVersionNeeded,
		HostOS:               currentHostOS(),
		FileType:             arjFileTypeMain,
		SecurityVersion:      2,
		SecurityEnvelopePos:  0x10203040,
		SecurityEnvelopeSize: 0x0102,
		ExtFlags:             0x04,
		ProtectionBlocks:     3,
		ProtectionFlags:      ProtectionFlagSFXStub,
		ProtectionReserved:   0x3344,
		FirstHeaderExtra:     []byte{0x00, 0x00, 0x00, 0x00, 0xaa, 0xbb},
		MainExtendedHeaders: [][]byte{
			{0x10, 0x20, 0x30},
			{0x40, 0x50},
		},
	}
}

func writeVolumeArchiveWithHeader(t *testing.T, path string, hdr ArchiveHeader, entries []volumeEntry) {
	t.Helper()

	var buf bytes.Buffer
	w := NewWriter(&buf)

	header := cloneArchiveHeader(hdr)
	if err := w.SetArchiveHeader(&header); err != nil {
		t.Fatalf("SetArchiveHeader(%s): %v", path, err)
	}

	for _, entry := range entries {
		fw, err := w.CreateHeader(&FileHeader{
			Name:   entry.name,
			Method: Store,
			Flags:  entry.flags,
		})
		if err != nil {
			t.Fatalf("CreateHeader(%s): %v", entry.name, err)
		}
		if _, err := fw.Write(entry.payload); err != nil {
			t.Fatalf("Write(%s): %v", entry.name, err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close writer for %s: %v", path, err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}
