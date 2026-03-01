package arj

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"testing"
)

func TestSecurityModesRequirePasswordForGarbledEntryOnOpen(t *testing.T) {
	archive := writeSecurityModeArchive(t)
	localOff := firstLocalHeaderOffset(t, archive)
	archive = addHeaderFlags(t, archive, localOff, FlagGarbled)

	r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, err = r.File[0].Open()
	if !errors.Is(err, ErrPasswordRequired) {
		t.Fatalf("Open error = %v, want %v", err, ErrPasswordRequired)
	}
	assertNoUnsupportedModeError(t, err)
}

func TestSecurityModesRejectSecuredEntryOnOpen(t *testing.T) {
	archive := writeSecurityModeArchive(t)
	localOff := firstLocalHeaderOffset(t, archive)
	archive = addHeaderFlags(t, archive, localOff, FlagSecured)

	r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, err = r.File[0].Open()
	if !errors.Is(err, ErrSecurityMode) {
		t.Fatalf("Open error = %v, want %v", err, ErrSecurityMode)
	}
	assertUnsupportedModeError(t, err, UnsupportedModeKindSecured, UnsupportedSecurityModeEnvelope, FlagSecured, 0)
}

func TestSecurityModesRejectSignatureEntryOnOpen(t *testing.T) {
	archive := writeSecurityModeArchive(t)
	localOff := firstLocalHeaderOffset(t, archive)
	archive = addHeaderFlags(t, archive, localOff, FlagOldSecured)

	r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, err = r.File[0].Open()
	if !errors.Is(err, ErrSecurityMode) {
		t.Fatalf("Open error = %v, want %v", err, ErrSecurityMode)
	}
	assertUnsupportedModeError(t, err, UnsupportedModeKindSecured, UnsupportedSecurityModeSignature, FlagOldSecured, 0)
}

func TestSecurityModesRejectMainSecuredModeOnOpen(t *testing.T) {
	archive := writeSecurityModeArchive(t)
	mainOff, err := findMainHeaderOffset(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("findMainHeaderOffset: %v", err)
	}
	archive = addHeaderFlags(t, archive, mainOff, FlagSecured)

	r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if !r.ArchiveHeader.HasSecurityEnvelope() {
		t.Fatalf("ArchiveHeader.HasSecurityEnvelope = false, want true")
	}
	_, err = r.File[0].Open()
	if !errors.Is(err, ErrSecurityMode) {
		t.Fatalf("Open error = %v, want %v", err, ErrSecurityMode)
	}
	assertUnsupportedModeError(t, err, UnsupportedModeKindSecured, UnsupportedSecurityModeEnvelope, FlagSecured, 0)
}

func TestSecurityModesRejectMainProtectionModeOnOpen(t *testing.T) {
	archive := writeSecurityModeArchive(t)
	mainOff, err := findMainHeaderOffset(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("findMainHeaderOffset: %v", err)
	}
	archive = addHeaderFlags(t, archive, mainOff, FlagProtection)

	r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if !r.ArchiveHeader.HasProtectionData() {
		t.Fatalf("ArchiveHeader.HasProtectionData = false, want true")
	}
	_, err = r.File[0].Open()
	if !errors.Is(err, ErrSecurityMode) {
		t.Fatalf("Open error = %v, want %v", err, ErrSecurityMode)
	}
	assertUnsupportedModeError(t, err, UnsupportedModeKindSecured, UnsupportedSecurityModeProtection, FlagProtection, 0)
}

func TestSecurityModesEncryptedTakesPrecedence(t *testing.T) {
	archive := writeSecurityModeArchive(t)
	localOff := firstLocalHeaderOffset(t, archive)
	archive = addHeaderFlags(t, archive, localOff, FlagGarbled|FlagSecured|FlagOldSecured)

	r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, err = r.File[0].Open()
	if !errors.Is(err, ErrPasswordRequired) {
		t.Fatalf("Open error = %v, want %v", err, ErrPasswordRequired)
	}
	assertNoUnsupportedModeError(t, err)
}

func TestSecurityModesEnvelopePrecedesSignatureOnOpen(t *testing.T) {
	archive := writeSecurityModeArchive(t)
	localOff := firstLocalHeaderOffset(t, archive)
	archive = addHeaderFlags(t, archive, localOff, FlagSecured|FlagOldSecured)

	r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, err = r.File[0].Open()
	if !errors.Is(err, ErrSecurityMode) {
		t.Fatalf("Open error = %v, want %v", err, ErrSecurityMode)
	}
	assertUnsupportedModeError(t, err, UnsupportedModeKindSecured, UnsupportedSecurityModeEnvelope, FlagSecured|FlagOldSecured, 0)
}

func TestSecurityModesMainEnvelopePrecedesSignatureAndProtectionOnOpen(t *testing.T) {
	archive := writeSecurityModeArchive(t)
	mainOff, err := findMainHeaderOffset(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("findMainHeaderOffset: %v", err)
	}
	archive = addHeaderFlags(t, archive, mainOff, FlagSecured|FlagOldSecured|FlagProtection)

	r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if !r.ArchiveHeader.HasSecurityEnvelope() {
		t.Fatalf("ArchiveHeader.HasSecurityEnvelope = false, want true")
	}
	if !r.ArchiveHeader.HasSecuritySignature() {
		t.Fatalf("ArchiveHeader.HasSecuritySignature = false, want true")
	}
	if !r.ArchiveHeader.HasProtectionData() {
		t.Fatalf("ArchiveHeader.HasProtectionData = false, want true")
	}
	_, err = r.File[0].Open()
	if !errors.Is(err, ErrSecurityMode) {
		t.Fatalf("Open error = %v, want %v", err, ErrSecurityMode)
	}
	assertUnsupportedModeError(t, err, UnsupportedModeKindSecured, UnsupportedSecurityModeEnvelope, FlagSecured|FlagOldSecured|FlagProtection, 0)
}

func TestSecurityModesMainSignaturePrecedesProtectionOnOpen(t *testing.T) {
	archive := writeSecurityModeArchive(t)
	mainOff, err := findMainHeaderOffset(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("findMainHeaderOffset: %v", err)
	}
	archive = addHeaderFlags(t, archive, mainOff, FlagOldSecured|FlagProtection)

	r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if !r.ArchiveHeader.HasSecuritySignature() {
		t.Fatalf("ArchiveHeader.HasSecuritySignature = false, want true")
	}
	if !r.ArchiveHeader.HasProtectionData() {
		t.Fatalf("ArchiveHeader.HasProtectionData = false, want true")
	}
	_, err = r.File[0].Open()
	if !errors.Is(err, ErrSecurityMode) {
		t.Fatalf("Open error = %v, want %v", err, ErrSecurityMode)
	}
	assertUnsupportedModeError(t, err, UnsupportedModeKindSecured, UnsupportedSecurityModeSignature, FlagOldSecured|FlagProtection, 0)
}

func TestSecurityModesLocalFlagsPrecedeMainFlagsOnOpen(t *testing.T) {
	archive := writeSecurityModeArchive(t)
	mainOff, err := findMainHeaderOffset(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("findMainHeaderOffset: %v", err)
	}
	localOff := firstLocalHeaderOffset(t, archive)
	archive = addHeaderFlags(t, archive, mainOff, FlagSecured|FlagProtection)
	archive = addHeaderFlags(t, archive, localOff, FlagOldSecured)

	r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, err = r.File[0].Open()
	if !errors.Is(err, ErrSecurityMode) {
		t.Fatalf("Open error = %v, want %v", err, ErrSecurityMode)
	}
	assertUnsupportedModeError(t, err, UnsupportedModeKindSecured, UnsupportedSecurityModeSignature, FlagOldSecured, 0)
}

func TestSecurityModesLocalEncryptedPrecedesMainSecuredOnOpen(t *testing.T) {
	archive := writeSecurityModeArchive(t)
	mainOff, err := findMainHeaderOffset(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("findMainHeaderOffset: %v", err)
	}
	localOff := firstLocalHeaderOffset(t, archive)
	archive = addHeaderFlags(t, archive, mainOff, FlagSecured|FlagOldSecured|FlagProtection)
	archive = addHeaderFlags(t, archive, localOff, FlagGarbled)

	r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, err = r.File[0].Open()
	if !errors.Is(err, ErrPasswordRequired) {
		t.Fatalf("Open error = %v, want %v", err, ErrPasswordRequired)
	}
	assertNoUnsupportedModeError(t, err)
}

func TestSecurityModesRejectSecuredDirectoryOnOpenAndExtract(t *testing.T) {
	archive := writeSecurityModeDirectoryArchive(t)
	localOff := firstLocalHeaderOffset(t, archive)
	archive = addHeaderFlags(t, archive, localOff, FlagSecured)

	r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if got, want := len(r.File), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	if !r.File[0].isDir() {
		t.Fatalf("entry isDir = false, want true")
	}

	_, err = r.File[0].Open()
	if !errors.Is(err, ErrSecurityMode) {
		t.Fatalf("Open error = %v, want %v", err, ErrSecurityMode)
	}
	assertUnsupportedModeError(t, err, UnsupportedModeKindSecured, UnsupportedSecurityModeEnvelope, FlagSecured, 0)

	err = r.ExtractAll(t.TempDir())
	if !errors.Is(err, ErrSecurityMode) {
		t.Fatalf("ExtractAll error = %v, want %v", err, ErrSecurityMode)
	}
	assertUnsupportedModeError(t, err, UnsupportedModeKindSecured, UnsupportedSecurityModeEnvelope, FlagSecured, 0)
}

func TestSecurityModesRejectMainSecuredDirectoryArchiveOnExtract(t *testing.T) {
	archive := writeSecurityModeDirectoryArchive(t)
	mainOff, err := findMainHeaderOffset(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("findMainHeaderOffset: %v", err)
	}
	archive = addHeaderFlags(t, archive, mainOff, FlagSecured)

	r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	err = r.ExtractAll(t.TempDir())
	if !errors.Is(err, ErrSecurityMode) {
		t.Fatalf("ExtractAll error = %v, want %v", err, ErrSecurityMode)
	}
	assertUnsupportedModeError(t, err, UnsupportedModeKindSecured, UnsupportedSecurityModeEnvelope, FlagSecured, 0)
}

func TestSecurityModesRejectUnsupportedFlagsOnCreateHeader(t *testing.T) {
	for _, tc := range localSecurityFlagCases() {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf)

			_, err := w.CreateHeader(&FileHeader{Name: "bad.bin", Flags: tc.flags})
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("CreateHeader error = %v, want %v", err, tc.wantErr)
			}
			assertUnsupportedModeError(t, err, tc.wantKind, tc.wantSecurityMode, tc.flags, tc.wantVersion)
			if got := buf.Len(); got != 0 {
				t.Fatalf("written bytes = %d, want 0", got)
			}
		})
	}
}

func TestSecurityModesRejectUnsupportedFlagsOnCreateRaw(t *testing.T) {
	for _, tc := range localSecurityFlagCases() {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf)

			_, err := w.CreateRaw(&FileHeader{Name: "bad.bin", Flags: tc.flags})
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("CreateRaw error = %v, want %v", err, tc.wantErr)
			}
			assertUnsupportedModeError(t, err, tc.wantKind, tc.wantSecurityMode, tc.flags, tc.wantVersion)
			if got := buf.Len(); got != 0 {
				t.Fatalf("written bytes = %d, want 0", got)
			}
		})
	}
}

func TestSecurityModesRejectUnsupportedFlagsOnCopy(t *testing.T) {
	for _, tc := range localSecurityFlagCases() {
		t.Run(tc.name, func(t *testing.T) {
			archive := writeSecurityModeArchive(t)
			localOff := firstLocalHeaderOffset(t, archive)
			archive = addHeaderFlags(t, archive, localOff, tc.flags)

			srcReader, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
			if err != nil {
				t.Fatalf("NewReader source: %v", err)
			}

			var dst bytes.Buffer
			dw := NewWriter(&dst)
			err = dw.Copy(srcReader.File[0])
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Copy error = %v, want %v", err, tc.wantErr)
			}
			assertUnsupportedModeError(t, err, tc.wantKind, tc.wantSecurityMode, tc.flags, tc.wantVersion)
			if got := dst.Len(); got != 0 {
				t.Fatalf("written bytes = %d, want 0", got)
			}
		})
	}
}

func TestSecurityModesRejectUnsupportedMainFlagsOnCopy(t *testing.T) {
	for _, tc := range mainSecurityFlagCases() {
		t.Run(tc.name, func(t *testing.T) {
			archive := writeSecurityModeArchive(t)
			mainOff, err := findMainHeaderOffset(bytes.NewReader(archive), int64(len(archive)))
			if err != nil {
				t.Fatalf("findMainHeaderOffset: %v", err)
			}
			archive = addHeaderFlags(t, archive, mainOff, tc.flags)

			srcReader, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
			if err != nil {
				t.Fatalf("NewReader source: %v", err)
			}

			var dst bytes.Buffer
			dw := NewWriter(&dst)
			err = dw.Copy(srcReader.File[0])
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Copy error = %v, want %v", err, tc.wantErr)
			}
			assertUnsupportedModeError(t, err, tc.wantKind, tc.wantSecurityMode, tc.flags, tc.wantVersion)
			if got := dst.Len(); got != 0 {
				t.Fatalf("written bytes = %d, want 0", got)
			}
		})
	}
}

func TestSecurityModesRejectUnsupportedFlagsOnSetArchiveHeader(t *testing.T) {
	for _, tc := range mainSecurityFlagCases() {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf)
			err := w.SetArchiveHeader(&ArchiveHeader{
				FirstHeaderSize: arjMinFirstHeaderSize,
				FileType:        arjFileTypeMain,
				Flags:           tc.flags,
				Name:            "archive.arj",
			})
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("SetArchiveHeader error = %v, want %v", err, tc.wantErr)
			}
			assertUnsupportedModeError(t, err, tc.wantKind, tc.wantSecurityMode, tc.flags, tc.wantVersion)
			if got := buf.Len(); got != 0 {
				t.Fatalf("written bytes = %d, want 0", got)
			}
		})
	}
}

func TestSecurityModesRejectUnsupportedFlagsOnWriteMainHeader(t *testing.T) {
	for _, tc := range mainSecurityFlagCases() {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf)

			h := w.mainHeaderForWrite()
			h.Flags = tc.flags
			w.archiveHdr = &h

			err := w.writeMainHeader()
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("writeMainHeader error = %v, want %v", err, tc.wantErr)
			}
			assertUnsupportedModeError(t, err, tc.wantKind, tc.wantSecurityMode, tc.flags, tc.wantVersion)
			if got := buf.Len(); got != 0 {
				t.Fatalf("written bytes = %d, want 0", got)
			}
		})
	}
}

func writeSecurityModeArchive(t *testing.T) []byte {
	t.Helper()

	var buf bytes.Buffer
	w := NewWriter(&buf)
	fw, err := w.Create("secure.bin")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := io.WriteString(fw, "payload"); err != nil {
		t.Fatalf("WriteString: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	return append([]byte(nil), buf.Bytes()...)
}

func writeSecurityModeDirectoryArchive(t *testing.T) []byte {
	t.Helper()

	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.CreateHeader(&FileHeader{Name: "secure-dir/"}); err != nil {
		t.Fatalf("CreateHeader: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	return append([]byte(nil), buf.Bytes()...)
}

func firstLocalHeaderOffset(t *testing.T, archive []byte) int64 {
	t.Helper()

	mainOff, err := findMainHeaderOffset(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("findMainHeaderOffset: %v", err)
	}
	_, _, next, err := readHeaderBlock(bytes.NewReader(archive), int64(len(archive)), mainOff)
	if err != nil {
		t.Fatalf("readHeaderBlock(main): %v", err)
	}
	return next
}

func addHeaderFlags(t *testing.T, archive []byte, off int64, flags uint8) []byte {
	t.Helper()

	out := append([]byte(nil), archive...)
	i := int(off)
	if i < 0 || i+4 > len(out) {
		t.Fatalf("header offset out of range: %d", i)
	}
	if out[i] != arjHeaderID1 || out[i+1] != arjHeaderID2 {
		t.Fatalf("invalid header signature at offset %d", i)
	}
	basicSize := int(binary.LittleEndian.Uint16(out[i+2 : i+4]))
	basicStart := i + 4
	basicEnd := basicStart + basicSize
	if basicSize <= 0 || basicEnd+4 > len(out) {
		t.Fatalf("invalid basic header bounds at offset %d", i)
	}
	out[basicStart+4] |= flags
	binary.LittleEndian.PutUint32(out[basicEnd:basicEnd+4], crc32.ChecksumIEEE(out[basicStart:basicEnd]))
	return out
}

type securityFlagCase struct {
	name             string
	flags            uint8
	wantErr          error
	wantKind         UnsupportedModeKind
	wantSecurityMode UnsupportedSecurityMode
	wantVersion      uint8
}

func localSecurityFlagCases() []securityFlagCase {
	return []securityFlagCase{
		{
			name:             "garbled",
			flags:            FlagGarbled,
			wantErr:          ErrEncrypted,
			wantKind:         UnsupportedModeKindEncrypted,
			wantSecurityMode: UnsupportedSecurityModeNone,
			wantVersion:      EncryptOld,
		},
		{
			name:             "old_secured",
			flags:            FlagOldSecured,
			wantErr:          ErrSecurityMode,
			wantKind:         UnsupportedModeKindSecured,
			wantSecurityMode: UnsupportedSecurityModeSignature,
		},
		{
			name:             "secured",
			flags:            FlagSecured,
			wantErr:          ErrSecurityMode,
			wantKind:         UnsupportedModeKindSecured,
			wantSecurityMode: UnsupportedSecurityModeEnvelope,
		},
		{
			name:             "secured_precedes_old_secured",
			flags:            FlagSecured | FlagOldSecured,
			wantErr:          ErrSecurityMode,
			wantKind:         UnsupportedModeKindSecured,
			wantSecurityMode: UnsupportedSecurityModeEnvelope,
		},
		{
			name:             "garbled_and_secured",
			flags:            FlagGarbled | FlagSecured | FlagOldSecured,
			wantErr:          ErrEncrypted,
			wantKind:         UnsupportedModeKindEncrypted,
			wantSecurityMode: UnsupportedSecurityModeNone,
			wantVersion:      EncryptOld,
		},
	}
}

func mainSecurityFlagCases() []securityFlagCase {
	return []securityFlagCase{
		{
			name:             "garbled",
			flags:            FlagGarbled,
			wantErr:          ErrEncrypted,
			wantKind:         UnsupportedModeKindEncrypted,
			wantSecurityMode: UnsupportedSecurityModeNone,
			wantVersion:      EncryptOld,
		},
		{
			name:             "signature",
			flags:            FlagOldSecured,
			wantErr:          ErrSecurityMode,
			wantKind:         UnsupportedModeKindSecured,
			wantSecurityMode: UnsupportedSecurityModeSignature,
		},
		{
			name:             "envelope",
			flags:            FlagSecured,
			wantErr:          ErrSecurityMode,
			wantKind:         UnsupportedModeKindSecured,
			wantSecurityMode: UnsupportedSecurityModeEnvelope,
		},
		{
			name:             "envelope_precedes_signature_and_protection",
			flags:            FlagSecured | FlagOldSecured | FlagProtection,
			wantErr:          ErrSecurityMode,
			wantKind:         UnsupportedModeKindSecured,
			wantSecurityMode: UnsupportedSecurityModeEnvelope,
		},
		{
			name:             "signature_precedes_protection",
			flags:            FlagOldSecured | FlagProtection,
			wantErr:          ErrSecurityMode,
			wantKind:         UnsupportedModeKindSecured,
			wantSecurityMode: UnsupportedSecurityModeSignature,
		},
		{
			name:             "protection",
			flags:            FlagProtection,
			wantErr:          ErrSecurityMode,
			wantKind:         UnsupportedModeKindSecured,
			wantSecurityMode: UnsupportedSecurityModeProtection,
		},
		{
			name:             "garbled_and_protection",
			flags:            FlagGarbled | FlagProtection,
			wantErr:          ErrEncrypted,
			wantKind:         UnsupportedModeKindEncrypted,
			wantSecurityMode: UnsupportedSecurityModeNone,
			wantVersion:      EncryptOld,
		},
	}
}

func assertUnsupportedModeError(t *testing.T, err error, wantKind UnsupportedModeKind, wantSecurityMode UnsupportedSecurityMode, wantFlags, wantVersion uint8) {
	t.Helper()

	var modeErr *UnsupportedModeError
	if !errors.As(err, &modeErr) {
		t.Fatalf("error %v does not include *UnsupportedModeError", err)
	}
	if got := modeErr.Kind; got != wantKind {
		t.Fatalf("UnsupportedModeError.Kind = %v, want %v", got, wantKind)
	}
	if got := modeErr.SecurityMode; got != wantSecurityMode {
		t.Fatalf("UnsupportedModeError.SecurityMode = %v, want %v", got, wantSecurityMode)
	}
	if got := modeErr.Flags; got != wantFlags {
		t.Fatalf("UnsupportedModeError.Flags = 0x%02x, want 0x%02x", got, wantFlags)
	}
	if got := modeErr.EncryptionVersion; got != wantVersion {
		t.Fatalf("UnsupportedModeError.EncryptionVersion = %d, want %d", got, wantVersion)
	}
	wantTyped := &UnsupportedModeError{
		Kind:              wantKind,
		SecurityMode:      wantSecurityMode,
		Flags:             wantFlags,
		EncryptionVersion: wantVersion,
	}
	if !errors.Is(err, wantTyped) {
		t.Fatalf("errors.Is(%v, %v) = false, want true", err, wantTyped)
	}
	legacyTyped := &UnsupportedModeError{
		Kind:              wantKind,
		Flags:             wantFlags,
		EncryptionVersion: wantVersion,
	}
	if !errors.Is(err, legacyTyped) {
		t.Fatalf("errors.Is(%v, %v) = false, want true", err, legacyTyped)
	}
}

func assertNoUnsupportedModeError(t *testing.T, err error) {
	t.Helper()

	var modeErr *UnsupportedModeError
	if errors.As(err, &modeErr) {
		t.Fatalf("error unexpectedly included UnsupportedModeError: %+v", modeErr)
	}
}
