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

func TestEncryptedEntryOpenWithReaderPassword(t *testing.T) {
	const (
		payload  = "encrypted-std-payload"
		password = "swordfish"
		modifier = uint8(0x5a)
	)

	cases := []struct {
		name    string
		version uint8
	}{
		{name: "old", version: EncryptOld},
		{name: "std", version: EncryptStd},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			archive := writeSingleFileArchive(t, &FileHeader{
				Name:   "enc.bin",
				Method: Store,
			}, payload)
			archive = garbleFirstEntry(t, archive, password, modifier, tc.version)

			r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
			if err != nil {
				t.Fatalf("NewReader: %v", err)
			}
			r.SetPassword(password)

			rc, err := r.File[0].Open()
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			got, err := io.ReadAll(rc)
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			if err := rc.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if got, want := string(got), payload; got != want {
				t.Fatalf("payload = %q, want %q", got, want)
			}
		})
	}
}

func TestEncryptedEntryOpenWithPerFilePasswordOverride(t *testing.T) {
	const (
		payload  = "encrypted-override-payload"
		password = "swordfish"
		modifier = uint8(0x44)
	)

	archive := writeSingleFileArchive(t, &FileHeader{
		Name:   "override.bin",
		Method: Store,
	}, payload)
	archive = garbleFirstEntry(t, archive, password, modifier, EncryptStd)

	r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	r.SetPassword("wrong")

	rc, err := r.File[0].OpenWithPassword(password)
	if err != nil {
		t.Fatalf("OpenWithPassword: %v", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got, want := string(got), payload; got != want {
		t.Fatalf("payload = %q, want %q", got, want)
	}
}

func TestEncryptedEntryMissingPassword(t *testing.T) {
	archive := writeSingleFileArchive(t, &FileHeader{
		Name:   "missing.bin",
		Method: Store,
	}, "missing-password")
	archive = garbleFirstEntry(t, archive, "secret", 0x11, EncryptStd)

	r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, err = r.File[0].Open()
	if !errors.Is(err, ErrPasswordRequired) {
		t.Fatalf("Open error = %v, want %v", err, ErrPasswordRequired)
	}
	var modeErr *UnsupportedModeError
	if errors.As(err, &modeErr) {
		t.Fatalf("Open error unexpectedly included UnsupportedModeError: %+v", modeErr)
	}
}

func TestEncryptedEntryWrongPassword(t *testing.T) {
	archive := writeSingleFileArchive(t, &FileHeader{
		Name:   "wrong.bin",
		Method: Store,
	}, "wrong-password")
	archive = garbleFirstEntry(t, archive, "secret", 0x11, EncryptStd)

	r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	r.SetPassword("bad")

	rc, err := r.File[0].Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_, err = io.ReadAll(rc)
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("ReadAll error = %v, want %v", err, ErrChecksum)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestEncryptedEntryOpenMethods0To4WithReaderPassword(t *testing.T) {
	const password = "method-password"

	variants := []struct {
		name    string
		version uint8
	}{
		{name: "old", version: EncryptOld},
		{name: "std", version: EncryptStd},
	}
	methods := []uint16{Store, Method1, Method2, Method3, Method4}

	for _, method := range methods {
		method := method
		for _, variant := range variants {
			variant := variant
			t.Run(fmt.Sprintf("method-%d/%s", method, variant.name), func(t *testing.T) {
				payload := encryptedMethodPayload(method)
				archive := writeSingleFileArchive(t, &FileHeader{
					Name:   "method.bin",
					Method: method,
				}, string(payload))
				archive = garbleFirstEntry(t, archive, password, uint8(0x30+method), variant.version)

				r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
				if err != nil {
					t.Fatalf("NewReader: %v", err)
				}
				r.SetPassword(password)

				rc, err := r.File[0].Open()
				if err != nil {
					t.Fatalf("Open: %v", err)
				}
				got, readErr := io.ReadAll(rc)
				closeErr := rc.Close()
				if readErr != nil {
					t.Fatalf("ReadAll: %v", readErr)
				}
				if closeErr != nil {
					t.Fatalf("Close: %v", closeErr)
				}
				if !bytes.Equal(got, payload) {
					t.Fatalf("payload mismatch for method %d variant %s", method, variant.name)
				}
			})
		}
	}
}

func TestEncryptedEntryWrongPasswordMethods0To4(t *testing.T) {
	const (
		password = "correct-password"
		wrong    = "incorrect-password"
	)

	variants := []struct {
		name    string
		version uint8
	}{
		{name: "old", version: EncryptOld},
		{name: "std", version: EncryptStd},
	}
	methods := []uint16{Store, Method1, Method2, Method3, Method4}

	for _, method := range methods {
		method := method
		for _, variant := range variants {
			variant := variant
			t.Run(fmt.Sprintf("method-%d/%s", method, variant.name), func(t *testing.T) {
				payload := encryptedMethodPayload(method)
				archive := writeSingleFileArchive(t, &FileHeader{
					Name:   "wrong-method.bin",
					Method: method,
				}, string(payload))
				archive = garbleFirstEntry(t, archive, password, uint8(0x40+method), variant.version)

				r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
				if err != nil {
					t.Fatalf("NewReader: %v", err)
				}
				r.SetPassword(wrong)

				rc, err := r.File[0].Open()
				if err != nil {
					t.Fatalf("Open: %v", err)
				}
				_, readErr := io.ReadAll(rc)
				closeErr := rc.Close()
				if readErr == nil {
					t.Fatalf("ReadAll with wrong password succeeded for method %d variant %s", method, variant.name)
				}
				if !errors.Is(readErr, ErrChecksum) && !errors.Is(readErr, ErrFormat) {
					t.Fatalf("ReadAll error = %v, want %v or %v", readErr, ErrChecksum, ErrFormat)
				}
				if closeErr != nil {
					t.Fatalf("Close: %v", closeErr)
				}
			})
		}
	}
}

func TestEncryptedEntryUnsupportedVersionsMethods0To4(t *testing.T) {
	const password = "unsupported-password"

	methods := []uint16{Store, Method1, Method2, Method3, Method4}
	variants := []uint8{EncryptGOST256, EncryptGOST256L, EncryptGOST40}

	for _, method := range methods {
		method := method
		for _, version := range variants {
			version := version
			t.Run(fmt.Sprintf("method-%d/version-%d", method, version), func(t *testing.T) {
				payload := encryptedMethodPayload(method)
				archive := writeSingleFileArchive(t, &FileHeader{
					Name:   "unsupported-method.bin",
					Method: method,
				}, string(payload))
				archive = garbleFirstEntry(t, archive, password, uint8(0x50+method), version)

				r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
				if err != nil {
					t.Fatalf("NewReader: %v", err)
				}
				r.SetPassword(password)

				_, err = r.File[0].Open()
				if !errors.Is(err, ErrUnsupportedEncryption) {
					t.Fatalf("Open error = %v, want %v", err, ErrUnsupportedEncryption)
				}
				if !errors.Is(err, ErrEncrypted) {
					t.Fatalf("Open error = %v, want %v", err, ErrEncrypted)
				}

				var modeErr *UnsupportedModeError
				if !errors.As(err, &modeErr) {
					t.Fatalf("Open error = %v, want *UnsupportedModeError", err)
				}
				if got, want := modeErr.Kind, UnsupportedModeKindEncrypted; got != want {
					t.Fatalf("UnsupportedModeError.Kind = %v, want %v", got, want)
				}
				if got, want := modeErr.Flags, FlagGarbled; got != want {
					t.Fatalf("UnsupportedModeError.Flags = 0x%02x, want 0x%02x", got, want)
				}
				if got, want := modeErr.EncryptionVersion, version; got != want {
					t.Fatalf("UnsupportedModeError.EncryptionVersion = %d, want %d", got, want)
				}
			})
		}
	}
}

func TestEncryptedMethod14DecodeLimitsEnforced(t *testing.T) {
	const (
		password = "decode-limits"
		version  = EncryptStd
	)

	for _, method := range []uint16{Method1, Method2, Method3, Method4} {
		method := method
		payload := encryptedMethodPayload(method)
		archive := writeSingleFileArchive(t, &FileHeader{
			Name:   "limits.bin",
			Method: method,
		}, string(payload))
		archive = garbleFirstEntry(t, archive, password, uint8(0x60+method), version)

		t.Run(fmt.Sprintf("method-%d/compressed", method), func(t *testing.T) {
			r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
			if err != nil {
				t.Fatalf("NewReader: %v", err)
			}
			f := r.File[0]
			if f.CompressedSize64 <= 1 {
				t.Fatalf("compressed size too small for limit test: %d", f.CompressedSize64)
			}
			r.SetPassword(password)
			r.SetMethod14DecodeLimits(Method14DecodeLimits{
				MaxCompressedSize:   f.CompressedSize64 - 1,
				MaxUncompressedSize: f.UncompressedSize64,
			})

			_, err = f.Open()
			if !errors.Is(err, ErrFormat) {
				t.Fatalf("Open error = %v, want %v", err, ErrFormat)
			}
		})

		t.Run(fmt.Sprintf("method-%d/uncompressed", method), func(t *testing.T) {
			r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
			if err != nil {
				t.Fatalf("NewReader: %v", err)
			}
			f := r.File[0]
			if f.UncompressedSize64 <= 1 {
				t.Fatalf("uncompressed size too small for limit test: %d", f.UncompressedSize64)
			}
			r.SetPassword(password)
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
}

func TestEncryptedEntryUnsupportedVersion(t *testing.T) {
	archive := writeSingleFileArchive(t, &FileHeader{
		Name:   "unsupported.bin",
		Method: Store,
	}, "unsupported-version")
	archive = garbleFirstEntry(t, archive, "secret", 0x66, EncryptGOST256)

	r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	r.SetPassword("secret")

	_, err = r.File[0].Open()
	if !errors.Is(err, ErrUnsupportedEncryption) {
		t.Fatalf("Open error = %v, want %v", err, ErrUnsupportedEncryption)
	}
	if !errors.Is(err, ErrEncrypted) {
		t.Fatalf("Open error = %v, want %v", err, ErrEncrypted)
	}

	var modeErr *UnsupportedModeError
	if !errors.As(err, &modeErr) {
		t.Fatalf("Open error = %v, want *UnsupportedModeError", err)
	}
	if got, want := modeErr.Kind, UnsupportedModeKindEncrypted; got != want {
		t.Fatalf("UnsupportedModeError.Kind = %v, want %v", got, want)
	}
	if got, want := modeErr.Flags, FlagGarbled; got != want {
		t.Fatalf("UnsupportedModeError.Flags = 0x%02x, want 0x%02x", got, want)
	}
	if got, want := modeErr.EncryptionVersion, EncryptGOST256; got != want {
		t.Fatalf("UnsupportedModeError.EncryptionVersion = %d, want %d", got, want)
	}
}

func TestLocalHeaderPasswordModifierRoundTrip(t *testing.T) {
	const modifier = uint8(0xa5)

	src := writeSingleFileArchive(t, &FileHeader{
		Name:             "modifier.bin",
		Method:           Store,
		PasswordModifier: modifier,
	}, "modifier-payload")

	localBasic := readFirstLocalBasicHeader(t, src)
	if got := localBasic[7]; got != modifier {
		t.Fatalf("source local byte 7 = 0x%02x, want 0x%02x", got, modifier)
	}

	r1, err := NewReader(bytes.NewReader(src), int64(len(src)))
	if err != nil {
		t.Fatalf("NewReader source: %v", err)
	}
	if got := r1.File[0].PasswordModifier; got != modifier {
		t.Fatalf("source PasswordModifier = 0x%02x, want 0x%02x", got, modifier)
	}

	dst := copySingleFileArchive(t, r1.File[0])
	localBasic2 := readFirstLocalBasicHeader(t, dst)
	if got := localBasic2[7]; got != modifier {
		t.Fatalf("destination local byte 7 = 0x%02x, want 0x%02x", got, modifier)
	}

	r2, err := NewReader(bytes.NewReader(dst), int64(len(dst)))
	if err != nil {
		t.Fatalf("NewReader destination: %v", err)
	}
	if got := r2.File[0].PasswordModifier; got != modifier {
		t.Fatalf("destination PasswordModifier = 0x%02x, want 0x%02x", got, modifier)
	}
}

func TestReaderRegisterDecompressorNilDisablesFallback(t *testing.T) {
	archive := writeSingleFileArchive(t, &FileHeader{
		Name:   "disable-dcomp.bin",
		Method: Store,
	}, "payload")

	r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	r.RegisterDecompressor(Store, nil)

	_, err = r.File[0].Open()
	if !errors.Is(err, ErrAlgorithm) {
		t.Fatalf("Open error = %v, want %v", err, ErrAlgorithm)
	}
}

func TestMultiSegmentOpenUsesLazySegmentOpen(t *testing.T) {
	archive := writeSingleFileArchive(t, &FileHeader{
		Name:   "lazy-segments.bin",
		Method: Store,
	}, "AB")

	r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if got, want := len(r.File), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}

	f := r.File[0]
	f.segments = []fileSegment{
		{
			dataOffset:       f.dataOffset,
			method:           Store,
			flags:            f.Flags,
			extFlags:         f.ExtFlags,
			passwordModifier: f.PasswordModifier,
			compressedSize:   1,
			uncompressedSize: 1,
			crc32:            crc32.ChecksumIEEE([]byte("A")),
		},
		{
			dataOffset:       f.dataOffset + 1,
			method:           0xCAFE,
			flags:            f.Flags,
			extFlags:         f.ExtFlags,
			passwordModifier: f.PasswordModifier,
			compressedSize:   1,
			uncompressedSize: 1,
			crc32:            crc32.ChecksumIEEE([]byte("B")),
		},
	}

	rc, err := f.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()

	buf := make([]byte, 1)
	n, err := rc.Read(buf)
	if n != 1 || err != nil {
		t.Fatalf("first read = (%d, %v), want (1, nil)", n, err)
	}
	if got, want := string(buf), "A"; got != want {
		t.Fatalf("first payload = %q, want %q", got, want)
	}

	n, err = rc.Read(buf)
	if n != 0 {
		t.Fatalf("second read n = %d, want 0", n)
	}
	if !errors.Is(err, ErrAlgorithm) {
		t.Fatalf("second read error = %v, want %v", err, ErrAlgorithm)
	}
}

func TestNewReaderPreservesNonFormatReadErrors(t *testing.T) {
	archive := writeSingleFileArchive(t, &FileHeader{
		Name:   "io-error.bin",
		Method: Store,
	}, "payload")

	wantErr := io.ErrClosedPipe
	_, err := NewReader(&failingReaderAt{
		data:   archive,
		failAt: 4,
		err:    wantErr,
	}, int64(len(archive)))
	if !errors.Is(err, wantErr) {
		t.Fatalf("NewReader error = %v, want %v", err, wantErr)
	}
	if errors.Is(err, ErrFormat) {
		t.Fatalf("NewReader error = %v unexpectedly matched %v", err, ErrFormat)
	}
}

func garbleFirstEntry(t *testing.T, archive []byte, password string, modifier, version uint8) []byte {
	t.Helper()

	out := append([]byte(nil), archive...)
	localOff := firstLocalHeaderOffset(t, out)
	i := int(localOff)
	if i < 0 || i+4 > len(out) {
		t.Fatalf("local header offset out of range: %d", i)
	}
	if out[i] != arjHeaderID1 || out[i+1] != arjHeaderID2 {
		t.Fatalf("invalid local header signature at offset %d", i)
	}

	basicSize := int(binary.LittleEndian.Uint16(out[i+2 : i+4]))
	basicStart := i + 4
	basicEnd := basicStart + basicSize
	if basicSize < arjMinFirstHeaderSize || basicEnd+4 > len(out) {
		t.Fatalf("invalid local header bounds")
	}

	out[basicStart+4] |= FlagGarbled
	out[basicStart+7] = modifier
	out[basicStart+28] = (out[basicStart+28] & 0xf0) | (version & 0x0f)
	binary.LittleEndian.PutUint32(out[basicEnd:basicEnd+4], crc32.ChecksumIEEE(out[basicStart:basicEnd]))

	_, _, dataOff, err := readHeaderBlock(bytes.NewReader(out), int64(len(out)), localOff)
	if err != nil {
		t.Fatalf("readHeaderBlock(local): %v", err)
	}
	compressedSize := int(binary.LittleEndian.Uint32(out[basicStart+12 : basicStart+16]))
	start := int(dataOff)
	end := start + compressedSize
	if start < 0 || end < start || end > len(out) {
		t.Fatalf("invalid local data bounds: start=%d end=%d len=%d", start, end, len(out))
	}
	applyGarbledXORInPlace(out[start:end], []byte(password), modifier, 0)

	return out
}

func encryptedMethodPayload(method uint16) []byte {
	header := []byte(fmt.Sprintf("encrypted-method-%d\n", method))
	body := bytes.Repeat([]byte{
		byte(method),
		0x00,
		0x1f,
		0x7f,
		0x80,
		0xff,
		'\n',
	}, 256)
	return append(header, body...)
}

type failingReaderAt struct {
	data   []byte
	failAt int64
	err    error
}

func (r *failingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= r.failAt {
		return 0, r.err
	}
	if off < 0 || off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}
