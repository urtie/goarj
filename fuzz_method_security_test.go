package arj

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"
)

const (
	fuzzMaxRoundTripPayload = 1 << 16
	fuzzMaxArchiveSize      = 1 << 20
	fuzzMaxMalformedComp    = 1 << 15
	fuzzMaxMalformedOutput  = 1 << 16
)

const (
	fuzzLocalSecurityFlagMask = FlagGarbled | FlagOldSecured | FlagSecured
	fuzzMainSecurityFlagMask  = FlagGarbled | FlagOldSecured | FlagSecured | FlagProtection
)

func FuzzMethod14RoundTrip(f *testing.F) {
	seeds := [][]byte{
		nil,
		[]byte("native method payload"),
		bytes.Repeat([]byte{0x00, 0xFF, 0x10, 0xEF}, 64),
		bytes.Repeat([]byte("method-14-roundtrip-"), 48),
		compatibilityFixturePayload(),
	}
	for _, n := range []int{1, 2, 3, 4, 7, 8, 15, 16, 31, 32, 63, 64, 255, 256, 257, 511, 512, 1023, 2048} {
		seeds = append(seeds, fuzzPatternBytes(n))
	}
	for _, method := range []uint8{uint8(Method1), uint8(Method2), uint8(Method3), uint8(Method4)} {
		for _, seed := range seeds {
			f.Add(method, append([]byte(nil), seed...))
		}
	}

	f.Fuzz(func(t *testing.T, methodByte uint8, payload []byte) {
		if len(payload) > fuzzMaxRoundTripPayload {
			return
		}
		method := [...]uint16{Method1, Method2, Method3, Method4}[int(methodByte)%4]

		compressed, err := compressMethod14Payload(method, payload)
		if err != nil {
			t.Fatalf("compress method %d: %v", method, err)
		}
		roundTrip, err := decompressMethod14Payload(method, compressed, uint64(len(payload)))
		if err != nil {
			t.Fatalf("decompress method %d: %v", method, err)
		}
		if !bytes.Equal(roundTrip, payload) {
			t.Fatalf("codec round-trip mismatch method %d", method)
		}

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
			t.Fatalf("Close writer method %d: %v", method, err)
		}

		r, err := NewReader(bytes.NewReader(archive.Bytes()), int64(archive.Len()))
		if err != nil {
			t.Fatalf("NewReader method %d: %v", method, err)
		}
		if got, want := len(r.File), 1; got != want {
			t.Fatalf("file count = %d, want %d", got, want)
		}
		rc, err := r.File[0].Open()
		if err != nil {
			t.Fatalf("Open method %d: %v", method, err)
		}
		got, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		if readErr != nil {
			t.Fatalf("ReadAll method %d: %v", method, readErr)
		}
		if closeErr != nil {
			t.Fatalf("Close method %d: %v", method, closeErr)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("archive round-trip mismatch method %d", method)
		}
	})
}

func FuzzMethod14MalformedBitstreams(f *testing.F) {
	seeds := []struct {
		method     uint8
		compressed []byte
		origSize   uint32
	}{
		{method: uint8(Method1), compressed: nil, origSize: 0},
		{method: uint8(Method2), compressed: []byte{0x00}, origSize: 1},
		{method: uint8(Method3), compressed: []byte{0xff, 0xff}, origSize: 32},
		{method: uint8(Method4), compressed: encodeMethod4LiteralOnly([]byte("seed")), origSize: 4},
	}
	seeds = append(seeds, fuzzMethod14MalformedCorpus()...)
	for _, seed := range seeds {
		f.Add(seed.method, append([]byte(nil), seed.compressed...), seed.origSize)
	}

	f.Fuzz(func(t *testing.T, methodByte uint8, compressed []byte, origSize uint32) {
		if len(compressed) > fuzzMaxMalformedComp {
			return
		}

		method := [...]uint16{Method1, Method2, Method3, Method4}[int(methodByte)%4]
		wantSize := uint64(origSize % fuzzMaxMalformedOutput)
		limits := Method14DecodeLimits{
			MaxCompressedSize:   fuzzMaxMalformedComp,
			MaxUncompressedSize: fuzzMaxMalformedOutput,
		}

		defer func() {
			if recovered := recover(); recovered != nil {
				t.Fatalf("panic during method %d decode: %v", method, recovered)
			}
		}()

		out, err := decompressMethod14PayloadWithLimits(method, compressed, wantSize, limits)
		if err != nil {
			if !errors.Is(err, ErrFormat) {
				t.Fatalf("decode error = %v, want %v-compatible error", err, ErrFormat)
			}
			return
		}
		if uint64(len(out)) != wantSize {
			t.Fatalf("decoded size = %d, want %d", len(out), wantSize)
		}
	})
}

func FuzzSecurityFlagMutations(f *testing.F) {
	seeds := fuzzSecuritySeedArchives()
	for i := range seeds {
		idx := uint8(i)
		f.Add(idx, uint8(0), uint8(FlagGarbled), uint8(0), uint8(0))
		f.Add(idx, uint8(1), uint8(FlagSecured), uint8(0), uint8(0))
		f.Add(idx, uint8(2), uint8(FlagGarbled|FlagOldSecured|FlagSecured), uint8(0), uint8(0))
		f.Add(idx, uint8(1), uint8(FlagProtection), uint8(0), uint8(0))
		f.Add(idx, uint8(2), uint8(FlagOldSecured|FlagProtection), uint8(0), uint8(1))
		f.Add(idx, uint8(0), uint8(FlagOldSecured), uint8(1), uint8(3))
		f.Add(idx, uint8(1), uint8(FlagSecured), uint8(2), uint8(2))
		f.Add(idx, uint8(0), uint8(FlagGarbled), uint8(0), uint8(0x84))
		f.Add(idx, uint8(1), uint8(FlagGarbled), uint8(0), uint8(0x88))
		f.Add(idx, uint8(2), uint8(FlagGarbled), uint8(0), uint8(0x8c))
	}

	f.Fuzz(func(t *testing.T, seedIdx, target, bits, malformed, tweak uint8) {
		if len(seeds) == 0 {
			t.Skip("no seed archives available")
		}
		base := seeds[int(seedIdx)%len(seeds)]
		if len(base.archive) == 0 || len(base.archive) > fuzzMaxArchiveSize {
			return
		}
		archive := append([]byte(nil), base.archive...)

		mainOff, err := findMainHeaderOffset(bytes.NewReader(archive), int64(len(archive)))
		if err != nil {
			return
		}
		localOff, ok := fuzzFirstLocalHeaderOffset(archive, mainOff)
		if !ok {
			return
		}

		localFlags := uint8(0)
		mainFlags := uint8(0)

		switch target % 3 {
		case 0:
			archive, localFlags, ok = fuzzMutateHeaderSecurityFlags(archive, localOff, bits, fuzzLocalSecurityFlagMask, malformed, tweak)
		case 1:
			archive, mainFlags, ok = fuzzMutateHeaderSecurityFlags(archive, mainOff, bits, fuzzMainSecurityFlagMask, malformed, tweak)
		default:
			archive, mainFlags, ok = fuzzMutateHeaderSecurityFlags(archive, mainOff, bits, fuzzMainSecurityFlagMask, 0, tweak)
			if !ok {
				return
			}
			archive, localFlags, ok = fuzzMutateHeaderSecurityFlags(archive, localOff, bits, fuzzLocalSecurityFlagMask, malformed, tweak)
		}
		if !ok {
			return
		}

		r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
		if malformed%3 != 0 {
			if !errors.Is(err, ErrFormat) {
				t.Fatalf("NewReader malformed error = %v, want %v", err, ErrFormat)
			}
			return
		}
		if err != nil {
			t.Fatalf("NewReader: %v", err)
		}
		if len(r.File) == 0 {
			t.Fatalf("expected at least one file")
		}

		want := fuzzExpectedSecurityOpenOutcome(
			localFlags,
			mainFlags,
			r.File[0].EncryptionVersion(),
			r.ArchiveHeader.EncryptionVersion(),
		)
		rc, err := r.File[0].Open()
		if want.err != nil {
			if !errors.Is(err, want.err) {
				t.Fatalf("Open error = %v, want %v", err, want.err)
			}
			if want.mode != nil {
				assertFuzzUnsupportedModeError(t, err, want.mode)
			} else {
				var modeErr *UnsupportedModeError
				if errors.As(err, &modeErr) {
					t.Fatalf("Open error unexpectedly included UnsupportedModeError: %+v", modeErr)
				}
			}
			return
		}
		if err != nil {
			t.Fatalf("Open unexpected error: %v", err)
		}
		gotPayload, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		if readErr != nil {
			t.Fatalf("ReadAll: %v", readErr)
		}
		if closeErr != nil {
			t.Fatalf("Close: %v", closeErr)
		}
		if !bytes.Equal(gotPayload, base.payload) {
			t.Fatalf("payload mismatch for successful open")
		}
	})
}

type fuzzExpectedOpenOutcome struct {
	err  error
	mode *UnsupportedModeError
}

func fuzzExpectedSecurityOpenOutcome(localFlags, mainFlags, localVersion, mainVersion uint8) fuzzExpectedOpenOutcome {
	if localFlags&FlagGarbled != 0 {
		switch localVersion {
		case EncryptOld, EncryptStd:
			return fuzzExpectedOpenOutcome{err: ErrPasswordRequired}
		default:
			return fuzzExpectedOpenOutcome{
				err: ErrEncrypted,
				mode: &UnsupportedModeError{
					Kind:              UnsupportedModeKindEncrypted,
					Flags:             localFlags,
					EncryptionVersion: localVersion,
				},
			}
		}
	}
	if localFlags&FlagSecured != 0 {
		return fuzzExpectedOpenOutcome{
			err: ErrSecurityMode,
			mode: &UnsupportedModeError{
				Kind:         UnsupportedModeKindSecured,
				SecurityMode: UnsupportedSecurityModeEnvelope,
				Flags:        localFlags,
			},
		}
	}
	if localFlags&FlagOldSecured != 0 {
		return fuzzExpectedOpenOutcome{
			err: ErrSecurityMode,
			mode: &UnsupportedModeError{
				Kind:         UnsupportedModeKindSecured,
				SecurityMode: UnsupportedSecurityModeSignature,
				Flags:        localFlags,
			},
		}
	}
	if mainFlags&FlagGarbled != 0 {
		return fuzzExpectedOpenOutcome{
			err: ErrEncrypted,
			mode: &UnsupportedModeError{
				Kind:              UnsupportedModeKindEncrypted,
				Flags:             mainFlags,
				EncryptionVersion: mainVersion,
			},
		}
	}
	if mainFlags&FlagSecured != 0 {
		return fuzzExpectedOpenOutcome{
			err: ErrSecurityMode,
			mode: &UnsupportedModeError{
				Kind:         UnsupportedModeKindSecured,
				SecurityMode: UnsupportedSecurityModeEnvelope,
				Flags:        mainFlags,
			},
		}
	}
	if mainFlags&FlagOldSecured != 0 {
		return fuzzExpectedOpenOutcome{
			err: ErrSecurityMode,
			mode: &UnsupportedModeError{
				Kind:         UnsupportedModeKindSecured,
				SecurityMode: UnsupportedSecurityModeSignature,
				Flags:        mainFlags,
			},
		}
	}
	if mainFlags&FlagProtection != 0 {
		return fuzzExpectedOpenOutcome{
			err: ErrSecurityMode,
			mode: &UnsupportedModeError{
				Kind:         UnsupportedModeKindSecured,
				SecurityMode: UnsupportedSecurityModeProtection,
				Flags:        mainFlags,
			},
		}
	}
	return fuzzExpectedOpenOutcome{}
}

func assertFuzzUnsupportedModeError(t *testing.T, err error, want *UnsupportedModeError) {
	t.Helper()

	var got *UnsupportedModeError
	if !errors.As(err, &got) {
		t.Fatalf("error %v does not include *UnsupportedModeError", err)
	}
	if got.Kind != want.Kind {
		t.Fatalf("UnsupportedModeError.Kind = %v, want %v", got.Kind, want.Kind)
	}
	if got.SecurityMode != want.SecurityMode {
		t.Fatalf("UnsupportedModeError.SecurityMode = %v, want %v", got.SecurityMode, want.SecurityMode)
	}
	if got.Flags != want.Flags {
		t.Fatalf("UnsupportedModeError.Flags = 0x%02x, want 0x%02x", got.Flags, want.Flags)
	}
	if got.EncryptionVersion != want.EncryptionVersion {
		t.Fatalf("UnsupportedModeError.EncryptionVersion = %d, want %d", got.EncryptionVersion, want.EncryptionVersion)
	}
	if !errors.Is(err, want) {
		t.Fatalf("errors.Is(%v, %v) = false, want true", err, want)
	}
}

type fuzzSecuritySeed struct {
	archive []byte
	payload []byte
}

func fuzzSecuritySeedArchives() []fuzzSecuritySeed {
	var out []fuzzSecuritySeed
	appendSeed := func(archive []byte) {
		if len(archive) == 0 || len(archive) > fuzzMaxArchiveSize {
			return
		}
		payload, ok := fuzzExtractFirstPayload(archive)
		if !ok {
			return
		}
		out = append(out, fuzzSecuritySeed{
			archive: append([]byte(nil), archive...),
			payload: payload,
		})
	}

	if generated := fuzzGeneratedSecurityArchive(); len(generated) > 0 {
		appendSeed(generated)
	}
	for _, name := range []string{
		"compat_method1.arj",
		"compat_method2.arj",
		"compat_method3.arj",
		"compat_method4.arj",
	} {
		b, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			continue
		}
		appendSeed(b)
	}
	return out
}

func fuzzExtractFirstPayload(archive []byte) ([]byte, bool) {
	r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil || len(r.File) == 0 {
		return nil, false
	}
	rc, err := r.File[0].Open()
	if err != nil {
		return nil, false
	}
	payload, readErr := io.ReadAll(rc)
	closeErr := rc.Close()
	if readErr != nil || closeErr != nil {
		return nil, false
	}
	return append([]byte(nil), payload...), true
}

func fuzzGeneratedSecurityArchive() []byte {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	fw, err := w.Create("secure.bin")
	if err != nil {
		return nil
	}
	if _, err := io.WriteString(fw, "payload"); err != nil {
		return nil
	}
	if err := w.Close(); err != nil {
		return nil
	}
	return append([]byte(nil), buf.Bytes()...)
}

func fuzzFirstLocalHeaderOffset(archive []byte, mainOff int64) (int64, bool) {
	_, _, next, err := readHeaderBlock(bytes.NewReader(archive), int64(len(archive)), mainOff)
	if err != nil {
		return 0, false
	}
	return next, true
}

func fuzzMutateHeaderSecurityFlags(archive []byte, off int64, bits, mask, malformed, tweak uint8) ([]byte, uint8, bool) {
	out := append([]byte(nil), archive...)
	i := int(off)
	if i < 0 || i+4 > len(out) {
		return nil, 0, false
	}
	if out[i] != arjHeaderID1 || out[i+1] != arjHeaderID2 {
		return nil, 0, false
	}
	basicSize := int(binary.LittleEndian.Uint16(out[i+2 : i+4]))
	basicStart := i + 4
	basicEnd := basicStart + basicSize
	if basicSize < arjMinFirstHeaderSize || basicEnd+4 > len(out) {
		return nil, 0, false
	}

	flagIndex := basicStart + 4
	mutatedFlags := (out[flagIndex] &^ mask) | (bits & mask)
	out[flagIndex] = mutatedFlags

	if tweak&0x80 != 0 {
		hostData := binary.LittleEndian.Uint16(out[basicStart+28 : basicStart+30])
		version := (tweak >> 1) & 0x0f
		hostData = (hostData & 0xfff0) | uint16(version)
		binary.LittleEndian.PutUint16(out[basicStart+28:basicStart+30], hostData)
	}

	crcIndex := basicEnd
	binary.LittleEndian.PutUint32(out[crcIndex:crcIndex+4], crc32.ChecksumIEEE(out[basicStart:basicEnd]))

	switch malformed % 3 {
	case 1:
		out[crcIndex+int(tweak)%4] ^= 1 << (tweak % 8)
	case 2:
		cut := int(tweak%8) + 1
		if len(out) <= cut {
			return nil, 0, false
		}
		out = out[:len(out)-cut]
	}

	return out, mutatedFlags, true
}

func fuzzPatternBytes(n int) []byte {
	if n <= 0 {
		return nil
	}
	out := make([]byte, n)
	for i := range out {
		out[i] = byte((i * 31) ^ (i >> 3) ^ 0xA5)
	}
	return out
}

func fuzzMethod14MalformedCorpus() []struct {
	method     uint8
	compressed []byte
	origSize   uint32
} {
	var out []struct {
		method     uint8
		compressed []byte
		origSize   uint32
	}

	methods := []uint16{Method1, Method2, Method3, Method4}
	for _, method := range methods {
		payload := fuzzPatternBytes(256 + int(method))
		compressed, err := compressMethod14Payload(method, payload)
		if err != nil || len(compressed) == 0 {
			continue
		}

		methodByte := uint8(method)
		out = append(out,
			struct {
				method     uint8
				compressed []byte
				origSize   uint32
			}{
				method:     methodByte,
				compressed: append([]byte(nil), compressed...),
				origSize:   uint32(len(payload)),
			},
			struct {
				method     uint8
				compressed []byte
				origSize   uint32
			}{
				method:     methodByte,
				compressed: append([]byte(nil), compressed[:len(compressed)-1]...),
				origSize:   uint32(len(payload)),
			},
			struct {
				method     uint8
				compressed []byte
				origSize   uint32
			}{
				method:     methodByte,
				compressed: append(append([]byte(nil), compressed...), 0x00, 0xff, 0x7f),
				origSize:   uint32(len(payload)),
			},
			struct {
				method     uint8
				compressed []byte
				origSize   uint32
			}{
				method:     methodByte,
				compressed: append([]byte(nil), compressed...),
				origSize:   fuzzMaxMalformedOutput - 1,
			},
		)

		flip := append([]byte(nil), compressed...)
		flip[0] ^= 0x80
		out = append(out, struct {
			method     uint8
			compressed []byte
			origSize   uint32
		}{
			method:     methodByte,
			compressed: flip,
			origSize:   uint32(len(payload)),
		})
	}
	return out
}
