package arj

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestVolumePaths(t *testing.T) {
	tmp := t.TempDir()
	base := filepath.Join(tmp, "set")
	mustWriteFile(t, base+".arj", []byte("x"))
	mustWriteFile(t, base+".a01", []byte("x"))
	mustWriteFile(t, base+".a02", []byte("x"))

	t.Run("from arj", func(t *testing.T) {
		got, err := VolumePaths(base + ".arj")
		if err != nil {
			t.Fatalf("VolumePaths: %v", err)
		}
		want := []string{base + ".arj", base + ".a01", base + ".a02"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("VolumePaths = %v, want %v", got, want)
		}
	})

	t.Run("from a01", func(t *testing.T) {
		got, err := VolumePaths(base + ".a01")
		if err != nil {
			t.Fatalf("VolumePaths: %v", err)
		}
		want := []string{base + ".arj", base + ".a01", base + ".a02"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("VolumePaths = %v, want %v", got, want)
		}
	})

	t.Run("non volume path", func(t *testing.T) {
		path := filepath.Join(tmp, "plain.bin")
		got, err := VolumePaths(path)
		if err != nil {
			t.Fatalf("VolumePaths: %v", err)
		}
		if want := []string{path}; !reflect.DeepEqual(got, want) {
			t.Fatalf("VolumePaths = %v, want %v", got, want)
		}
	})

	t.Run("missing first volume", func(t *testing.T) {
		_, err := VolumePaths(filepath.Join(tmp, "missing.a01"))
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("VolumePaths error = %v, want %v", err, os.ErrNotExist)
		}
	})
}

func TestVolumePathsContinuationTripleDigitAndMixedCase(t *testing.T) {
	tmp := t.TempDir()
	base := filepath.Join(tmp, "set")

	mustWriteFile(t, base+".ARJ", []byte("x"))
	for i := 1; i <= 100; i++ {
		partPath := fmt.Sprintf("%s.a%02d", base, i)
		if i == 100 {
			partPath = base + ".A100"
		}
		mustWriteFile(t, partPath, []byte("x"))
	}

	for _, openPath := range []string{base + ".a99", base + ".a100", base + ".A100"} {
		got, err := VolumePaths(openPath)
		if err != nil {
			t.Fatalf("VolumePaths(%s): %v", openPath, err)
		}
		if gotLen, want := len(got), 101; gotLen != want {
			t.Fatalf("VolumePaths(%s) len = %d, want %d", openPath, gotLen, want)
		}
		if gotFirst, wantFirst := got[0], base+".ARJ"; gotFirst != wantFirst {
			t.Fatalf("VolumePaths(%s) first = %q, want %q", openPath, gotFirst, wantFirst)
		}
		if gotPart99, wantPart99 := got[99], base+".a99"; gotPart99 != wantPart99 {
			t.Fatalf("VolumePaths(%s) .a99 = %q, want %q", openPath, gotPart99, wantPart99)
		}
		if gotLast, wantLast := got[100], base+".A100"; gotLast != wantLast {
			t.Fatalf("VolumePaths(%s) .a100 = %q, want %q", openPath, gotLast, wantLast)
		}
	}
}

func TestVolumePathsContinuationThreeDigitWidth(t *testing.T) {
	tmp := t.TempDir()
	base := filepath.Join(tmp, "wide")

	mustWriteFile(t, base+".arj", []byte("x"))
	mustWriteFile(t, base+".a001", []byte("x"))
	mustWriteFile(t, base+".A002", []byte("x"))

	for _, openPath := range []string{base + ".arj", base + ".a001", base + ".A002"} {
		got, err := VolumePaths(openPath)
		if err != nil {
			t.Fatalf("VolumePaths(%s): %v", openPath, err)
		}
		want := []string{base + ".arj", base + ".a001", base + ".A002"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("VolumePaths(%s) = %v, want %v", openPath, got, want)
		}
	}
}

func TestIsVolumePartExt(t *testing.T) {
	tests := []struct {
		ext  string
		want bool
	}{
		{ext: ".a01", want: true},
		{ext: ".A01", want: true},
		{ext: ".a99", want: true},
		{ext: ".a100", want: true},
		{ext: ".a1000", want: true},
		{ext: ".A100", want: true},
		{ext: ".a001", want: true},
		{ext: ".a0001", want: false},
		{ext: ".a1", want: false},
		{ext: ".a00", want: false},
		{ext: ".b01", want: false},
		{ext: ".arj", want: false},
	}

	for _, tc := range tests {
		if got := isVolumePartExt(tc.ext); got != tc.want {
			t.Fatalf("isVolumePartExt(%q) = %t, want %t", tc.ext, got, tc.want)
		}
	}
}

func TestVolumePathsWithOptionsMaxVolumes(t *testing.T) {
	tmp := t.TempDir()
	base := filepath.Join(tmp, "set")
	mustWriteFile(t, base+".arj", []byte("x"))
	mustWriteFile(t, base+".a01", []byte("x"))
	mustWriteFile(t, base+".a02", []byte("x"))

	t.Run("limit exceeded", func(t *testing.T) {
		_, err := VolumePathsWithOptions(base+".arj", MultiVolumeOptions{MaxVolumes: 2})
		if !errors.Is(err, ErrTooManyVolumes) {
			t.Fatalf("VolumePathsWithOptions error = %v, want %v", err, ErrTooManyVolumes)
		}
	})

	t.Run("limit accepted", func(t *testing.T) {
		got, err := VolumePathsWithOptions(base+".arj", MultiVolumeOptions{MaxVolumes: 3})
		if err != nil {
			t.Fatalf("VolumePathsWithOptions: %v", err)
		}
		want := []string{base + ".arj", base + ".a01", base + ".a02"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("VolumePathsWithOptions = %v, want %v", got, want)
		}
	})

	t.Run("invalid limit", func(t *testing.T) {
		_, err := VolumePathsWithOptions(base+".arj", MultiVolumeOptions{MaxVolumes: -1})
		if !errors.Is(err, ErrInvalidMaxVolumeCount) {
			t.Fatalf("VolumePathsWithOptions error = %v, want %v", err, ErrInvalidMaxVolumeCount)
		}
	})
}

func TestVolumePathsDefaultMaxVolumeCountGuard(t *testing.T) {
	tmp := t.TempDir()
	base := filepath.Join(tmp, "dense")
	mustWriteFile(t, base+".arj", []byte("x"))

	for i := 1; i <= DefaultMaxVolumeCount; i++ {
		mustWriteFile(t, fmt.Sprintf("%s.a%02d", base, i), []byte("x"))
	}

	_, err := VolumePaths(base + ".arj")
	if !errors.Is(err, ErrTooManyVolumes) {
		t.Fatalf("VolumePaths error = %v, want %v", err, ErrTooManyVolumes)
	}
}

func TestOpenMultiReaderSingleVolumeCompatibility(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "single.arj")
	wantPayload := []byte("single-volume-payload")
	writeVolumeArchive(t, archivePath, []volumeEntry{
		{name: "single.txt", payload: wantPayload},
	})

	single, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer single.Close()

	multi, err := OpenMultiReader(archivePath)
	if err != nil {
		t.Fatalf("OpenMultiReader: %v", err)
	}
	defer multi.Close()

	if got, want := len(single.File), 1; got != want {
		t.Fatalf("OpenReader file count = %d, want %d", got, want)
	}
	if got, want := len(multi.File), 1; got != want {
		t.Fatalf("OpenMultiReader file count = %d, want %d", got, want)
	}

	gotPayload := mustReadFileEntry(t, multi.File[0])
	if !bytes.Equal(gotPayload, wantPayload) {
		t.Fatalf("payload = %q, want %q", gotPayload, wantPayload)
	}
}

func TestOpenMultiReaderSettersShareReaderStateWithFiles(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "state.arj")
	payload := []byte("stateful-payload")
	writeVolumeArchive(t, archivePath, []volumeEntry{
		{name: "state.txt", payload: payload},
	})

	multi, err := OpenMultiReader(archivePath)
	if err != nil {
		t.Fatalf("OpenMultiReader: %v", err)
	}
	defer multi.Close()

	if got, want := len(multi.File), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	f := multi.File[0]
	if f.arj != &multi.Reader {
		t.Fatalf("file reader pointer mismatch: got %p want %p", f.arj, &multi.Reader)
	}

	multi.SetPassword("top-secret")
	password := f.arj.passwordBytes()
	if got, want := string(password), "top-secret"; got != want {
		t.Fatalf("file reader password = %q, want %q", got, want)
	}
	clearBytes(password)

	decompressorCalled := false
	multi.RegisterDecompressor(Store, func(in io.Reader) io.ReadCloser {
		decompressorCalled = true
		return io.NopCloser(in)
	})

	gotPayload := mustReadFileEntry(t, f)
	if !bytes.Equal(gotPayload, payload) {
		t.Fatalf("payload = %q, want %q", gotPayload, payload)
	}
	if !decompressorCalled {
		t.Fatal("registered decompressor was not used")
	}
}

func TestMultiReadCloserCloseScrubsReaderPassword(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "scrub.arj")
	writeVolumeArchive(t, archivePath, []volumeEntry{
		{name: "scrub.txt", payload: []byte("payload")},
	})

	multi, err := OpenMultiReader(archivePath)
	if err != nil {
		t.Fatalf("OpenMultiReader: %v", err)
	}
	multi.SetPassword("top-secret")
	if len(multi.password) == 0 {
		t.Fatal("password unexpectedly empty after SetPassword")
	}
	sensitive := multi.password[:len(multi.password):len(multi.password)]

	if err := multi.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if multi.password != nil {
		t.Fatalf("password slice = %v, want nil", multi.password)
	}
	for i, b := range sensitive {
		if b != 0 {
			t.Fatalf("scrubbed byte %d = %d, want 0", i, b)
		}
	}
}

func TestOpenMultiReaderPropagatesBaseOffset(t *testing.T) {
	tmp := t.TempDir()
	base := filepath.Join(tmp, "prefixed")
	part1 := []byte("left-")
	part2 := []byte("right")

	writeVolumeArchive(t, base+".arj", []volumeEntry{
		{name: "joined.bin", flags: FlagVolume, payload: part1},
	})
	writeVolumeArchive(t, base+".a01", []volumeEntry{
		{name: "joined.bin", flags: FlagExtFile, payload: part2},
	})

	prefix := []byte("MZ-sfx-prefix")
	firstVolume, err := os.ReadFile(base + ".arj")
	if err != nil {
		t.Fatalf("ReadFile(first volume): %v", err)
	}
	if err := os.WriteFile(base+".arj", append(prefix, firstVolume...), 0o600); err != nil {
		t.Fatalf("WriteFile(prefixed first volume): %v", err)
	}

	single, err := OpenReader(base + ".arj")
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer single.Close()
	wantBaseOffset := int64(len(prefix))
	if got := single.BaseOffset(); got != wantBaseOffset {
		t.Fatalf("single BaseOffset = %d, want %d", got, wantBaseOffset)
	}

	multi, err := OpenMultiReader(base + ".arj")
	if err != nil {
		t.Fatalf("OpenMultiReader: %v", err)
	}
	defer multi.Close()
	if got := multi.BaseOffset(); got != wantBaseOffset {
		t.Fatalf("multi BaseOffset = %d, want %d", got, wantBaseOffset)
	}

	if got, want := len(multi.File), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	if got := mustReadFileEntry(t, multi.File[0]); string(got) != "left-right" {
		t.Fatalf("payload = %q, want %q", got, "left-right")
	}
}

func TestOpenMultiReaderReconstructsContinuedSegments(t *testing.T) {
	tmp := t.TempDir()
	base := filepath.Join(tmp, "split")
	part1 := bytes.Repeat([]byte("part-1-"), 200)
	part2 := bytes.Repeat([]byte("part-2-"), 150)

	writeVolumeArchive(t, base+".arj", []volumeEntry{
		{name: "full.txt", payload: []byte("full")},
		{name: "split.bin", flags: FlagVolume, payload: part1},
	})
	writeVolumeArchive(t, base+".a01", []volumeEntry{
		{name: "split.bin", flags: FlagExtFile, payload: part2},
		{name: "tail.txt", payload: []byte("tail")},
	})

	multi, err := OpenMultiReader(base + ".arj")
	if err != nil {
		t.Fatalf("OpenMultiReader: %v", err)
	}
	defer multi.Close()

	if got, want := len(multi.File), 3; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}

	var split *File
	for _, f := range multi.File {
		if f.Name == "split.bin" {
			split = f
			break
		}
	}
	if split == nil {
		t.Fatalf("split.bin not found")
	}

	if split.Flags&(FlagVolume|FlagExtFile) != 0 {
		t.Fatalf("split flags = 0x%02x, want volume/ext bits cleared", split.Flags)
	}

	joined := append(append([]byte(nil), part1...), part2...)
	gotPayload := mustReadFileEntry(t, split)
	if !bytes.Equal(gotPayload, joined) {
		t.Fatalf("split payload mismatch")
	}
	if got, want := split.UncompressedSize64, uint64(len(joined)); got != want {
		t.Fatalf("split size = %d, want %d", got, want)
	}

	raw, err := split.OpenRaw()
	if err != nil {
		t.Fatalf("OpenRaw: %v", err)
	}
	gotRaw, err := io.ReadAll(raw)
	if err != nil {
		t.Fatalf("ReadAll raw: %v", err)
	}
	if !bytes.Equal(gotRaw, joined) {
		t.Fatalf("split raw payload mismatch")
	}

	// Existing OpenReader behavior stays segment-scoped.
	single, err := OpenReader(base + ".arj")
	if err != nil {
		t.Fatalf("OpenReader first volume: %v", err)
	}
	defer single.Close()
	if got, want := len(single.File), 2; got != want {
		t.Fatalf("single-volume file count = %d, want %d", got, want)
	}
	gotSingle := mustReadFileEntry(t, single.File[1])
	if !bytes.Equal(gotSingle, part1) {
		t.Fatalf("single-volume segment payload mismatch")
	}
}

func TestOpenMultiReaderWithOptionsMaxVolumes(t *testing.T) {
	tmp := t.TempDir()
	base := filepath.Join(tmp, "limited")
	part1 := []byte("left-")
	part2 := []byte("right")

	writeVolumeArchive(t, base+".arj", []volumeEntry{
		{name: "x.bin", flags: FlagVolume, payload: part1},
	})
	writeVolumeArchive(t, base+".a01", []volumeEntry{
		{name: "x.bin", flags: FlagExtFile, payload: part2},
	})

	_, err := OpenMultiReaderWithOptions(base+".arj", MultiVolumeOptions{MaxVolumes: 1})
	if !errors.Is(err, ErrTooManyVolumes) {
		t.Fatalf("OpenMultiReaderWithOptions error = %v, want %v", err, ErrTooManyVolumes)
	}
}

func TestOpenMultiReaderWithOptionsPassesReaderParserLimits(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "limits.arj")
	writeVolumeArchive(t, archivePath, []volumeEntry{
		{name: "a.txt", payload: []byte("a")},
		{name: "b.txt", payload: []byte("b")},
	})

	_, err := OpenMultiReaderWithOptions(archivePath, MultiVolumeOptions{
		ReaderOptions: ReaderOptions{
			ParserLimits: ParserLimits{MaxEntries: 1},
		},
	})
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("OpenMultiReaderWithOptions error = %v, want %v", err, ErrFormat)
	}
}

func TestOpenMultiReaderWithOptionsEnforcesMergedLogicalEntryLimit(t *testing.T) {
	tmp := t.TempDir()
	base := filepath.Join(tmp, "merged-limit")

	writeVolumeArchive(t, base+".arj", []volumeEntry{
		{name: "split.bin", flags: FlagVolume, payload: []byte("left-")},
		{name: "a.txt", payload: []byte("a")},
	})
	writeVolumeArchive(t, base+".a01", []volumeEntry{
		{name: "split.bin", flags: FlagExtFile, payload: []byte("right")},
		{name: "b.txt", payload: []byte("b")},
	})

	_, err := OpenMultiReaderWithOptions(base+".arj", MultiVolumeOptions{
		ReaderOptions: ReaderOptions{
			ParserLimits: ParserLimits{MaxEntries: 2},
		},
	})
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("OpenMultiReaderWithOptions error = %v, want %v", err, ErrFormat)
	}
	if err == nil || !strings.Contains(err.Error(), "max entries exceeded") {
		t.Fatalf("OpenMultiReaderWithOptions error = %v, want max entries exceeded", err)
	}
}

func TestOpenMultiReaderWithOptionsRejectsInvalidReaderOptions(t *testing.T) {
	_, err := OpenMultiReaderWithOptions("unused.arj", MultiVolumeOptions{
		ReaderOptions: ReaderOptions{
			ParserLimits: ParserLimits{MaxEntries: -1},
		},
	})
	if !errors.Is(err, ErrInvalidParserMaxEntries) {
		t.Fatalf("OpenMultiReaderWithOptions error = %v, want %v", err, ErrInvalidParserMaxEntries)
	}
}

func TestOpenMultiReaderFromContinuationPath(t *testing.T) {
	tmp := t.TempDir()
	base := filepath.Join(tmp, "set")
	part1 := []byte("left-")
	part2 := []byte("right")

	writeVolumeArchive(t, base+".arj", []volumeEntry{
		{name: "x.bin", flags: FlagVolume, payload: part1},
	})
	writeVolumeArchive(t, base+".a01", []volumeEntry{
		{name: "x.bin", flags: FlagExtFile, payload: part2},
	})

	multi, err := OpenMultiReader(base + ".a01")
	if err != nil {
		t.Fatalf("OpenMultiReader(.a01): %v", err)
	}
	defer multi.Close()

	if got, want := len(multi.File), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	if got := mustReadFileEntry(t, multi.File[0]); string(got) != "left-right" {
		t.Fatalf("payload = %q, want %q", got, "left-right")
	}
}

func TestOpenMultiReaderFromContinuationPathMixedCaseFirstVolume(t *testing.T) {
	tmp := t.TempDir()
	base := filepath.Join(tmp, "set")
	part1 := []byte("left-")
	part2 := []byte("right")

	writeVolumeArchive(t, base+".Arj", []volumeEntry{
		{name: "x.bin", flags: FlagVolume, payload: part1},
	})
	writeVolumeArchive(t, base+".a01", []volumeEntry{
		{name: "x.bin", flags: FlagExtFile, payload: part2},
	})

	multi, err := OpenMultiReader(base + ".a01")
	if err != nil {
		t.Fatalf("OpenMultiReader(.a01): %v", err)
	}
	defer multi.Close()

	if got, want := len(multi.File), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	if got := mustReadFileEntry(t, multi.File[0]); string(got) != "left-right" {
		t.Fatalf("payload = %q, want %q", got, "left-right")
	}
}

func TestOpenMultiReaderRejectsUnrelatedBasenameVolumes(t *testing.T) {
	tmp := t.TempDir()
	base := filepath.Join(tmp, "set")

	writeVolumeArchive(t, base+".arj", []volumeEntry{
		{name: "left.txt", payload: []byte("left")},
	})
	writeVolumeArchive(t, base+".a01", []volumeEntry{
		{name: "right.txt", payload: []byte("right")},
	})

	_, err := OpenMultiReader(base + ".arj")
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("OpenMultiReader unrelated set error = %v, want %v", err, ErrFormat)
	}
}

func TestOpenMultiReaderMissingContinuation(t *testing.T) {
	tmp := t.TempDir()
	archivePath := filepath.Join(tmp, "broken.arj")
	writeVolumeArchive(t, archivePath, []volumeEntry{
		{name: "broken.bin", flags: FlagVolume, payload: []byte("part-1")},
	})

	_, err := OpenMultiReader(archivePath)
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("OpenMultiReader error = %v, want %v", err, ErrFormat)
	}
}

func TestOpenMultiReaderRejectsMixedMainHeaderVolumeFlagTransition(t *testing.T) {
	tmp := t.TempDir()
	base := filepath.Join(tmp, "mixed")

	writeVolumeArchive(t, base+".arj", []volumeEntry{
		{name: "joined.bin", flags: FlagVolume, payload: []byte("left-")},
	})
	writeVolumeArchive(t, base+".a01", []volumeEntry{
		{name: "joined.bin", flags: FlagExtFile, payload: []byte("right")},
	})

	f, err := os.OpenFile(base+".a01", os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile(%s): %v", base+".a01", err)
	}
	if err := patchMainVolumeFlag(f, true); err != nil {
		_ = f.Close()
		t.Fatalf("patchMainVolumeFlag(%s): %v", base+".a01", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close(%s): %v", base+".a01", err)
	}

	_, err = OpenMultiReader(base + ".arj")
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("OpenMultiReader mixed main flag error = %v, want %v", err, ErrFormat)
	}
}

func TestOpenMultiReaderRejectsMixedMainHeaderChapterNumber(t *testing.T) {
	tmp := t.TempDir()
	base := filepath.Join(tmp, "chapter-mixed")

	writeVolumeArchive(t, base+".arj", []volumeEntry{
		{name: "joined.bin", flags: FlagVolume, payload: []byte("left-")},
	})
	writeVolumeArchive(t, base+".a01", []volumeEntry{
		{name: "joined.bin", flags: FlagExtFile, payload: []byte("right")},
	})

	mutateMainHeaderHostData(t, base+".a01", 0x0100)

	_, err := OpenMultiReader(base + ".arj")
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("OpenMultiReader mixed chapter error = %v, want %v", err, ErrFormat)
	}
}

func TestOpenMultiReaderWithARJBinarySplitFixture(t *testing.T) {
	arjPath := requireInteropARJBinary(t)

	tmp := t.TempDir()
	payloadPath := filepath.Join(tmp, "payload.bin")
	archivePath := filepath.Join(tmp, "fixture.arj")

	rng := rand.New(rand.NewSource(42))
	payload := make([]byte, 96*1024)
	if _, err := rng.Read(payload); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	if err := os.WriteFile(payloadPath, payload, 0o600); err != nil {
		t.Fatalf("WriteFile(%s): %v", payloadPath, err)
	}

	runInteropARJCommand(t, arjPath, "a", "-y", "-i", "-m0", "-v10k", archivePath, payloadPath)

	if _, err := os.Stat(filepath.Join(tmp, "fixture.a01")); err != nil {
		t.Fatalf("expected continuation volume fixture.a01: %v", err)
	}

	multi, err := OpenMultiReader(archivePath)
	if err != nil {
		t.Fatalf("OpenMultiReader: %v", err)
	}
	defer multi.Close()

	if got, want := len(multi.File), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	gotPayload := mustReadFileEntry(t, multi.File[0])
	if !bytes.Equal(gotPayload, payload) {
		t.Fatalf("multi-volume payload mismatch")
	}

	// Single-volume API remains segment-scoped on the first archive file.
	single, err := OpenReader(archivePath)
	if err != nil {
		t.Fatalf("OpenReader first volume: %v", err)
	}
	defer single.Close()
	firstVolumePayload := mustReadFileEntry(t, single.File[0])
	if len(firstVolumePayload) >= len(payload) {
		t.Fatalf("first volume payload length = %d, want < %d", len(firstVolumePayload), len(payload))
	}
}

func TestOpenMultiReaderWithARJBinarySplitCompressedFixture(t *testing.T) {
	arjPath := requireInteropARJBinary(t)

	tmp := t.TempDir()
	payloadPath := filepath.Join(tmp, "payload.bin")
	archivePath := filepath.Join(tmp, "fixture-compressed.arj")

	rng := rand.New(rand.NewSource(4242))
	payload := make([]byte, 128*1024)
	if _, err := rng.Read(payload); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	if err := os.WriteFile(payloadPath, payload, 0o600); err != nil {
		t.Fatalf("WriteFile(%s): %v", payloadPath, err)
	}

	runInteropARJCommand(t, arjPath, "a", "-y", "-i", "-m1", "-v10k", archivePath, payloadPath)

	if _, err := os.Stat(filepath.Join(tmp, "fixture-compressed.a01")); err != nil {
		t.Fatalf("expected continuation volume fixture-compressed.a01: %v", err)
	}

	multi, err := OpenMultiReader(archivePath)
	if err != nil {
		t.Fatalf("OpenMultiReader: %v", err)
	}
	defer multi.Close()

	if got, want := len(multi.File), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	gotPayload := mustReadFileEntry(t, multi.File[0])
	if !bytes.Equal(gotPayload, payload) {
		t.Fatalf("compressed multi-volume payload mismatch")
	}
}

func TestConcatenatedReaderAtReadAtForwardProgressSearchCount(t *testing.T) {
	const volumeCount = 256

	readers := make([]io.ReaderAt, volumeCount)
	sizes := make([]int64, volumeCount)
	want := make([]byte, volumeCount)
	for i := 0; i < volumeCount; i++ {
		b := []byte{byte(i)}
		readers[i] = bytes.NewReader(b)
		sizes[i] = 1
		want[i] = b[0]
	}

	cr, err := newConcatenatedReaderAt(readers, sizes)
	if err != nil {
		t.Fatalf("newConcatenatedReaderAt: %v", err)
	}

	searchCalls := 0
	cr.volumeIndexHook = func() {
		searchCalls++
	}

	got := make([]byte, volumeCount)
	n, err := cr.ReadAt(got, 0)
	if err != nil {
		t.Fatalf("ReadAt start: %v", err)
	}
	if n != len(got) {
		t.Fatalf("ReadAt start bytes = %d, want %d", n, len(got))
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("ReadAt start payload mismatch")
	}
	if searchCalls != 1 {
		t.Fatalf("volume index search calls = %d, want 1", searchCalls)
	}

	searchCalls = 0
	mid := make([]byte, 64)
	n, err = cr.ReadAt(mid, 80)
	if err != nil {
		t.Fatalf("ReadAt mid: %v", err)
	}
	if n != len(mid) {
		t.Fatalf("ReadAt mid bytes = %d, want %d", n, len(mid))
	}
	if !bytes.Equal(mid, want[80:144]) {
		t.Fatalf("ReadAt mid payload mismatch")
	}
	if searchCalls != 1 {
		t.Fatalf("volume index mid-search calls = %d, want 1", searchCalls)
	}
}

type volumeEntry struct {
	name    string
	flags   uint8
	payload []byte
}

func writeVolumeArchive(t *testing.T, path string, entries []volumeEntry) {
	t.Helper()

	var buf bytes.Buffer
	w := NewWriter(&buf)
	for _, entry := range entries {
		h := &FileHeader{
			Name:   entry.name,
			Method: Store,
			Flags:  entry.flags,
		}
		fw, err := w.CreateHeader(h)
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

func mustReadFileEntry(t *testing.T, f *File) []byte {
	t.Helper()

	rc, err := f.Open()
	if err != nil {
		t.Fatalf("Open(%s): %v", f.Name, err)
	}
	defer rc.Close()

	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll(%s): %v", f.Name, err)
	}
	return b
}

func mustWriteFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}

func mutateMainHeaderHostData(t *testing.T, path string, hostData uint16) {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", path, err)
	}
	if len(data) < 4 || data[0] != arjHeaderID1 || data[1] != arjHeaderID2 {
		t.Fatalf("invalid main header prefix in %s", path)
	}

	basicSize := int(binary.LittleEndian.Uint16(data[2:4]))
	basicStart := 4
	basicEnd := basicStart + basicSize
	if basicSize < arjMinFirstHeaderSize || basicEnd+4 > len(data) {
		t.Fatalf("main header size out of range in %s: %d", path, basicSize)
	}

	binary.LittleEndian.PutUint16(data[basicStart+28:basicStart+30], hostData)
	binary.LittleEndian.PutUint32(data[basicEnd:basicEnd+4], crc32.ChecksumIEEE(data[basicStart:basicEnd]))

	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}
