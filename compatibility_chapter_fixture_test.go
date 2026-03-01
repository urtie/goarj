package arj

import (
	"bytes"
	"io"
	"path/filepath"
	"strings"
	"testing"
)

type chapterCompatFixtureFile struct {
	name          string
	comment       string
	method        uint16
	flags         uint8
	extFlags      uint8
	chapterNumber uint8
	hostData      uint16
	payload       []byte
}

func readFixtureFilePayload(t *testing.T, f *File) []byte {
	t.Helper()

	rc, err := f.Open()
	if err != nil {
		t.Fatalf("Open %q: %v", f.Name, err)
	}
	data, err := io.ReadAll(rc)
	closeErr := rc.Close()
	if err != nil {
		t.Fatalf("ReadAll %q: %v", f.Name, err)
	}
	if closeErr != nil {
		t.Fatalf("Close %q: %v", f.Name, closeErr)
	}
	return data
}

func TestCompatibilityChapterCommentsFixture(t *testing.T) {
	t.Parallel()

	rc, err := OpenReader(filepath.Join("testdata", "compat_chapter_comments.arj"))
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer rc.Close()

	if got, want := rc.ArchiveName, "compat_chapter_comments.arj"; got != want {
		t.Fatalf("archive name = %q, want %q", got, want)
	}
	if got, want := rc.Comment, "archive-level-comment"; got != want {
		t.Fatalf("archive comment = %q, want %q", got, want)
	}
	if got, want := rc.ArchiveHeader.Flags, uint8(0x10); got != want {
		t.Fatalf("archive flags = 0x%02x, want 0x%02x", got, want)
	}
	if got, want := rc.ArchiveHeader.ExtFlags, uint8(0x00); got != want {
		t.Fatalf("archive ext flags = 0x%02x, want 0x%02x", got, want)
	}
	if got, want := rc.ArchiveHeader.ChapterNumber, uint8(1); got != want {
		t.Fatalf("archive chapter number = %d, want %d", got, want)
	}
	if got, want := rc.ArchiveHeader.HostData, uint16(0x0100); got != want {
		t.Fatalf("archive host data = 0x%04x, want 0x%04x", got, want)
	}

	wantFiles := map[string]chapterCompatFixtureFile{
		"alpha.txt": {
			name:          "alpha.txt",
			comment:       "alpha-file-comment",
			method:        Store,
			flags:         0x10,
			extFlags:      0x01,
			chapterNumber: 1,
			hostData:      0x0101,
			payload:       []byte("alpha payload chapter fixture\n"),
		},
		"beta.txt": {
			name:          "beta.txt",
			comment:       "beta-file-comment",
			method:        Store,
			flags:         0x10,
			extFlags:      0x01,
			chapterNumber: 1,
			hostData:      0x0101,
			payload:       []byte("beta payload chapter fixture\n"),
		},
	}
	wantMarker := chapterCompatFixtureFile{
		comment:       "",
		method:        Store,
		flags:         0x00,
		extFlags:      0x01,
		chapterNumber: 1,
		hostData:      0x0101,
		payload:       nil,
	}

	if got, want := len(rc.File), len(wantFiles)+1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}

	seen := make(map[string]bool, len(wantFiles))
	markerCount := 0
	for i, f := range rc.File {
		want, ok := wantFiles[f.Name]
		if !ok {
			if !isChapterFixtureMarkerName(f.Name) {
				t.Fatalf("file[%d] unexpected entry %q", i, f.Name)
			}
			markerCount++
			want = wantMarker
		} else if seen[f.Name] {
			t.Fatalf("file[%d] duplicate entry %q", i, f.Name)
		} else {
			seen[f.Name] = true
		}

		if got := f.Comment; got != want.comment {
			t.Fatalf("file[%d] comment = %q, want %q", i, got, want.comment)
		}
		if got := f.Method; got != want.method {
			t.Fatalf("file[%d] method = %d, want %d", i, got, want.method)
		}
		if got := f.Flags; got != want.flags {
			t.Fatalf("file[%d] flags = 0x%02x, want 0x%02x", i, got, want.flags)
		}
		if got := f.ExtFlags; got != want.extFlags {
			t.Fatalf("file[%d] ext flags = 0x%02x, want 0x%02x", i, got, want.extFlags)
		}
		if got := f.ChapterNumber; got != want.chapterNumber {
			t.Fatalf("file[%d] chapter number = %d, want %d", i, got, want.chapterNumber)
		}
		if got := f.HostData; got != want.hostData {
			t.Fatalf("file[%d] host data = 0x%04x, want 0x%04x", i, got, want.hostData)
		}
		if got, wantSize := f.UncompressedSize64, uint64(len(want.payload)); got != wantSize {
			t.Fatalf("file[%d] uncompressed size = %d, want %d", i, got, wantSize)
		}

		if gotPayload := readFixtureFilePayload(t, f); !bytes.Equal(gotPayload, want.payload) {
			t.Fatalf("file[%d] payload mismatch for %q", i, f.Name)
		}
	}
	for name := range wantFiles {
		if !seen[name] {
			t.Fatalf("missing expected entry %q", name)
		}
	}
	if markerCount != 1 {
		t.Fatalf("chapter marker count = %d, want %d", markerCount, 1)
	}
}

func TestCopyRoundTripPreservesChapterCommentsFixtureMetadata(t *testing.T) {
	t.Parallel()

	src, err := OpenReader(filepath.Join("testdata", "compat_chapter_comments.arj"))
	if err != nil {
		t.Fatalf("OpenReader source: %v", err)
	}
	defer src.Close()

	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.SetArchiveHeader(&src.ArchiveHeader); err != nil {
		t.Fatalf("SetArchiveHeader: %v", err)
	}
	for _, f := range src.File {
		if err := w.Copy(f); err != nil {
			t.Fatalf("Copy %q: %v", f.Name, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	dst, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("NewReader destination: %v", err)
	}

	if got, want := dst.ArchiveName, src.ArchiveName; got != want {
		t.Fatalf("archive name = %q, want %q", got, want)
	}
	if got, want := dst.Comment, src.Comment; got != want {
		t.Fatalf("archive comment = %q, want %q", got, want)
	}
	if got, want := dst.ArchiveHeader.Flags, src.ArchiveHeader.Flags; got != want {
		t.Fatalf("archive flags = 0x%02x, want 0x%02x", got, want)
	}
	if got, want := dst.ArchiveHeader.ExtFlags, src.ArchiveHeader.ExtFlags; got != want {
		t.Fatalf("archive ext flags = 0x%02x, want 0x%02x", got, want)
	}
	if got, want := dst.ArchiveHeader.ChapterNumber, src.ArchiveHeader.ChapterNumber; got != want {
		t.Fatalf("archive chapter number = %d, want %d", got, want)
	}
	if got, want := dst.ArchiveHeader.HostData, src.ArchiveHeader.HostData; got != want {
		t.Fatalf("archive host data = 0x%04x, want 0x%04x", got, want)
	}

	if got, want := len(dst.File), len(src.File); got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}

	srcByName := make(map[string]*File, len(src.File))
	for i, srcFile := range src.File {
		if _, exists := srcByName[srcFile.Name]; exists {
			t.Fatalf("source duplicate file[%d] name %q", i, srcFile.Name)
		}
		srcByName[srcFile.Name] = srcFile
	}
	dstByName := make(map[string]*File, len(dst.File))
	for i, dstFile := range dst.File {
		if _, exists := dstByName[dstFile.Name]; exists {
			t.Fatalf("destination duplicate file[%d] name %q", i, dstFile.Name)
		}
		dstByName[dstFile.Name] = dstFile
	}
	if got, want := len(dstByName), len(srcByName); got != want {
		t.Fatalf("mapped file count = %d, want %d", got, want)
	}

	for name, srcFile := range srcByName {
		dstFile, ok := dstByName[name]
		if !ok {
			t.Fatalf("missing destination entry %q", name)
		}
		if got, want := dstFile.Comment, srcFile.Comment; got != want {
			t.Fatalf("%q comment = %q, want %q", name, got, want)
		}
		if got, want := dstFile.Method, srcFile.Method; got != want {
			t.Fatalf("%q method = %d, want %d", name, got, want)
		}
		if got, want := dstFile.Flags, srcFile.Flags; got != want {
			t.Fatalf("%q flags = 0x%02x, want 0x%02x", name, got, want)
		}
		if got, want := dstFile.ExtFlags, srcFile.ExtFlags; got != want {
			t.Fatalf("%q ext flags = 0x%02x, want 0x%02x", name, got, want)
		}
		if got, want := dstFile.ChapterNumber, srcFile.ChapterNumber; got != want {
			t.Fatalf("%q chapter number = %d, want %d", name, got, want)
		}
		if got, want := dstFile.HostData, srcFile.HostData; got != want {
			t.Fatalf("%q host data = 0x%04x, want 0x%04x", name, got, want)
		}

		srcPayload := readFixtureFilePayload(t, srcFile)
		dstPayload := readFixtureFilePayload(t, dstFile)
		if !bytes.Equal(dstPayload, srcPayload) {
			t.Fatalf("%q payload mismatch after copy", name)
		}
	}
}

func isChapterFixtureMarkerName(name string) bool {
	if !strings.HasPrefix(name, "<<<") || !strings.HasSuffix(name, ">>>") {
		return false
	}
	id := strings.TrimSuffix(strings.TrimPrefix(name, "<<<"), ">>>")
	if id == "" {
		return false
	}
	for _, ch := range id {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}
