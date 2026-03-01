package arj

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateLocalHeaderLengthsBoundariesAndEmbeddedNUL(t *testing.T) {
	firstSize := int(arjMinFirstHeaderSize)
	maxFieldLen := arjMaxBasicHeaderSize - firstSize - 2

	okName := &FileHeader{
		FirstHeaderSize: arjMinFirstHeaderSize,
		Name:            strings.Repeat("n", maxFieldLen),
	}
	if err := validateLocalHeaderLengths(okName); err != nil {
		t.Fatalf("validateLocalHeaderLengths(name at boundary) = %v, want nil", err)
	}

	tooLongName := *okName
	tooLongName.Name += "x"
	if err := validateLocalHeaderLengths(&tooLongName); !errors.Is(err, errLongName) {
		t.Fatalf("validateLocalHeaderLengths(name too long) = %v, want %v", err, errLongName)
	}

	okComment := &FileHeader{
		FirstHeaderSize: arjMinFirstHeaderSize,
		Comment:         strings.Repeat("c", maxFieldLen),
	}
	if err := validateLocalHeaderLengths(okComment); err != nil {
		t.Fatalf("validateLocalHeaderLengths(comment at boundary) = %v, want nil", err)
	}

	tooLongComment := *okComment
	tooLongComment.Comment += "x"
	if err := validateLocalHeaderLengths(&tooLongComment); !errors.Is(err, errLongComment) {
		t.Fatalf("validateLocalHeaderLengths(comment too long) = %v, want %v", err, errLongComment)
	}

	combined := &FileHeader{
		FirstHeaderSize: arjMinFirstHeaderSize,
		Name:            strings.Repeat("n", maxFieldLen-1),
		Comment:         "x",
	}
	if err := validateLocalHeaderLengths(combined); err != nil {
		t.Fatalf("validateLocalHeaderLengths(combined boundary) = %v, want nil", err)
	}
	combined.Comment = "xx"
	if err := validateLocalHeaderLengths(combined); !errors.Is(err, errLongComment) {
		t.Fatalf("validateLocalHeaderLengths(combined too long) = %v, want %v", err, errLongComment)
	}

	if err := validateLocalHeaderLengths(&FileHeader{
		FirstHeaderSize: arjMinFirstHeaderSize,
		Name:            "bad\x00name",
	}); !errors.Is(err, ErrFormat) {
		t.Fatalf("validateLocalHeaderLengths(name with NUL) = %v, want %v", err, ErrFormat)
	}
	if err := validateLocalHeaderLengths(&FileHeader{
		FirstHeaderSize: arjMinFirstHeaderSize,
		Comment:         "bad\x00comment",
	}); !errors.Is(err, ErrFormat) {
		t.Fatalf("validateLocalHeaderLengths(comment with NUL) = %v, want %v", err, ErrFormat)
	}
	if err := validateLocalHeaderLengths(&FileHeader{
		FirstHeaderSize: arjMinFirstHeaderSize - 1,
		Name:            "small-first-size",
	}); !errors.Is(err, ErrFormat) {
		t.Fatalf("validateLocalHeaderLengths(first size too small) = %v, want %v", err, ErrFormat)
	}
}

func TestValidateMainHeaderLengthsBoundariesAndEmbeddedNUL(t *testing.T) {
	firstSize := int(arjMinFirstHeaderSize)
	maxFieldLen := arjMaxBasicHeaderSize - firstSize - 2

	okName := &ArchiveHeader{
		FirstHeaderSize: arjMinFirstHeaderSize,
		Name:            strings.Repeat("a", maxFieldLen),
	}
	if err := validateMainHeaderLengths(okName); err != nil {
		t.Fatalf("validateMainHeaderLengths(name at boundary) = %v, want nil", err)
	}

	tooLongName := *okName
	tooLongName.Name += "x"
	if err := validateMainHeaderLengths(&tooLongName); !errors.Is(err, errLongArchiveName) {
		t.Fatalf("validateMainHeaderLengths(name too long) = %v, want %v", err, errLongArchiveName)
	}

	okComment := &ArchiveHeader{
		FirstHeaderSize: arjMinFirstHeaderSize,
		Comment:         strings.Repeat("c", maxFieldLen),
	}
	if err := validateMainHeaderLengths(okComment); err != nil {
		t.Fatalf("validateMainHeaderLengths(comment at boundary) = %v, want nil", err)
	}

	tooLongComment := *okComment
	tooLongComment.Comment += "x"
	if err := validateMainHeaderLengths(&tooLongComment); !errors.Is(err, errLongArchiveComment) {
		t.Fatalf("validateMainHeaderLengths(comment too long) = %v, want %v", err, errLongArchiveComment)
	}

	combined := &ArchiveHeader{
		FirstHeaderSize: arjMinFirstHeaderSize,
		Name:            strings.Repeat("n", maxFieldLen-1),
		Comment:         "x",
	}
	if err := validateMainHeaderLengths(combined); err != nil {
		t.Fatalf("validateMainHeaderLengths(combined boundary) = %v, want nil", err)
	}
	combined.Comment = "xx"
	if err := validateMainHeaderLengths(combined); !errors.Is(err, errLongArchiveComment) {
		t.Fatalf("validateMainHeaderLengths(combined too long) = %v, want %v", err, errLongArchiveComment)
	}

	if err := validateMainHeaderLengths(&ArchiveHeader{
		FirstHeaderSize: arjMinFirstHeaderSize,
		Name:            "bad\x00name",
	}); !errors.Is(err, ErrFormat) {
		t.Fatalf("validateMainHeaderLengths(name with NUL) = %v, want %v", err, ErrFormat)
	}
	if err := validateMainHeaderLengths(&ArchiveHeader{
		FirstHeaderSize: arjMinFirstHeaderSize,
		Comment:         "bad\x00comment",
	}); !errors.Is(err, ErrFormat) {
		t.Fatalf("validateMainHeaderLengths(comment with NUL) = %v, want %v", err, ErrFormat)
	}
	if err := validateMainHeaderLengths(&ArchiveHeader{
		FirstHeaderSize: arjMinFirstHeaderSize - 1,
		Name:            "small-first-size",
	}); !errors.Is(err, ErrFormat) {
		t.Fatalf("validateMainHeaderLengths(first size too small) = %v, want %v", err, ErrFormat)
	}
}

func TestEnsureNoSymlinkComponentsIncludeTargetModes(t *testing.T) {
	tmp := t.TempDir()
	root := filepath.Join(tmp, "root")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("MkdirAll(root): %v", err)
	}

	outsideFile := filepath.Join(tmp, "outside.txt")
	if err := os.WriteFile(outsideFile, []byte("outside"), 0o600); err != nil {
		t.Fatalf("WriteFile(outside): %v", err)
	}
	linkTarget := filepath.Join(root, "leaf.txt")
	symlinkOrSkip(t, outsideFile, linkTarget)

	if err := ensureNoSymlinkComponents(root, linkTarget, "leaf.txt", false); err != nil {
		t.Fatalf("ensureNoSymlinkComponents(includeTarget=false) = %v, want nil", err)
	}
	if err := ensureNoSymlinkComponents(root, linkTarget, "leaf.txt", true); !errors.Is(err, ErrInsecurePath) {
		t.Fatalf("ensureNoSymlinkComponents(includeTarget=true) = %v, want %v", err, ErrInsecurePath)
	}

	outsideDir := filepath.Join(tmp, "outside-dir")
	if err := os.MkdirAll(outsideDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(outside-dir): %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "nested"), 0o755); err != nil {
		t.Fatalf("MkdirAll(root/nested): %v", err)
	}
	intermediate := filepath.Join(root, "nested", "link")
	symlinkOrSkip(t, outsideDir, intermediate)
	intermediateTarget := filepath.Join(intermediate, "child.txt")
	if err := ensureNoSymlinkComponents(root, intermediateTarget, "nested/link/child.txt", false); !errors.Is(err, ErrInsecurePath) {
		t.Fatalf("ensureNoSymlinkComponents(intermediate symlink) = %v, want %v", err, ErrInsecurePath)
	}
}

func TestSafeExtractRelativePathPropertyInvariants(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 2000; i++ {
		name := randomPathCandidate(rng)
		assertSafeExtractRelativePathInvariants(t, name)
	}
}

func FuzzSafeExtractRelativePathInvariants(f *testing.F) {
	seeds := []string{
		"",
		".",
		"..",
		"../secret.txt",
		"/etc/passwd",
		"safe/path.txt",
		"safe/dir/",
		"a//b",
		"a/./b",
		`a\b`,
		"a\x00b",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, name string) {
		assertSafeExtractRelativePathInvariants(t, name)
	})
}

func TestGarbledReaderShortReadContinuity(t *testing.T) {
	plain := []byte("garbled-short-read-continuity")
	password := []byte("pw")
	modifier := uint8(0x5a)

	cipher := append([]byte(nil), plain...)
	applyGarbledXORInPlace(cipher, password, modifier, 0)

	gr := newGarbledReader(&fixedChunkReader{data: cipher, maxChunk: 1}, password, modifier)
	got, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("decoded payload = %q, want %q", got, plain)
	}
	if len(gr.password) != 0 {
		t.Fatalf("password length after EOF = %d, want 0", len(gr.password))
	}
	if gr.modifier != 0 {
		t.Fatalf("modifier after EOF = %d, want 0", gr.modifier)
	}
	if gr.idx != 0 {
		t.Fatalf("idx after EOF = %d, want 0", gr.idx)
	}
}

func TestGarbledReaderCopiesPasswordSlice(t *testing.T) {
	plain := []byte("garbled-password-copy")
	password := []byte("secret")
	modifier := uint8(0x33)

	cipher := append([]byte(nil), plain...)
	applyGarbledXORInPlace(cipher, password, modifier, 0)

	gr := newGarbledReader(bytes.NewReader(cipher), password, modifier)
	for i := range password {
		password[i] = 'x'
	}

	got, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("decoded payload = %q, want %q", got, plain)
	}
}

func assertSafeExtractRelativePathInvariants(t *testing.T, name string) {
	t.Helper()

	got, err := safeExtractRelativePath(name)
	if err != nil {
		if !errors.Is(err, ErrInsecurePath) {
			t.Fatalf("safeExtractRelativePath(%q) error = %v, want %v", name, err, ErrInsecurePath)
		}
		var pathErr *fs.PathError
		if !errors.As(err, &pathErr) {
			t.Fatalf("safeExtractRelativePath(%q) error type = %T, want *fs.PathError", name, err)
		}
		if pathErr.Op != "extract" {
			t.Fatalf("safeExtractRelativePath(%q) op = %q, want %q", name, pathErr.Op, "extract")
		}
		if pathErr.Path != name {
			t.Fatalf("safeExtractRelativePath(%q) path = %q, want %q", name, pathErr.Path, name)
		}
		return
	}

	if got == "" || got == "." {
		t.Fatalf("safeExtractRelativePath(%q) = %q, want non-empty relative path", name, got)
	}
	if !filepath.IsLocal(got) {
		t.Fatalf("safeExtractRelativePath(%q) = %q, want filepath.IsLocal=true", name, got)
	}
	if !fs.ValidPath(filepath.ToSlash(got)) {
		t.Fatalf("safeExtractRelativePath(%q) = %q, want fs.ValidPath(to-slash)=true", name, got)
	}
}

func randomPathCandidate(rng *rand.Rand) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789./\\:-_"

	n := rng.Intn(24)
	var b strings.Builder
	for i := 0; i < n; i++ {
		b.WriteByte(alphabet[rng.Intn(len(alphabet))])
	}
	out := b.String()

	switch rng.Intn(8) {
	case 0:
		return ""
	case 1:
		return "." + out
	case 2:
		return ".." + out
	case 3:
		return "../" + out
	case 4:
		return "/" + out
	case 5:
		return out + "/"
	default:
		return out
	}
}

type fixedChunkReader struct {
	data     []byte
	maxChunk int
}

func (r *fixedChunkReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	n := r.maxChunk
	if n <= 0 || n > len(r.data) {
		n = len(r.data)
	}
	if n > len(p) {
		n = len(p)
	}
	copy(p, r.data[:n])
	r.data = r.data[n:]
	return n, nil
}
