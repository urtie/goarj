package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	goarj "goarj"
)

var fixtureTimestamp = time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC)

const (
	compatLongName    = "this-is-a-very-long-filename-for-arj-compatibility-fixture-testing-abcdefghijklmnopqrstuvwxyz-0123456789.txt"
	defaultARJTimeout = 2 * time.Minute
	maxARJOutputBytes = 1 << 20
)

type compatTool struct {
	arjPath    string
	arjTimeout time.Duration
	env        []string
}

type fixtureBuilder struct {
	name  string
	build func(workDir, archiveName string) error
}

type expectedCompatEntry struct {
	name    string
	method  uint16
	comment string
	payload []byte
}

var (
	checkARJToGoFixturesFn = checkARJToGoFixtures
	checkGoToARJFn         = func(tool *compatTool, baseDir string, smoke bool) error {
		return tool.checkGoToARJ(baseDir, smoke)
	}
)

func main() {
	if len(os.Args) < 2 {
		usage(os.Stderr)
		os.Exit(2)
	}

	var err error
	switch os.Args[1] {
	case "regen":
		err = runRegen(os.Args[2:])
	case "check":
		err = runCheck(os.Args[2:])
	case "all":
		err = runAll(os.Args[2:])
	case "-h", "--help", "help":
		usage(os.Stdout)
		return
	default:
		usage(os.Stderr)
		os.Exit(2)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "compattool: %v\n", err)
		os.Exit(1)
	}
}

func runRegen(args []string) error {
	fs := flag.NewFlagSet("regen", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	outDir := fs.String("out", "testdata", "output directory for generated fixtures")
	smoke := fs.Bool("smoke", false, "regenerate a small smoke subset instead of the full fixture set")
	arjBinary := fs.String("arj", "", "path to arj binary (default: resolve \"arj\" from PATH)")
	arjTimeout := fs.Duration("arj-timeout", defaultARJTimeout, "timeout for each arj command")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("regen takes no positional arguments")
	}

	tool, err := newCompatTool(*arjBinary, *arjTimeout)
	if err != nil {
		return err
	}

	if err := tool.generateFixtures(*outDir, *smoke); err != nil {
		return err
	}

	mode := "full"
	if *smoke {
		mode = "smoke"
	}
	fmt.Printf("compattool: regenerated %s fixture set in %s\n", mode, *outDir)
	return nil
}

func runCheck(args []string) error {
	fs := flag.NewFlagSet("check", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	smoke := fs.Bool("smoke", false, "run a fast smoke matrix instead of the full matrix")
	keepWork := fs.Bool("keep-work", false, "keep temporary work directory for debugging")
	arjBinary := fs.String("arj", "", "path to arj binary (default: resolve \"arj\" from PATH)")
	arjTimeout := fs.Duration("arj-timeout", defaultARJTimeout, "timeout for each arj command")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("check takes no positional arguments")
	}

	tool, err := newCompatTool(*arjBinary, *arjTimeout)
	if err != nil {
		return err
	}

	workDir, err := os.MkdirTemp("", "goarj-compat-check-*")
	if err != nil {
		return fmt.Errorf("create work dir: %w", err)
	}
	if *keepWork {
		fmt.Printf("compattool: keeping work dir %s\n", workDir)
	} else {
		defer os.RemoveAll(workDir)
	}

	arjToGoDir := filepath.Join(workDir, "arj-to-go")
	if err := tool.generateFixtures(arjToGoDir, *smoke); err != nil {
		return fmt.Errorf("generate arj->go fixtures: %w", err)
	}
	if err := checkARJToGoFixturesFn(arjToGoDir, *smoke); err != nil {
		return fmt.Errorf("arj->go checks failed: %w", err)
	}

	if err := checkGoToARJFn(tool, filepath.Join(workDir, "go-to-arj"), *smoke); err != nil {
		return fmt.Errorf("go->arj checks failed: %w", err)
	}

	mode := "full"
	if *smoke {
		mode = "smoke"
	}
	fmt.Printf("compattool: %s interoperability checks passed\n", mode)
	return nil
}

func runAll(args []string) error {
	fs := flag.NewFlagSet("all", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	outDir := fs.String("out", "testdata", "output directory for generated fixtures")
	smoke := fs.Bool("smoke", false, "run smoke regen/check instead of full")
	arjBinary := fs.String("arj", "", "path to arj binary (default: resolve \"arj\" from PATH)")
	arjTimeout := fs.Duration("arj-timeout", defaultARJTimeout, "timeout for each arj command")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("all takes no positional arguments")
	}

	tool, err := newCompatTool(*arjBinary, *arjTimeout)
	if err != nil {
		return err
	}
	if err := tool.generateFixtures(*outDir, *smoke); err != nil {
		return err
	}

	workDir, err := os.MkdirTemp("", "goarj-compat-all-*")
	if err != nil {
		return fmt.Errorf("create work dir: %w", err)
	}
	defer os.RemoveAll(workDir)

	arjToGoDir := filepath.Join(workDir, "arj-to-go")
	if err := tool.generateFixtures(arjToGoDir, *smoke); err != nil {
		return fmt.Errorf("generate arj->go fixtures for checks: %w", err)
	}
	if err := checkARJToGoFixturesFn(arjToGoDir, *smoke); err != nil {
		return fmt.Errorf("arj->go checks failed: %w", err)
	}
	if err := checkGoToARJFn(tool, filepath.Join(workDir, "go-to-arj"), *smoke); err != nil {
		return fmt.Errorf("go->arj checks failed: %w", err)
	}

	mode := "full"
	if *smoke {
		mode = "smoke"
	}
	fmt.Printf("compattool: %s regen+check completed\n", mode)
	return nil
}

func newCompatTool(requestedARJPath string, arjTimeout time.Duration) (*compatTool, error) {
	if arjTimeout <= 0 {
		return nil, fmt.Errorf("invalid arj timeout %s: must be > 0", arjTimeout)
	}

	arjPath, err := resolveARJPath(requestedARJPath)
	if err != nil {
		return nil, err
	}

	fmt.Fprintf(os.Stderr, "compattool: using arj binary %s (timeout %s)\n", arjPath, arjTimeout)

	return &compatTool{
		arjPath:    arjPath,
		arjTimeout: arjTimeout,
		env:        deterministicARJEnv(),
	}, nil
}

func resolveARJPath(requested string) (string, error) {
	requested = strings.TrimSpace(requested)
	lookup := requested
	if lookup == "" {
		lookup = "arj"
	}

	path, err := exec.LookPath(lookup)
	if err != nil {
		if requested == "" {
			return "", fmt.Errorf("arj binary not found in PATH: %w", err)
		}
		return "", fmt.Errorf("arj binary %q not found: %w", requested, err)
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve arj binary path %q: %w", path, err)
	}
	cleanedPath := filepath.Clean(absPath)
	if err := validateResolvedARJBinary(cleanedPath); err != nil {
		return "", fmt.Errorf("invalid arj binary %q: %w", cleanedPath, err)
	}
	return cleanedPath, nil
}

func validateResolvedARJBinary(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("path is a directory")
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("path is not a regular file")
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o111 == 0 {
		return fmt.Errorf("path is not executable")
	}
	return nil
}

func deterministicARJEnv() []string {
	env := os.Environ()
	out := make([]string, 0, len(env)+4)
	for _, kv := range env {
		if shouldFilterDeterministicEnvKey(kv) {
			continue
		}
		out = append(out, kv)
	}
	out = append(out, "ARJ_SW=", "TZ=UTC", "LC_ALL=C", "LANG=C")
	return out
}

func shouldFilterDeterministicEnvKey(kv string) bool {
	key := kv
	if i := strings.IndexByte(kv, '='); i >= 0 {
		key = kv[:i]
	}
	return strings.EqualFold(key, "ARJ_SW") ||
		strings.EqualFold(key, "TZ") ||
		strings.EqualFold(key, "LC_ALL") ||
		strings.EqualFold(key, "LANG")
}

func (t *compatTool) runARJ(workDir string, args ...string) error {
	ctx, cancel := context.WithTimeout(context.Background(), t.arjTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, t.arjPath, args...)
	cmd.Dir = workDir
	cmd.Env = t.env
	out := newCappedOutputBuffer(maxARJOutputBytes)
	cmd.Stdout = out
	cmd.Stderr = out
	err := cmd.Run()
	if ctx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("arj %s timed out after %s\noutput:\n%s", strings.Join(args, " "), t.arjTimeout, out.String())
	}
	if err != nil {
		return fmt.Errorf("arj %s failed: %w\noutput:\n%s", strings.Join(args, " "), err, out.String())
	}
	return nil
}

func (t *compatTool) generateFixtures(outDir string, smoke bool) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("create output dir %q: %w", outDir, err)
	}

	for _, fixture := range t.fixtureBuilders(smoke) {
		if err := t.buildAndInstallFixture(outDir, fixture); err != nil {
			return err
		}
	}
	return nil
}

func (t *compatTool) fixtureBuilders(smoke bool) []fixtureBuilder {
	full := []fixtureBuilder{
		{
			name: "compat_method1.arj",
			build: func(workDir, archiveName string) error {
				return t.buildMethodFixture(workDir, archiveName, goarj.Method1)
			},
		},
		{
			name: "compat_method2.arj",
			build: func(workDir, archiveName string) error {
				return t.buildMethodFixture(workDir, archiveName, goarj.Method2)
			},
		},
		{
			name: "compat_method3.arj",
			build: func(workDir, archiveName string) error {
				return t.buildMethodFixture(workDir, archiveName, goarj.Method3)
			},
		},
		{
			name: "compat_method4.arj",
			build: func(workDir, archiveName string) error {
				return t.buildMethodFixture(workDir, archiveName, goarj.Method4)
			},
		},
		{name: "compat_multi_file.arj", build: t.buildMultiFileFixture},
		{name: "compat_nested_paths.arj", build: t.buildNestedPathsFixture},
		{name: "compat_longname_comment.arj", build: t.buildLongNameCommentFixture},
		{name: "compat_mixed_methods.arj", build: t.buildMixedMethodsFixture},
		{name: "compat_chapter_comments.arj", build: t.buildChapterCommentsFixture},
	}

	if !smoke {
		return full
	}
	return []fixtureBuilder{
		full[0],           // compat_method1.arj
		full[6],           // compat_longname_comment.arj
		full[len(full)-1], // compat_chapter_comments.arj
	}
}

func (t *compatTool) buildAndInstallFixture(outDir string, fixture fixtureBuilder) error {
	workDir, err := os.MkdirTemp("", "goarj-compat-fixture-*")
	if err != nil {
		return fmt.Errorf("%s: create temp dir: %w", fixture.name, err)
	}
	defer os.RemoveAll(workDir)

	if err := fixture.build(workDir, fixture.name); err != nil {
		return fmt.Errorf("%s: %w", fixture.name, err)
	}

	src := filepath.Join(workDir, fixture.name)
	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("%s: expected generated archive missing: %w", fixture.name, err)
	}

	dst := filepath.Join(outDir, fixture.name)
	if err := copyFile(src, dst, 0o644); err != nil {
		return fmt.Errorf("%s: write output fixture: %w", fixture.name, err)
	}
	return nil
}

func (t *compatTool) buildMethodFixture(workDir, archiveName string, method uint16) error {
	if err := writeFixtureInput(filepath.Join(workDir, "payload.bin"), compatibilityFixturePayload()); err != nil {
		return err
	}
	return t.runARJ(workDir, "a", "-y", "-i", fmt.Sprintf("-m%d", method), archiveName, "payload.bin")
}

func (t *compatTool) buildMultiFileFixture(workDir, archiveName string) error {
	files := []struct {
		name    string
		payload []byte
	}{
		{name: "alpha.txt", payload: []byte("alpha payload line 1\nalpha payload line 2\n")},
		{name: "beta.bin", payload: []byte("BETA-00112233445566778899\n")},
		{name: "gamma.dat", payload: []byte("gamma fixture bytes with punctuation !@#$%^&*()\n")},
	}
	for _, file := range files {
		if err := writeFixtureInput(filepath.Join(workDir, file.name), file.payload); err != nil {
			return err
		}
	}

	if err := t.runARJ(workDir, "a", "-y", "-i", "-m1", archiveName, "alpha.txt"); err != nil {
		return err
	}
	return t.runARJ(workDir, "a", "-y", "-i", "-m0", archiveName, "beta.bin", "gamma.dat")
}

func (t *compatTool) buildNestedPathsFixture(workDir, archiveName string) error {
	files := []struct {
		name    string
		payload []byte
	}{
		{name: "root.txt", payload: []byte("root-level payload\n")},
		{name: "dir1/child.txt", payload: []byte("child payload in dir1\n")},
		{name: "dir1/dir2/deep.bin", payload: []byte("deep payload dir1/dir2\n")},
	}
	for _, file := range files {
		if err := writeFixtureInput(filepath.Join(workDir, filepath.FromSlash(file.name)), file.payload); err != nil {
			return err
		}
	}

	return t.runARJ(workDir, "a", "-y", "-i", "-m0", archiveName, "root.txt", "dir1/child.txt", "dir1/dir2/deep.bin")
}

func (t *compatTool) buildLongNameCommentFixture(workDir, archiveName string) error {
	if err := writeFixtureInput(filepath.Join(workDir, compatLongName), []byte("long filename payload for compatibility fixture\n")); err != nil {
		return err
	}
	if err := writeFixtureInput(filepath.Join(workDir, "archive.cmt"), []byte("archive comment for long filename fixture")); err != nil {
		return err
	}
	if err := writeFixtureInput(filepath.Join(workDir, "file.cmt"), []byte("file comment for long filename fixture")); err != nil {
		return err
	}

	if err := t.runARJ(workDir, "a", "-y", "-i", "-m1", archiveName, compatLongName); err != nil {
		return err
	}
	return t.runARJ(workDir, "c", "-y", "-i", archiveName, compatLongName, "-zarchive.cmt", "-jzfile.cmt")
}

func (t *compatTool) buildMixedMethodsFixture(workDir, archiveName string) error {
	files := []struct {
		name    string
		method  uint16
		payload []byte
	}{
		{name: "method1.txt", method: goarj.Method1, payload: []byte("method1 payload payload payload payload payload\n")},
		{name: "method2.txt", method: goarj.Method2, payload: []byte("method2 payload payload payload payload payload\n")},
		{name: "method3.txt", method: goarj.Method3, payload: []byte("method3 payload payload payload payload payload\n")},
		{name: "method4.txt", method: goarj.Method4, payload: []byte("method4 payload payload payload payload payload\n")},
	}
	for _, file := range files {
		if err := writeFixtureInput(filepath.Join(workDir, file.name), file.payload); err != nil {
			return err
		}
	}

	for _, file := range files {
		if err := t.runARJ(workDir, "a", "-y", "-i", fmt.Sprintf("-m%d", file.method), archiveName, file.name); err != nil {
			return err
		}
	}
	return nil
}

func (t *compatTool) buildChapterCommentsFixture(workDir, archiveName string) error {
	if err := writeFixtureInput(filepath.Join(workDir, "alpha.txt"), []byte("alpha payload chapter fixture\n")); err != nil {
		return err
	}
	if err := writeFixtureInput(filepath.Join(workDir, "beta.txt"), []byte("beta payload chapter fixture\n")); err != nil {
		return err
	}
	if err := writeFixtureInput(filepath.Join(workDir, "archive.cmt"), []byte("archive-level-comment")); err != nil {
		return err
	}
	if err := writeFixtureInput(filepath.Join(workDir, "alpha.cmt"), []byte("alpha-file-comment")); err != nil {
		return err
	}
	if err := writeFixtureInput(filepath.Join(workDir, "beta.cmt"), []byte("beta-file-comment")); err != nil {
		return err
	}

	if err := t.runARJ(workDir, "a", "-y", "-i", "-m0", archiveName, "alpha.txt", "beta.txt"); err != nil {
		return err
	}
	if err := t.runARJ(workDir, "c", "-y", "-i", archiveName, "-zarchive.cmt"); err != nil {
		return err
	}
	if err := t.runARJ(workDir, "c", "-y", "-i", archiveName, "alpha.txt", "-jzalpha.cmt"); err != nil {
		return err
	}
	if err := t.runARJ(workDir, "c", "-y", "-i", archiveName, "beta.txt", "-jzbeta.cmt"); err != nil {
		return err
	}
	return t.runARJ(workDir, "cc", "-y", "-i", archiveName)
}

func writeFixtureInput(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir for %s: %w", path, err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := os.Chtimes(path, fixtureTimestamp, fixtureTimestamp); err != nil {
		return fmt.Errorf("set timestamp for %s: %w", path, err)
	}
	return nil
}

func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	dstDir := filepath.Dir(dst)
	out, err := os.CreateTemp(dstDir, ".compat-copy-*")
	if err != nil {
		return err
	}
	tmpPath := out.Name()
	removeTemp := true
	defer func() {
		_ = out.Close()
		if removeTemp {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	if err := out.Sync(); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpPath, mode); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, dst); err != nil {
		if !os.IsExist(err) && !errors.Is(err, os.ErrExist) {
			return err
		}
		if removeErr := os.Remove(dst); removeErr != nil {
			return removeErr
		}
		if renameErr := os.Rename(tmpPath, dst); renameErr != nil {
			return renameErr
		}
	}
	removeTemp = false
	return nil
}

type cappedOutputBuffer struct {
	max       int
	buf       bytes.Buffer
	truncated bool
}

func newCappedOutputBuffer(max int) *cappedOutputBuffer {
	if max <= 0 {
		max = 1
	}
	return &cappedOutputBuffer{max: max}
}

func (b *cappedOutputBuffer) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if b.buf.Len() >= b.max {
		b.truncated = true
		return len(p), nil
	}
	remaining := b.max - b.buf.Len()
	if len(p) > remaining {
		_, _ = b.buf.Write(p[:remaining])
		b.truncated = true
		return len(p), nil
	}
	_, _ = b.buf.Write(p)
	return len(p), nil
}

func (b *cappedOutputBuffer) String() string {
	if b == nil {
		return ""
	}
	if !b.truncated {
		return b.buf.String()
	}
	return fmt.Sprintf("%s\n[output truncated at %d bytes]", b.buf.String(), b.max)
}

func compatibilityFixturePayload() []byte {
	return bytes.Repeat([]byte("goarj-fixture-compatibility-block-0123456789\n"), 400)
}

func checkARJToGoFixtures(dir string, smoke bool) error {
	if err := checkMethodFixture(filepath.Join(dir, "compat_method1.arj"), goarj.Method1); err != nil {
		return err
	}

	if !smoke {
		if err := checkMethodFixture(filepath.Join(dir, "compat_method2.arj"), goarj.Method2); err != nil {
			return err
		}
		if err := checkMethodFixture(filepath.Join(dir, "compat_method3.arj"), goarj.Method3); err != nil {
			return err
		}
		if err := checkMethodFixture(filepath.Join(dir, "compat_method4.arj"), goarj.Method4); err != nil {
			return err
		}

		if err := checkSimpleFixtureArchive(
			filepath.Join(dir, "compat_multi_file.arj"),
			"",
			[]expectedCompatEntry{
				{name: "alpha.txt", method: goarj.Method1, comment: "", payload: []byte("alpha payload line 1\nalpha payload line 2\n")},
				{name: "beta.bin", method: goarj.Store, comment: "", payload: []byte("BETA-00112233445566778899\n")},
				{name: "gamma.dat", method: goarj.Store, comment: "", payload: []byte("gamma fixture bytes with punctuation !@#$%^&*()\n")},
			},
		); err != nil {
			return err
		}

		if err := checkSimpleFixtureArchive(
			filepath.Join(dir, "compat_nested_paths.arj"),
			"",
			[]expectedCompatEntry{
				{name: "root.txt", method: goarj.Store, comment: "", payload: []byte("root-level payload\n")},
				{name: "dir1/child.txt", method: goarj.Store, comment: "", payload: []byte("child payload in dir1\n")},
				{name: "dir1/dir2/deep.bin", method: goarj.Store, comment: "", payload: []byte("deep payload dir1/dir2\n")},
			},
		); err != nil {
			return err
		}

		if err := checkSimpleFixtureArchive(
			filepath.Join(dir, "compat_mixed_methods.arj"),
			"",
			[]expectedCompatEntry{
				{name: "method1.txt", method: goarj.Method1, comment: "", payload: []byte("method1 payload payload payload payload payload\n")},
				{name: "method2.txt", method: goarj.Method2, comment: "", payload: []byte("method2 payload payload payload payload payload\n")},
				{name: "method3.txt", method: goarj.Method3, comment: "", payload: []byte("method3 payload payload payload payload payload\n")},
				{name: "method4.txt", method: goarj.Method4, comment: "", payload: []byte("method4 payload payload payload payload payload\n")},
			},
		); err != nil {
			return err
		}
	}

	if err := checkSimpleFixtureArchive(
		filepath.Join(dir, "compat_longname_comment.arj"),
		"archive comment for long filename fixture",
		[]expectedCompatEntry{
			{
				name:    compatLongName,
				method:  goarj.Method1,
				comment: "file comment for long filename fixture",
				payload: []byte("long filename payload for compatibility fixture\n"),
			},
		},
	); err != nil {
		return err
	}

	return checkChapterFixtureArchive(filepath.Join(dir, "compat_chapter_comments.arj"))
}

func checkMethodFixture(path string, method uint16) error {
	r, err := goarj.OpenReader(path)
	if err != nil {
		return fmt.Errorf("%s: OpenReader: %w", path, err)
	}
	defer r.Close()

	if got, want := len(r.File), 1; got != want {
		return fmt.Errorf("%s: file count = %d, want %d", path, got, want)
	}
	file := r.File[0]
	if got, want := file.Method, method; got != want {
		return fmt.Errorf("%s: method = %d, want %d", path, got, want)
	}
	if got, want := file.UncompressedSize64, uint64(len(compatibilityFixturePayload())); got != want {
		return fmt.Errorf("%s: uncompressed size = %d, want %d", path, got, want)
	}

	payload, err := readEntryPayload(file)
	if err != nil {
		return fmt.Errorf("%s: read payload: %w", path, err)
	}
	if !bytes.Equal(payload, compatibilityFixturePayload()) {
		return fmt.Errorf("%s: payload mismatch", path)
	}
	return nil
}

func checkSimpleFixtureArchive(path, archiveComment string, expected []expectedCompatEntry) error {
	r, err := goarj.OpenReader(path)
	if err != nil {
		return fmt.Errorf("%s: OpenReader: %w", path, err)
	}
	defer r.Close()

	if got, want := r.Comment, archiveComment; got != want {
		return fmt.Errorf("%s: archive comment = %q, want %q", path, got, want)
	}
	if got, want := len(r.File), len(expected); got != want {
		return fmt.Errorf("%s: file count = %d, want %d", path, got, want)
	}

	wantByName := make(map[string]expectedCompatEntry, len(expected))
	for _, want := range expected {
		if _, exists := wantByName[want.name]; exists {
			return fmt.Errorf("%s: test setup has duplicate expected file name %q", path, want.name)
		}
		wantByName[want.name] = want
	}

	seen := make(map[string]bool, len(expected))
	for i, file := range r.File {
		want, ok := wantByName[file.Name]
		if !ok {
			return fmt.Errorf("%s: file[%d] unexpected entry %q", path, i, file.Name)
		}
		if seen[file.Name] {
			return fmt.Errorf("%s: file[%d] duplicate entry %q", path, i, file.Name)
		}
		seen[file.Name] = true

		if got := file.Method; got != want.method {
			return fmt.Errorf("%s: file[%d] method = %d, want %d", path, i, got, want.method)
		}
		if got := file.Comment; got != want.comment {
			return fmt.Errorf("%s: file[%d] comment = %q, want %q", path, i, got, want.comment)
		}

		payload, err := readEntryPayload(file)
		if err != nil {
			return fmt.Errorf("%s: file[%d] read payload: %w", path, i, err)
		}
		if !bytes.Equal(payload, want.payload) {
			return fmt.Errorf("%s: file[%d] payload mismatch for %q", path, i, file.Name)
		}
	}

	for name := range wantByName {
		if !seen[name] {
			return fmt.Errorf("%s: missing expected entry %q", path, name)
		}
	}
	return nil
}

func checkChapterFixtureArchive(path string) error {
	r, err := goarj.OpenReader(path)
	if err != nil {
		return fmt.Errorf("%s: OpenReader: %w", path, err)
	}
	defer r.Close()

	if got, want := r.Comment, "archive-level-comment"; got != want {
		return fmt.Errorf("%s: archive comment = %q, want %q", path, got, want)
	}
	wantFiles := map[string]struct {
		comment string
		payload []byte
	}{
		"alpha.txt": {
			comment: "alpha-file-comment",
			payload: []byte("alpha payload chapter fixture\n"),
		},
		"beta.txt": {
			comment: "beta-file-comment",
			payload: []byte("beta payload chapter fixture\n"),
		},
	}

	seenFiles := make(map[string]bool, len(wantFiles))
	var chapterNumber uint8
	markerCount := 0

	setChapterNumber := func(label string, value uint8) error {
		if value == 0 {
			return fmt.Errorf("%s: %s chapter number = 0, want non-zero", path, label)
		}
		if chapterNumber == 0 {
			chapterNumber = value
			return nil
		}
		if value != chapterNumber {
			return fmt.Errorf("%s: %s chapter number = %d, want %d", path, label, value, chapterNumber)
		}
		return nil
	}

	for i, file := range r.File {
		if want, ok := wantFiles[file.Name]; ok {
			if seenFiles[file.Name] {
				return fmt.Errorf("%s: duplicate file entry for %q", path, file.Name)
			}
			seenFiles[file.Name] = true

			if got := file.Comment; got != want.comment {
				return fmt.Errorf("%s: file[%d] comment = %q, want %q", path, i, got, want.comment)
			}
			if got, wantMethod := file.Method, goarj.Store; got != wantMethod {
				return fmt.Errorf("%s: file[%d] method = %d, want %d", path, i, got, wantMethod)
			}
			if err := setChapterNumber(fmt.Sprintf("file[%d]", i), file.ChapterNumber); err != nil {
				return err
			}
			if got, wantSize := file.UncompressedSize64, uint64(len(want.payload)); got != wantSize {
				return fmt.Errorf("%s: file[%d] uncompressed size = %d, want %d", path, i, got, wantSize)
			}

			payload, err := readEntryPayload(file)
			if err != nil {
				return fmt.Errorf("%s: file[%d] read payload: %w", path, i, err)
			}
			if !bytes.Equal(payload, want.payload) {
				return fmt.Errorf("%s: file[%d] payload mismatch", path, i)
			}
			continue
		}

		if !isChapterMarkerName(file.Name) {
			return fmt.Errorf("%s: file[%d] unexpected entry %q", path, i, file.Name)
		}
		markerCount++
		if err := setChapterNumber(fmt.Sprintf("file[%d]", i), file.ChapterNumber); err != nil {
			return err
		}
		payload, err := readEntryPayload(file)
		if err != nil {
			return fmt.Errorf("%s: file[%d] read payload: %w", path, i, err)
		}
		if len(payload) != 0 {
			return fmt.Errorf("%s: file[%d] marker payload length = %d, want 0", path, i, len(payload))
		}
	}

	for name := range wantFiles {
		if !seenFiles[name] {
			return fmt.Errorf("%s: missing expected file %q", path, name)
		}
	}
	if markerCount == 0 {
		return fmt.Errorf("%s: missing chapter marker entry", path)
	}
	if chapterNumber == 0 {
		return fmt.Errorf("%s: missing chapter number metadata", path)
	}
	if got := r.ArchiveHeader.ChapterNumber; got != 0 && got != chapterNumber {
		return fmt.Errorf("%s: archive chapter number = %d, want %d", path, got, chapterNumber)
	}
	return nil
}

func isChapterMarkerName(name string) bool {
	if !strings.HasPrefix(name, "<<<") || !strings.HasSuffix(name, ">>>") {
		return false
	}
	marker := strings.TrimSuffix(strings.TrimPrefix(name, "<<<"), ">>>")
	if marker == "" {
		return false
	}
	for _, r := range marker {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func readEntryPayload(file *goarj.File) ([]byte, error) {
	rc, err := file.Open()
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(rc)
	closeErr := rc.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return data, nil
}

func (t *compatTool) checkGoToARJ(baseDir string, smoke bool) error {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return fmt.Errorf("create go->arj dir: %w", err)
	}

	methods := []uint16{goarj.Store, goarj.Method1, goarj.Method2, goarj.Method3, goarj.Method4}
	if smoke {
		methods = []uint16{goarj.Store, goarj.Method4}
	}

	for _, method := range methods {
		workDir := filepath.Join(baseDir, fmt.Sprintf("method-%d", method))
		if err := os.MkdirAll(workDir, 0o755); err != nil {
			return fmt.Errorf("create method work dir: %w", err)
		}

		fileName := fmt.Sprintf("payload-method-%d.bin", method)
		payload := interopPayload(method)
		archiveBytes, err := buildGoArchive(fileName, method, payload)
		if err != nil {
			return fmt.Errorf("build go archive for method %d: %w", method, err)
		}

		archiveName := fmt.Sprintf("go-produced-method-%d.arj", method)
		archivePath := filepath.Join(workDir, archiveName)
		if err := os.WriteFile(archivePath, archiveBytes, 0o644); err != nil {
			return fmt.Errorf("write %s: %w", archivePath, err)
		}

		extractDir := filepath.Join(workDir, "extract")
		if err := os.MkdirAll(extractDir, 0o755); err != nil {
			return fmt.Errorf("create extract dir: %w", err)
		}

		if err := t.runARJ(workDir, "x", "-y", "-i", archiveName, "-ht"+extractDir); err != nil {
			return fmt.Errorf("extract method %d archive with arj: %w", method, err)
		}

		gotPayload, err := os.ReadFile(filepath.Join(extractDir, fileName))
		if err != nil {
			return fmt.Errorf("read extracted payload for method %d: %w", method, err)
		}
		if !bytes.Equal(gotPayload, payload) {
			return fmt.Errorf("payload mismatch after arj extraction for method %d", method)
		}
	}
	return nil
}

func buildGoArchive(fileName string, method uint16, payload []byte) ([]byte, error) {
	var archive bytes.Buffer
	w := goarj.NewWriter(&archive)
	fw, err := w.CreateHeader(&goarj.FileHeader{Name: fileName, Method: method})
	if err != nil {
		return nil, err
	}
	if _, err := fw.Write(payload); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return archive.Bytes(), nil
}

func interopPayload(method uint16) []byte {
	header := []byte(fmt.Sprintf("interop-payload-method-%d\n", method))
	body := bytes.Repeat([]byte{
		byte(method),
		0x00,
		0x7F,
		0x80,
		0xFF,
		'\n',
	}, 4096)
	return append(header, body...)
}

func usage(w io.Writer) {
	fmt.Fprintf(w, "Usage: %s <command> [options]\n", filepath.Base(os.Args[0]))
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Commands:")
	fmt.Fprintln(w, "  regen   Regenerate compatibility fixtures using the reference arj binary.")
	fmt.Fprintln(w, "  check   Run bidirectional interoperability checks (arj->go and go->arj).")
	fmt.Fprintln(w, "  all     Run regen and check in a single invocation.")
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Common flags (for regen/check/all):")
	fmt.Fprintln(w, "  -arj <path>              Explicit arj binary path.")
	fmt.Fprintln(w, "  -arj-timeout <duration>  Timeout per arj invocation (default 2m0s).")
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Examples:")
	fmt.Fprintln(w, "  go run ./cmd/compattool regen")
	fmt.Fprintln(w, "  go run ./cmd/compattool check -smoke")
	fmt.Fprintln(w, "  go run ./cmd/compattool all -out testdata")
}
