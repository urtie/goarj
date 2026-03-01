package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	goarj "goarj"
)

func TestResolveARJPathExplicit(t *testing.T) {
	arjPath := helperARJBinaryPath(t)

	got, err := resolveARJPath(arjPath)
	if err != nil {
		t.Fatalf("resolveARJPath(%q): %v", arjPath, err)
	}
	if got != filepath.Clean(arjPath) {
		t.Fatalf("resolveARJPath(%q) = %q, want %q", arjPath, got, filepath.Clean(arjPath))
	}
}

func TestRunARJWithRelativeARJPath(t *testing.T) {
	tmp := t.TempDir()
	helperPath := helperARJBinaryPath(t)
	helperName := "arj-helper" + filepath.Ext(helperPath)
	helperDir := filepath.Join(tmp, "bin")
	if err := os.MkdirAll(helperDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", helperDir, err)
	}
	helperCopy := filepath.Join(helperDir, helperName)
	if err := copyFile(helperPath, helperCopy, 0o755); err != nil {
		t.Fatalf("copyFile(%s, %s): %v", helperPath, helperCopy, err)
	}

	t.Chdir(tmp)
	t.Setenv(compatARJHelperModeEnv, "noop")

	tool, err := newCompatTool(filepath.Join(".", "bin", helperName), 5*time.Second)
	if err != nil {
		t.Fatalf("newCompatTool relative path: %v", err)
	}
	if !filepath.IsAbs(tool.arjPath) {
		t.Fatalf("newCompatTool resolved non-absolute arj path: %q", tool.arjPath)
	}
	if got, want := tool.arjPath, filepath.Clean(helperCopy); got != want {
		t.Fatalf("newCompatTool resolved arj path = %q, want %q", got, want)
	}

	workDir := t.TempDir()
	if err := tool.runARJ(workDir, "a", "-y", "-i", "archive.arj", "payload.bin"); err != nil {
		t.Fatalf("runARJ with relative arj path: %v", err)
	}
}

func TestResolveARJPathMissingExplicit(t *testing.T) {
	tmp := t.TempDir()
	_, err := resolveARJPath(filepath.Join(tmp, "missing-arj"))
	if err == nil {
		t.Fatalf("resolveARJPath missing path: expected error")
	}
}

func TestValidateResolvedARJBinaryAcceptsExecutable(t *testing.T) {
	if err := validateResolvedARJBinary(helperARJBinaryPath(t)); err != nil {
		t.Fatalf("validateResolvedARJBinary executable: %v", err)
	}
}

func TestValidateResolvedARJBinaryRejectsDirectory(t *testing.T) {
	err := validateResolvedARJBinary(t.TempDir())
	if err == nil {
		t.Fatalf("validateResolvedARJBinary directory: expected error")
	}
	if !strings.Contains(err.Error(), "directory") {
		t.Fatalf("validateResolvedARJBinary directory error = %q, want substring %q", err, "directory")
	}
}

func TestValidateResolvedARJBinaryRejectsNonExecutableFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("non-executable permission bits are not meaningful on windows")
	}

	tmp := t.TempDir()
	path := filepath.Join(tmp, "not-executable-arj")
	if err := os.WriteFile(path, []byte("stub"), 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}

	err := validateResolvedARJBinary(path)
	if err == nil {
		t.Fatalf("validateResolvedARJBinary non-executable file: expected error")
	}
	if !strings.Contains(err.Error(), "not executable") {
		t.Fatalf("validateResolvedARJBinary non-executable error = %q, want substring %q", err, "not executable")
	}
}

func TestRunARJTimeout(t *testing.T) {
	tmp := t.TempDir()
	arjPath := helperARJPathForMode(t, "sleep")

	tool := &compatTool{
		arjPath:    arjPath,
		arjTimeout: 50 * time.Millisecond,
		env:        os.Environ(),
	}

	err := tool.runARJ(tmp, "a", "-y")
	if err == nil {
		t.Fatalf("runARJ timeout: expected error")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("runARJ timeout error = %q, want substring %q", err, "timed out")
	}
}

func TestRunARJOutputIsBounded(t *testing.T) {
	tmp := t.TempDir()
	arjPath := helperARJPathForMode(t, "flood")

	tool := &compatTool{
		arjPath:    arjPath,
		arjTimeout: 5 * time.Second,
		env:        os.Environ(),
	}

	err := tool.runARJ(tmp, "a", "-y")
	if err == nil {
		t.Fatalf("runARJ flood: expected error")
	}
	if !strings.Contains(err.Error(), "output truncated") {
		t.Fatalf("runARJ flood error = %q, want truncated marker", err)
	}
	if got, max := len(err.Error()), maxARJOutputBytes+4096; got > max {
		t.Fatalf("runARJ flood error length = %d, want <= %d", got, max)
	}
}

func TestCopyFile(t *testing.T) {
	tmp := t.TempDir()
	src := filepath.Join(tmp, "src.bin")
	dst := filepath.Join(tmp, "dst.bin")

	payload := bytes.Repeat([]byte("goarj-copy-buffer-test-0123456789"), 1<<10)
	if err := os.WriteFile(src, payload, 0o600); err != nil {
		t.Fatalf("WriteFile(%s): %v", src, err)
	}

	if err := copyFile(src, dst, 0o640); err != nil {
		t.Fatalf("copyFile: %v", err)
	}

	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", dst, err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("copy payload mismatch")
	}

	fi, err := os.Stat(dst)
	if err != nil {
		t.Fatalf("Stat(%s): %v", dst, err)
	}
	if runtime.GOOS != "windows" {
		if gotMode := fi.Mode().Perm(); gotMode != 0o640 {
			t.Fatalf("dst mode = %#o, want %#o", gotMode, 0o640)
		}
	}
}

func TestDeterministicARJEnv(t *testing.T) {
	t.Setenv("ARJ_SW", "custom-options")
	t.Setenv("TZ", "America/New_York")
	t.Setenv("LC_ALL", "en_US.UTF-8")
	t.Setenv("LANG", "en_US.UTF-8")
	t.Setenv("GOARJ_TEST_KEEP", "1")

	env := deterministicARJEnv()

	if !containsEnvEntry(env, "ARJ_SW=") {
		t.Fatalf("deterministic env missing ARJ_SW reset")
	}
	if !containsEnvEntry(env, "TZ=UTC") {
		t.Fatalf("deterministic env missing TZ=UTC")
	}
	if !containsEnvEntry(env, "LC_ALL=C") {
		t.Fatalf("deterministic env missing LC_ALL=C")
	}
	if !containsEnvEntry(env, "LANG=C") {
		t.Fatalf("deterministic env missing LANG=C")
	}
	if containsEnvEntry(env, "ARJ_SW=custom-options") {
		t.Fatalf("deterministic env unexpectedly kept caller ARJ_SW")
	}
	if containsEnvEntry(env, "TZ=America/New_York") {
		t.Fatalf("deterministic env unexpectedly kept caller TZ")
	}
	if containsEnvEntry(env, "LC_ALL=en_US.UTF-8") {
		t.Fatalf("deterministic env unexpectedly kept caller LC_ALL")
	}
	if containsEnvEntry(env, "LANG=en_US.UTF-8") {
		t.Fatalf("deterministic env unexpectedly kept caller LANG")
	}
	if !containsEnvEntry(env, "GOARJ_TEST_KEEP=1") {
		t.Fatalf("deterministic env dropped unrelated variable")
	}
}

func TestDeterministicARJEnvFiltersMixedCaseControlVars(t *testing.T) {
	t.Setenv("aRj_sW", "mixed-options")
	t.Setenv("tZ", "Europe/Paris")
	t.Setenv("Lc_All", "fr_FR.UTF-8")
	t.Setenv("lAnG", "fr_FR.UTF-8")

	env := deterministicARJEnv()

	if containsEnvEntry(env, "aRj_sW=mixed-options") {
		t.Fatalf("deterministic env unexpectedly kept mixed-case ARJ_SW")
	}
	if containsEnvEntry(env, "tZ=Europe/Paris") {
		t.Fatalf("deterministic env unexpectedly kept mixed-case TZ")
	}
	if containsEnvEntry(env, "Lc_All=fr_FR.UTF-8") {
		t.Fatalf("deterministic env unexpectedly kept mixed-case LC_ALL")
	}
	if containsEnvEntry(env, "lAnG=fr_FR.UTF-8") {
		t.Fatalf("deterministic env unexpectedly kept mixed-case LANG")
	}
	if !containsEnvEntry(env, "ARJ_SW=") {
		t.Fatalf("deterministic env missing ARJ_SW reset")
	}
	if !containsEnvEntry(env, "TZ=UTC") {
		t.Fatalf("deterministic env missing TZ=UTC")
	}
	if !containsEnvEntry(env, "LC_ALL=C") {
		t.Fatalf("deterministic env missing LC_ALL=C")
	}
	if !containsEnvEntry(env, "LANG=C") {
		t.Fatalf("deterministic env missing LANG=C")
	}
}

func TestFixtureBuildersSelection(t *testing.T) {
	tool := &compatTool{}

	full := fixtureNames(tool.fixtureBuilders(false))
	wantFull := []string{
		"compat_method1.arj",
		"compat_method2.arj",
		"compat_method3.arj",
		"compat_method4.arj",
		"compat_multi_file.arj",
		"compat_nested_paths.arj",
		"compat_longname_comment.arj",
		"compat_mixed_methods.arj",
		"compat_chapter_comments.arj",
	}
	if got, want := strings.Join(full, ","), strings.Join(wantFull, ","); got != want {
		t.Fatalf("full fixtures = %q, want %q", got, want)
	}

	smoke := fixtureNames(tool.fixtureBuilders(true))
	wantSmoke := []string{
		"compat_method1.arj",
		"compat_longname_comment.arj",
		"compat_chapter_comments.arj",
	}
	if got, want := strings.Join(smoke, ","), strings.Join(wantSmoke, ","); got != want {
		t.Fatalf("smoke fixtures = %q, want %q", got, want)
	}
}

func TestRunRegenSmokeCreatesSelectedFixtures(t *testing.T) {
	tmp := t.TempDir()
	logPath := filepath.Join(tmp, "arj.log")
	t.Setenv("ARJ_STUB_LOG", logPath)
	arjPath := helperARJPathForMode(t, "arj-stub")

	outDir := filepath.Join(tmp, "out")
	if err := runRegen([]string{"-smoke", "-out", outDir, "-arj", arjPath, "-arj-timeout", "5s"}); err != nil {
		t.Fatalf("runRegen smoke: %v", err)
	}

	assertDirEntriesExact(t, outDir, []string{
		"compat_chapter_comments.arj",
		"compat_longname_comment.arj",
		"compat_method1.arj",
	})

	logLines := readLogLines(t, logPath)
	if got, want := len(logLines), 8; got != want {
		t.Fatalf("stub command count = %d, want %d", got, want)
	}
	assertLoggedCommandArgsExact(t, logLines, [][]string{
		{"a", "-y", "-i", "-m1", "compat_method1.arj", "payload.bin"},
		{"a", "-y", "-i", "-m1", "compat_longname_comment.arj", compatLongName},
		{"c", "-y", "-i", "compat_longname_comment.arj", compatLongName, "-zarchive.cmt", "-jzfile.cmt"},
		{"a", "-y", "-i", "-m0", "compat_chapter_comments.arj", "alpha.txt", "beta.txt"},
		{"c", "-y", "-i", "compat_chapter_comments.arj", "-zarchive.cmt"},
		{"c", "-y", "-i", "compat_chapter_comments.arj", "alpha.txt", "-jzalpha.cmt"},
		{"c", "-y", "-i", "compat_chapter_comments.arj", "beta.txt", "-jzbeta.cmt"},
		{"cc", "-y", "-i", "compat_chapter_comments.arj"},
	})

	methodRecords := readStubArchiveRecords(t, filepath.Join(outDir, "compat_method1.arj"))
	assertStubArchiveRecordCommands(t, methodRecords, []string{"a"})
	assertStubArchiveIncludesInputSnapshot(t, methodRecords, "payload.bin", compatibilityFixturePayload())

	longNameRecords := readStubArchiveRecords(t, filepath.Join(outDir, "compat_longname_comment.arj"))
	assertStubArchiveRecordCommands(t, longNameRecords, []string{"a", "c"})
	assertStubArchiveIncludesInputSnapshot(t, longNameRecords, compatLongName, []byte("long filename payload for compatibility fixture\n"))
	assertStubArchiveIncludesInputSnapshot(t, longNameRecords, "archive.cmt", []byte("archive comment for long filename fixture"))
	assertStubArchiveIncludesInputSnapshot(t, longNameRecords, "file.cmt", []byte("file comment for long filename fixture"))

	chapterRecords := readStubArchiveRecords(t, filepath.Join(outDir, "compat_chapter_comments.arj"))
	assertStubArchiveRecordCommands(t, chapterRecords, []string{"a", "c", "c", "c", "cc"})
	assertStubArchiveIncludesInputSnapshot(t, chapterRecords, "alpha.txt", []byte("alpha payload chapter fixture\n"))
	assertStubArchiveIncludesInputSnapshot(t, chapterRecords, "beta.txt", []byte("beta payload chapter fixture\n"))
	assertStubArchiveIncludesInputSnapshot(t, chapterRecords, "archive.cmt", []byte("archive-level-comment"))
	assertStubArchiveIncludesInputSnapshot(t, chapterRecords, "alpha.cmt", []byte("alpha-file-comment"))
	assertStubArchiveIncludesInputSnapshot(t, chapterRecords, "beta.cmt", []byte("beta-file-comment"))
}

func TestRunCheckSmokeUsesCheckHooks(t *testing.T) {
	arjPath := helperARJPathForMode(t, "arj-stub")

	origARJToGo := checkARJToGoFixturesFn
	origGoToARJ := checkGoToARJFn
	defer func() {
		checkARJToGoFixturesFn = origARJToGo
		checkGoToARJFn = origGoToARJ
	}()

	var arjToGoCalled, goToARJCalled int
	checkARJToGoFixturesFn = func(dir string, smoke bool) error {
		arjToGoCalled++
		if !smoke {
			return fmt.Errorf("expected smoke mode")
		}
		assertDirEntriesExact(t, dir, []string{
			"compat_chapter_comments.arj",
			"compat_longname_comment.arj",
			"compat_method1.arj",
		})
		methodRecords := readStubArchiveRecords(t, filepath.Join(dir, "compat_method1.arj"))
		assertStubArchiveIncludesInputSnapshot(t, methodRecords, "payload.bin", compatibilityFixturePayload())
		longNameRecords := readStubArchiveRecords(t, filepath.Join(dir, "compat_longname_comment.arj"))
		assertStubArchiveIncludesInputSnapshot(t, longNameRecords, "archive.cmt", []byte("archive comment for long filename fixture"))
		chapterRecords := readStubArchiveRecords(t, filepath.Join(dir, "compat_chapter_comments.arj"))
		assertStubArchiveIncludesInputSnapshot(t, chapterRecords, "beta.cmt", []byte("beta-file-comment"))
		return nil
	}
	checkGoToARJFn = func(_ *compatTool, dir string, smoke bool) error {
		goToARJCalled++
		if !smoke {
			return fmt.Errorf("expected smoke mode")
		}
		if got, want := filepath.Base(dir), "go-to-arj"; got != want {
			return fmt.Errorf("go->arj dir base = %q, want %q", got, want)
		}
		return nil
	}

	if err := runCheck([]string{"-smoke", "-arj", arjPath, "-arj-timeout", "5s"}); err != nil {
		t.Fatalf("runCheck smoke: %v", err)
	}
	if got, want := arjToGoCalled, 1; got != want {
		t.Fatalf("arj->go hook calls = %d, want %d", got, want)
	}
	if got, want := goToARJCalled, 1; got != want {
		t.Fatalf("go->arj hook calls = %d, want %d", got, want)
	}
}

func TestRunAllSmokeCreatesFixturesAndUsesCheckHooks(t *testing.T) {
	arjPath := helperARJPathForMode(t, "arj-stub")
	tmp := t.TempDir()

	outDir := filepath.Join(tmp, "fixtures")

	origARJToGo := checkARJToGoFixturesFn
	origGoToARJ := checkGoToARJFn
	defer func() {
		checkARJToGoFixturesFn = origARJToGo
		checkGoToARJFn = origGoToARJ
	}()

	var arjToGoCalled, goToARJCalled int
	checkARJToGoFixturesFn = func(dir string, smoke bool) error {
		arjToGoCalled++
		if !smoke {
			return fmt.Errorf("expected smoke mode")
		}
		if got, want := filepath.Base(dir), "arj-to-go"; got != want {
			return fmt.Errorf("arj->go dir base = %q, want %q", got, want)
		}
		methodRecords := readStubArchiveRecords(t, filepath.Join(dir, "compat_method1.arj"))
		assertStubArchiveIncludesInputSnapshot(t, methodRecords, "payload.bin", compatibilityFixturePayload())
		return nil
	}
	checkGoToARJFn = func(_ *compatTool, dir string, smoke bool) error {
		goToARJCalled++
		if !smoke {
			return fmt.Errorf("expected smoke mode")
		}
		if got, want := filepath.Base(dir), "go-to-arj"; got != want {
			return fmt.Errorf("go->arj dir base = %q, want %q", got, want)
		}
		return nil
	}

	if err := runAll([]string{"-smoke", "-out", outDir, "-arj", arjPath, "-arj-timeout", "5s"}); err != nil {
		t.Fatalf("runAll smoke: %v", err)
	}
	assertDirEntriesExact(t, outDir, []string{
		"compat_chapter_comments.arj",
		"compat_longname_comment.arj",
		"compat_method1.arj",
	})
	if got, want := arjToGoCalled, 1; got != want {
		t.Fatalf("arj->go hook calls = %d, want %d", got, want)
	}
	if got, want := goToARJCalled, 1; got != want {
		t.Fatalf("go->arj hook calls = %d, want %d", got, want)
	}
}

func TestCheckGoToARJSmokeWithStubExtractor(t *testing.T) {
	tmp := t.TempDir()
	logPath := filepath.Join(tmp, "arj.log")
	t.Setenv("ARJ_STUB_LOG", logPath)
	arjPath := helperARJPathForMode(t, "arj-stub")

	tool := &compatTool{
		arjPath:    arjPath,
		arjTimeout: 5 * time.Second,
		env:        deterministicARJEnv(),
	}

	baseDir := filepath.Join(tmp, "go-to-arj")
	if err := tool.checkGoToARJ(baseDir, true); err != nil {
		t.Fatalf("checkGoToARJ smoke: %v", err)
	}

	for _, method := range []uint16{0, 4} {
		path := filepath.Join(baseDir, fmt.Sprintf("method-%d", method), "extract", fmt.Sprintf("payload-method-%d.bin", method))
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile(%s): %v", path, err)
		}
		want := interopPayload(method)
		if !bytes.Equal(got, want) {
			t.Fatalf("payload mismatch for method %d", method)
		}
	}

	logLines := readLogLines(t, logPath)
	if got, want := len(logLines), 2; got != want {
		t.Fatalf("extract command count = %d, want %d", got, want)
	}
	assertLoggedCommandArgsExact(t, logLines, [][]string{
		{"x", "-y", "-i", "go-produced-method-0.arj", "-ht" + filepath.Join(baseDir, "method-0", "extract")},
		{"x", "-y", "-i", "go-produced-method-4.arj", "-ht" + filepath.Join(baseDir, "method-4", "extract")},
	})
}

func TestCheckChapterFixtureArchiveAcceptsMetadataVariants(t *testing.T) {
	path := filepath.Join(t.TempDir(), "variant-chapter-comments.arj")
	if err := writeChapterFixtureVariant(path, 7, "<<<007>>>"); err != nil {
		t.Fatalf("writeChapterFixtureVariant: %v", err)
	}

	if err := checkChapterFixtureArchive(path); err != nil {
		t.Fatalf("checkChapterFixtureArchive variant: %v", err)
	}
}

func TestCheckChapterFixtureArchiveRejectsUnexpectedEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "variant-chapter-comments.arj")
	if err := writeChapterFixtureVariant(path, 3, "chapter-marker"); err != nil {
		t.Fatalf("writeChapterFixtureVariant: %v", err)
	}

	err := checkChapterFixtureArchive(path)
	if err == nil {
		t.Fatalf("checkChapterFixtureArchive unexpected entry: expected error")
	}
	if !strings.Contains(err.Error(), "unexpected entry") {
		t.Fatalf("checkChapterFixtureArchive unexpected entry error = %q, want substring %q", err, "unexpected entry")
	}
}

func TestIsChapterMarkerName(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{name: "<<<1>>>", want: true},
		{name: "<<<001>>>", want: true},
		{name: "<<<01a>>>", want: false},
		{name: "chapter-marker", want: false},
		{name: "<<<>>>", want: false},
	}

	for _, tc := range tests {
		if got := isChapterMarkerName(tc.name); got != tc.want {
			t.Fatalf("isChapterMarkerName(%q) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestRunModesRejectPositionalArgs(t *testing.T) {
	if err := runRegen([]string{"extra"}); err == nil {
		t.Fatalf("runRegen positional args: expected error")
	}
	if err := runCheck([]string{"extra"}); err == nil {
		t.Fatalf("runCheck positional args: expected error")
	}
	if err := runAll([]string{"extra"}); err == nil {
		t.Fatalf("runAll positional args: expected error")
	}
}

func TestRunModesRejectNonPositiveARJTimeout(t *testing.T) {
	tests := []struct {
		name string
		run  func([]string) error
	}{
		{name: "regen", run: runRegen},
		{name: "check", run: runCheck},
		{name: "all", run: runAll},
	}
	for _, tc := range tests {
		for _, timeout := range []string{"0s", "-1s"} {
			t.Run(tc.name+"/"+timeout, func(t *testing.T) {
				err := tc.run([]string{"-arj-timeout", timeout})
				if err == nil {
					t.Fatalf("%s with -arj-timeout=%s: expected error", tc.name, timeout)
				}
				if !strings.Contains(err.Error(), "invalid arj timeout") {
					t.Fatalf("%s timeout error = %q, want substring %q", tc.name, err, "invalid arj timeout")
				}
			})
		}
	}
}

func TestNewCompatToolRejectsNonPositiveTimeout(t *testing.T) {
	arjPath := helperARJPathForMode(t, "noop")
	for _, timeout := range []time.Duration{0, -1 * time.Second} {
		_, err := newCompatTool(arjPath, timeout)
		if err == nil {
			t.Fatalf("newCompatTool timeout %s: expected error", timeout)
		}
		if !strings.Contains(err.Error(), "invalid arj timeout") {
			t.Fatalf("newCompatTool timeout error = %q, want substring %q", err, "invalid arj timeout")
		}
	}
}

func helperARJBinaryPath(t *testing.T) string {
	t.Helper()
	path, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	return path
}

func helperARJPathForMode(t *testing.T, mode string) string {
	t.Helper()
	t.Setenv(compatARJHelperModeEnv, mode)
	return helperARJBinaryPath(t)
}

func fixtureNames(builders []fixtureBuilder) []string {
	out := make([]string, 0, len(builders))
	for _, b := range builders {
		out = append(out, b.name)
	}
	return out
}

func containsEnvEntry(env []string, want string) bool {
	for _, kv := range env {
		if kv == want {
			return true
		}
	}
	return false
}

func assertDirEntriesExact(t *testing.T, dir string, want []string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", dir, err)
	}
	got := make([]string, 0, len(entries))
	for _, entry := range entries {
		got = append(got, entry.Name())
	}
	gotJoined := strings.Join(got, ",")
	wantJoined := strings.Join(want, ",")
	if gotJoined != wantJoined {
		t.Fatalf("dir %s entries = %q, want %q", dir, gotJoined, wantJoined)
	}
}

func assertLoggedCommandArgsExact(t *testing.T, logLines []string, want [][]string) {
	t.Helper()
	got := make([][]string, 0, len(logLines))
	for i, line := range logLines {
		var record helperLogRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("log line %d malformed json = %q (err=%v)", i, line, err)
		}
		if strings.TrimSpace(record.WorkingDir) == "" {
			t.Fatalf("log line %d missing cwd = %q", i, line)
		}
		got = append(got, record.Args)
	}

	if gotLen, wantLen := len(got), len(want); gotLen != wantLen {
		t.Fatalf("logged command count = %d, want %d", gotLen, wantLen)
	}
	for i := range got {
		if strings.Join(got[i], "\x00") != strings.Join(want[i], "\x00") {
			t.Fatalf("logged command args[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func readLogLines(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", path, err)
	}
	raw := strings.TrimSpace(string(data))
	if raw == "" {
		return nil
	}
	return strings.Split(raw, "\n")
}

func readStubArchiveRecords(t *testing.T, path string) []stubArchiveRecord {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(%s): %v", path, err)
	}
	if fi.Size() == 0 {
		t.Fatalf("stub archive %s is empty", path)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", path, err)
	}
	raw := strings.TrimSpace(string(data))
	if raw == "" {
		t.Fatalf("stub archive %s content is empty", path)
	}

	lines := strings.Split(raw, "\n")
	records := make([]stubArchiveRecord, 0, len(lines))
	for i, line := range lines {
		var record stubArchiveRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("stub archive %s line %d malformed json = %q (err=%v)", path, i, line, err)
		}
		records = append(records, record)
	}
	return records
}

func assertStubArchiveRecordCommands(t *testing.T, records []stubArchiveRecord, want []string) {
	t.Helper()
	if got, wantLen := len(records), len(want); got != wantLen {
		t.Fatalf("stub archive command count = %d, want %d", got, wantLen)
	}
	for i := range want {
		if got, wantCommand := records[i].Command, want[i]; got != wantCommand {
			t.Fatalf("stub archive command[%d] = %q, want %q", i, got, wantCommand)
		}
	}
}

func assertStubArchiveIncludesInputSnapshot(t *testing.T, records []stubArchiveRecord, arg string, payload []byte) {
	t.Helper()
	wantHash := stubInputSHA256Hex(payload)
	for _, record := range records {
		for _, input := range record.Inputs {
			if input.Arg != arg {
				continue
			}
			if got, want := input.Size, len(payload); got != want {
				t.Fatalf("snapshot %q size = %d, want %d", arg, got, want)
			}
			if got := input.SHA256; got != wantHash {
				t.Fatalf("snapshot %q sha256 = %q, want %q", arg, got, wantHash)
			}
			return
		}
	}
	t.Fatalf("missing input snapshot for %q", arg)
}

func stubInputSHA256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum)
}

func writeChapterFixtureVariant(path string, chapterNumber uint8, markerName string) error {
	var archive bytes.Buffer
	w := goarj.NewWriter(&archive)
	if err := w.SetArchiveHeader(&goarj.ArchiveHeader{
		Name:          filepath.Base(path),
		Comment:       "archive-level-comment",
		Flags:         0x00,
		ExtFlags:      0x02,
		ChapterNumber: chapterNumber,
	}); err != nil {
		return err
	}

	files := []struct {
		name    string
		comment string
		payload []byte
	}{
		{
			name:    "alpha.txt",
			comment: "alpha-file-comment",
			payload: []byte("alpha payload chapter fixture\n"),
		},
		{
			name:    "beta.txt",
			comment: "beta-file-comment",
			payload: []byte("beta payload chapter fixture\n"),
		},
	}

	for _, file := range files {
		fw, err := w.CreateHeader(&goarj.FileHeader{
			Name:          file.name,
			Comment:       file.comment,
			Method:        goarj.Store,
			Flags:         0x00,
			ExtFlags:      0x05,
			ChapterNumber: chapterNumber,
		})
		if err != nil {
			return err
		}
		if _, err := fw.Write(file.payload); err != nil {
			return err
		}
	}

	if _, err := w.CreateHeader(&goarj.FileHeader{
		Name:          markerName,
		Method:        goarj.Store,
		Flags:         0x00,
		ExtFlags:      0x05,
		ChapterNumber: chapterNumber,
	}); err != nil {
		return err
	}

	if err := w.Close(); err != nil {
		return err
	}
	return os.WriteFile(path, archive.Bytes(), 0o600)
}
