package arj

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestInteropARJBinaryExtractsMethods0To4(t *testing.T) {
	arjPath := requireInteropARJBinary(t)

	for _, method := range []uint16{Store, Method1, Method2, Method3, Method4} {
		method := method
		t.Run(fmt.Sprintf("method-%d", method), func(t *testing.T) {
			fileName := fmt.Sprintf("payload-method-%d.bin", method)
			payload := interopARJPayload(method)
			archiveBytes := mustBuildInteropArchive(t, fileName, method, payload)

			tmpDir := t.TempDir()
			archivePath := filepath.Join(tmpDir, fmt.Sprintf("interop-method-%d.arj", method))
			if err := os.WriteFile(archivePath, archiveBytes, 0o600); err != nil {
				t.Fatalf("WriteFile(%s): %v", archivePath, err)
			}

			extractDir := filepath.Join(tmpDir, "extract")
			if err := os.Mkdir(extractDir, 0o755); err != nil {
				t.Fatalf("Mkdir(%s): %v", extractDir, err)
			}

			runInteropARJCommand(t, arjPath, "x", "-y", "-i", archivePath, "-ht"+extractDir)

			entries, err := os.ReadDir(extractDir)
			if err != nil {
				t.Fatalf("ReadDir(%s): %v", extractDir, err)
			}
			if got, want := len(entries), 1; got != want {
				t.Fatalf("extracted entry count = %d, want %d", got, want)
			}
			if got, want := entries[0].Name(), fileName; got != want {
				t.Fatalf("extracted file name = %q, want %q", got, want)
			}
			if entries[0].IsDir() {
				t.Fatalf("extracted entry %q is a directory, want file", entries[0].Name())
			}

			extractedPath := filepath.Join(extractDir, fileName)
			gotPayload, err := os.ReadFile(extractedPath)
			if err != nil {
				t.Fatalf("ReadFile(%s): %v", extractedPath, err)
			}
			if !bytes.Equal(gotPayload, payload) {
				t.Fatalf("payload mismatch after arj extraction for method %d", method)
			}
		})
	}
}

func requireInteropARJBinary(t *testing.T) string {
	t.Helper()

	arjPath, err := resolveInteropARJPath("arj")
	if err == nil {
		return filepath.Clean(arjPath)
	}

	if interopStrictMode() {
		t.Fatalf("arj binary not found in PATH in strict interop mode: %v", err)
	}
	t.Skipf("arj binary not found in PATH: %v", err)
	return ""
}

func resolveInteropARJPath(requested string) (string, error) {
	path, err := exec.LookPath(requested)
	if err != nil {
		return "", err
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	cleaned := filepath.Clean(absPath)
	if err := validateInteropARJBinary(cleaned); err != nil {
		return "", err
	}
	return cleaned, nil
}

func validateInteropARJBinary(path string) error {
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

func interopStrictMode() bool {
	raw, ok := os.LookupEnv("GOARJ_REQUIRE_INTEROP")
	if !ok {
		return false
	}
	v := strings.TrimSpace(strings.ToLower(raw))
	return v != "" && v != "0" && v != "false" && v != "no"
}

func TestInteropStrictModeFromExplicitEnv(t *testing.T) {
	t.Setenv("CI", "")
	t.Setenv("GOARJ_REQUIRE_INTEROP", "true")
	if !interopStrictMode() {
		t.Fatalf("interopStrictMode = false, want true for GOARJ_REQUIRE_INTEROP=true")
	}

	t.Setenv("GOARJ_REQUIRE_INTEROP", "0")
	if interopStrictMode() {
		t.Fatalf("interopStrictMode = true, want false for GOARJ_REQUIRE_INTEROP=0")
	}
}

func TestInteropStrictModeDefaultsDisabledOnCI(t *testing.T) {
	previous, had := os.LookupEnv("GOARJ_REQUIRE_INTEROP")
	if err := os.Unsetenv("GOARJ_REQUIRE_INTEROP"); err != nil {
		t.Fatalf("Unsetenv(GOARJ_REQUIRE_INTEROP): %v", err)
	}
	t.Cleanup(func() {
		if had {
			_ = os.Setenv("GOARJ_REQUIRE_INTEROP", previous)
			return
		}
		_ = os.Unsetenv("GOARJ_REQUIRE_INTEROP")
	})
	t.Setenv("CI", "1")
	if interopStrictMode() {
		t.Fatalf("interopStrictMode = true, want false when GOARJ_REQUIRE_INTEROP is unset")
	}
}

func TestInteropARJEnv(t *testing.T) {
	t.Setenv("ARJ_SW", "custom-options")
	t.Setenv("TZ", "America/New_York")
	t.Setenv("LC_ALL", "en_US.UTF-8")
	t.Setenv("LANG", "en_US.UTF-8")
	t.Setenv("GOARJ_TEST_KEEP", "1")

	env := interopARJEnv()
	if !interopContainsEnvEntry(env, "ARJ_SW=") {
		t.Fatalf("interop env missing ARJ_SW reset")
	}
	if !interopContainsEnvEntry(env, "TZ=UTC") {
		t.Fatalf("interop env missing TZ=UTC")
	}
	if !interopContainsEnvEntry(env, "LC_ALL=C") {
		t.Fatalf("interop env missing LC_ALL=C")
	}
	if !interopContainsEnvEntry(env, "LANG=C") {
		t.Fatalf("interop env missing LANG=C")
	}
	if interopContainsEnvEntry(env, "ARJ_SW=custom-options") {
		t.Fatalf("interop env unexpectedly kept caller ARJ_SW")
	}
	if interopContainsEnvEntry(env, "TZ=America/New_York") {
		t.Fatalf("interop env unexpectedly kept caller TZ")
	}
	if interopContainsEnvEntry(env, "LC_ALL=en_US.UTF-8") {
		t.Fatalf("interop env unexpectedly kept caller LC_ALL")
	}
	if interopContainsEnvEntry(env, "LANG=en_US.UTF-8") {
		t.Fatalf("interop env unexpectedly kept caller LANG")
	}
	if !interopContainsEnvEntry(env, "GOARJ_TEST_KEEP=1") {
		t.Fatalf("interop env dropped unrelated variable")
	}
}

func TestInteropARJEnvFiltersMixedCaseControlVars(t *testing.T) {
	t.Setenv("aRj_sW", "mixed-options")
	t.Setenv("tZ", "Europe/Paris")
	t.Setenv("Lc_All", "fr_FR.UTF-8")
	t.Setenv("lAnG", "fr_FR.UTF-8")

	env := interopARJEnv()
	if interopContainsEnvEntry(env, "aRj_sW=mixed-options") {
		t.Fatalf("interop env unexpectedly kept mixed-case ARJ_SW")
	}
	if interopContainsEnvEntry(env, "tZ=Europe/Paris") {
		t.Fatalf("interop env unexpectedly kept mixed-case TZ")
	}
	if interopContainsEnvEntry(env, "Lc_All=fr_FR.UTF-8") {
		t.Fatalf("interop env unexpectedly kept mixed-case LC_ALL")
	}
	if interopContainsEnvEntry(env, "lAnG=fr_FR.UTF-8") {
		t.Fatalf("interop env unexpectedly kept mixed-case LANG")
	}
	if !interopContainsEnvEntry(env, "ARJ_SW=") {
		t.Fatalf("interop env missing ARJ_SW reset")
	}
	if !interopContainsEnvEntry(env, "TZ=UTC") {
		t.Fatalf("interop env missing TZ=UTC")
	}
	if !interopContainsEnvEntry(env, "LC_ALL=C") {
		t.Fatalf("interop env missing LC_ALL=C")
	}
	if !interopContainsEnvEntry(env, "LANG=C") {
		t.Fatalf("interop env missing LANG=C")
	}
}

func mustBuildInteropArchive(t *testing.T, fileName string, method uint16, payload []byte) []byte {
	t.Helper()

	var archive bytes.Buffer
	w := NewWriter(&archive)
	fw, err := w.CreateHeader(&FileHeader{Name: fileName, Method: method})
	if err != nil {
		t.Fatalf("CreateHeader(method=%d): %v", method, err)
	}
	if _, err := fw.Write(payload); err != nil {
		t.Fatalf("Write(method=%d): %v", method, err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close(method=%d): %v", method, err)
	}
	return archive.Bytes()
}

func interopARJPayload(method uint16) []byte {
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

func interopARJEnv() []string {
	env := os.Environ()
	out := make([]string, 0, len(env)+4)
	for _, kv := range env {
		if shouldFilterInteropEnvKey(kv) {
			continue
		}
		out = append(out, kv)
	}
	// Keep ARJ invocation deterministic regardless of user/global defaults.
	out = append(out, "ARJ_SW=", "TZ=UTC", "LC_ALL=C", "LANG=C")
	return out
}

func shouldFilterInteropEnvKey(kv string) bool {
	key := kv
	if i := strings.IndexByte(kv, '='); i >= 0 {
		key = kv[:i]
	}
	return strings.EqualFold(key, "ARJ_SW") ||
		strings.EqualFold(key, "TZ") ||
		strings.EqualFold(key, "LC_ALL") ||
		strings.EqualFold(key, "LANG")
}

func interopContainsEnvEntry(env []string, want string) bool {
	for _, kv := range env {
		if kv == want {
			return true
		}
	}
	return false
}
