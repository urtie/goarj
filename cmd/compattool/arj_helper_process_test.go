package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

const compatARJHelperModeEnv = "GOARJ_COMPAT_ARJ_HELPER_MODE"

var methodArchivePattern = regexp.MustCompile(`method-([0-9]+)\.arj$`)

func TestMain(m *testing.M) {
	mode := strings.TrimSpace(os.Getenv(compatARJHelperModeEnv))
	if mode != "" {
		os.Exit(runCompatARJHelper(mode, os.Args[1:]))
	}
	os.Exit(m.Run())
}

func runCompatARJHelper(mode string, args []string) int {
	if err := appendHelperLog(args); err != nil {
		fmt.Fprintf(os.Stderr, "stub log: %v\n", err)
		return 2
	}

	switch mode {
	case "noop":
		return 0
	case "sleep":
		time.Sleep(2 * time.Second)
		return 0
	case "flood":
		chunk := strings.Repeat("f", 1<<12)
		for i := 0; i < 1024; i++ {
			fmt.Fprint(os.Stderr, chunk)
		}
		return 2
	case "arj-stub":
		return runCompatARJStub(args)
	default:
		fmt.Fprintf(os.Stderr, "unknown helper mode %q\n", mode)
		return 2
	}
}

func appendHelperLog(args []string) error {
	logPath := strings.TrimSpace(os.Getenv("ARJ_STUB_LOG"))
	if logPath == "" {
		return nil
	}
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	lineBytes, err := json.Marshal(helperLogRecord{
		WorkingDir: wd,
		Args:       append([]string(nil), args...),
	})
	if err != nil {
		return err
	}
	lineBytes = append(lineBytes, '\n')
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(lineBytes)
	return err
}

func runCompatARJStub(args []string) int {
	if len(args) == 0 {
		return 2
	}

	command := args[0]
	rest := args[1:]
	switch command {
	case "x":
		return runCompatARJExtractStub(rest)
	case "a", "c", "cc":
		return runCompatARJCreateStub(command, rest)
	default:
		fmt.Fprintf(os.Stderr, "stub arj: unsupported command %q\n", command)
		return 2
	}
}

func runCompatARJExtractStub(args []string) int {
	archive := ""
	extractDir := ""
	for _, arg := range args {
		switch {
		case strings.HasPrefix(arg, "-ht"):
			extractDir = strings.TrimPrefix(arg, "-ht")
		case strings.HasPrefix(arg, "-"):
		case archive == "":
			archive = arg
		}
	}
	if archive == "" || extractDir == "" {
		fmt.Fprintln(os.Stderr, "stub arj x: missing archive/extract")
		return 2
	}

	base := filepath.Base(archive)
	match := methodArchivePattern.FindStringSubmatch(base)
	if len(match) != 2 {
		fmt.Fprintf(os.Stderr, "stub arj x: cannot parse method from %s\n", archive)
		return 2
	}
	method, err := strconv.Atoi(match[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "stub arj x: parse method %q: %v\n", match[1], err)
		return 2
	}

	if err := os.MkdirAll(extractDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "stub arj x: mkdir %s: %v\n", extractDir, err)
		return 2
	}

	outPath := filepath.Join(extractDir, fmt.Sprintf("payload-method-%d.bin", method))
	if err := os.WriteFile(outPath, interopPayload(uint16(method)), 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "stub arj x: write %s: %v\n", outPath, err)
		return 2
	}
	return 0
}

func runCompatARJCreateStub(command string, args []string) int {
	positionals := make([]string, 0, len(args))
	for _, arg := range args {
		if strings.HasPrefix(arg, "-") {
			continue
		}
		positionals = append(positionals, arg)
	}
	switch command {
	case "a":
		if len(positionals) < 2 {
			fmt.Fprintln(os.Stderr, "stub arj a: missing archive/file arguments")
			return 2
		}
	case "c":
		if len(positionals) < 1 || len(positionals) > 2 {
			fmt.Fprintln(os.Stderr, "stub arj c: invalid positional arguments")
			return 2
		}
	case "cc":
		if len(positionals) != 1 {
			fmt.Fprintln(os.Stderr, "stub arj cc: invalid positional arguments")
			return 2
		}
	}

	archive := positionals[0]
	if strings.TrimSpace(archive) == "" {
		fmt.Fprintln(os.Stderr, "stub arj: missing archive argument")
		return 2
	}
	inputArgs, err := parseStubInputArgs(command, args, positionals)
	if err != nil {
		fmt.Fprintf(os.Stderr, "stub arj: parse input args: %v\n", err)
		return 2
	}
	inputs, err := snapshotStubInputs(inputArgs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "stub arj: snapshot inputs: %v\n", err)
		return 2
	}

	recordBytes, err := json.Marshal(stubArchiveRecord{
		Command: command,
		Args:    append([]string(nil), args...),
		Inputs:  inputs,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "stub arj: marshal record: %v\n", err)
		return 2
	}
	recordBytes = append(recordBytes, '\n')

	if err := os.MkdirAll(filepath.Dir(archive), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "stub arj: create parent dir for %s: %v\n", archive, err)
		return 2
	}
	f, err := os.OpenFile(archive, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "stub arj: create %s: %v\n", archive, err)
		return 2
	}
	defer f.Close()
	if _, err := f.Write(recordBytes); err != nil {
		fmt.Fprintf(os.Stderr, "stub arj: write %s: %v\n", archive, err)
		return 2
	}
	return 0
}

type helperLogRecord struct {
	WorkingDir string   `json:"wd"`
	Args       []string `json:"args"`
}

type stubArchiveRecord struct {
	Command string              `json:"command"`
	Args    []string            `json:"args"`
	Inputs  []stubInputSnapshot `json:"inputs,omitempty"`
}

type stubInputSnapshot struct {
	Arg    string `json:"arg"`
	Size   int    `json:"size"`
	SHA256 string `json:"sha256"`
}

func parseStubInputArgs(command string, args, positionals []string) ([]string, error) {
	out := make([]string, 0, len(args))
	seen := make(map[string]bool, len(args))
	addUnique := func(path string) {
		if strings.TrimSpace(path) == "" || seen[path] {
			return
		}
		seen[path] = true
		out = append(out, path)
	}

	switch command {
	case "a":
		for _, p := range positionals[1:] {
			addUnique(p)
		}
	case "c":
		if len(positionals) == 2 {
			addUnique(positionals[1])
		}
	}

	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "-z":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("missing value for -z")
			}
			addUnique(args[i+1])
			i++
		case arg == "-jz":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("missing value for -jz")
			}
			addUnique(args[i+1])
			i++
		case strings.HasPrefix(arg, "-z") && len(arg) > len("-z"):
			addUnique(strings.TrimPrefix(arg, "-z"))
		case strings.HasPrefix(arg, "-jz") && len(arg) > len("-jz"):
			addUnique(strings.TrimPrefix(arg, "-jz"))
		}
	}
	return out, nil
}

func snapshotStubInputs(paths []string) ([]stubInputSnapshot, error) {
	out := make([]stubInputSnapshot, 0, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", path, err)
		}
		sum := sha256.Sum256(data)
		out = append(out, stubInputSnapshot{
			Arg:    path,
			Size:   len(data),
			SHA256: fmt.Sprintf("%x", sum),
		})
	}
	return out, nil
}

func TestRunCompatARJStubRejectsUnknownCommand(t *testing.T) {
	if got := runCompatARJStub([]string{"invalid"}); got == 0 {
		t.Fatalf("runCompatARJStub(unknown) exit = %d, want non-zero", got)
	}
}

func TestRunCompatARJStubRejectsInvalidCommandLayouts(t *testing.T) {
	tests := [][]string{
		{"a", "-y", "-i", "only-archive.arj"},
		{"c", "-y", "-i"},
		{"cc", "-y", "-i", "archive.arj", "extra"},
	}
	for _, args := range tests {
		if got := runCompatARJStub(args); got == 0 {
			t.Fatalf("runCompatARJStub(%v) exit = %d, want non-zero", args, got)
		}
	}
}
