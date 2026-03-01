package arj

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultExtractMaxFiles is the default regular-file count limit used by
	// ExtractAll.
	DefaultExtractMaxFiles = 4096
	// DefaultExtractMaxTotalBytes is the default aggregate regular-file size
	// limit used by ExtractAll.
	DefaultExtractMaxTotalBytes int64 = 1 << 30
	// DefaultExtractMaxFileBytes is the default per-file size limit used by
	// ExtractAll.
	DefaultExtractMaxFileBytes int64 = 256 << 20

	// ExtractUnlimitedFiles disables the MaxFiles quota when used in
	// ExtractOptions.
	ExtractUnlimitedFiles = -1
	// ExtractUnlimitedBytes disables byte quotas when used in ExtractOptions.
	ExtractUnlimitedBytes int64 = -1

	extractCopyBufferSize = 256 << 10
)

type extractedDir struct {
	path    string
	name    string
	mode    fs.FileMode
	modTime time.Time
}

// ExtractOptions controls resource limits and safety mode for archive
// extraction.
//
// Zero quota values fall back to package defaults. Use ExtractUnlimitedFiles
// and ExtractUnlimitedBytes for explicit unlimited quotas.
type ExtractOptions struct {
	// MaxFiles caps extracted regular files. Directories do not count.
	MaxFiles int
	// MaxTotalBytes caps total extracted regular-file bytes.
	MaxTotalBytes int64
	// MaxFileBytes caps bytes extracted for any single regular file.
	MaxFileBytes int64
	// Strict enables fail-closed extraction semantics. On platforms where
	// strict extraction guarantees are unavailable, extraction fails with
	// ErrStrictModeUnsupported.
	Strict bool
}

type extractQuota struct {
	opts       ExtractOptions
	fileCount  int
	totalBytes int64
}

type extractQuotaWriter struct {
	dst       io.Writer
	quota     *extractQuota
	fileBytes int64
}

// extractTestHookBeforeCreate is used by tests to trigger deterministic
// symlink-swap races around file creation.
var extractTestHookBeforeCreate func(name string)

// extractTestHookBeforeCommit is used by tests to inspect the staged temp file
// right before it is committed via rename, and to force deterministic
// pre-commit failures.
var extractTestHookBeforeCommit func(name string, staged *os.File) error

// extractTestHookBeforeNonUnixFallbackCommit is used by non-unix tests to
// force deterministic failures after destination backup but before staged
// replacement commit.
var extractTestHookBeforeNonUnixFallbackCommit func(name, destination, stagedPath, backupPath string) error

// extractTestHookAfterNonUnixCommitRename is used by non-unix tests to force
// deterministic races after staged rename and before post-commit verification.
var extractTestHookAfterNonUnixCommitRename func(name, destination string) error

// extractTestHookPathMetadataApply is used by tests to detect calls into
// path-based metadata application helpers.
var extractTestHookPathMetadataApply func(path, entryName string)

// extractTestHookBeforePathMetadataTimestamp is used by tests to force
// deterministic races right before path-based timestamp application.
var extractTestHookBeforePathMetadataTimestamp func(path, entryName string) error

// extractTestHookBeforeTempPathMetadataTimestamp is used by tests to force
// deterministic races right before temp-path timestamp application.
var extractTestHookBeforeTempPathMetadataTimestamp func(path, entryName string, staged *os.File) error

// extractTestHookBeforePathMetadataTimestampCall is used by tests to force
// deterministic races after the final pre-Chtimes identity recheck.
var extractTestHookBeforePathMetadataTimestampCall func(path, entryName string) error

// extractTestHookAfterPathMetadataTimestampCall is used by tests to force
// deterministic races immediately after path-based Chtimes.
var extractTestHookAfterPathMetadataTimestampCall func(path, entryName string) error

// extractTestHookBeforeTempPathMetadataTimestampCall is used by tests to force
// deterministic races after the final temp-path pre-Chtimes identity recheck.
var extractTestHookBeforeTempPathMetadataTimestampCall func(path, entryName string, staged *os.File) error

// extractTestHookAfterTempPathMetadataTimestampCall is used by tests to force
// deterministic races immediately after temp-path Chtimes.
var extractTestHookAfterTempPathMetadataTimestampCall func(path, entryName string, staged *os.File) error

var extractCopyBufferPool = sync.Pool{
	New: func() any {
		return make([]byte, extractCopyBufferSize)
	},
}

// ExtractAll extracts all archive entries under dir.
//
// It applies default resource limits and best-effort path safety checks.
// For fail-closed extraction semantics, call ExtractAllWithOptions with
// StrictExtractOptions.
func (r *Reader) ExtractAll(dir string) error {
	return r.ExtractAllWithOptions(dir, defaultExtractOptions())
}

// ExtractAllWithOptions extracts all archive entries under dir with
// configurable limits.
func (r *Reader) ExtractAllWithOptions(dir string, opts ExtractOptions) error {
	if err := validateExtractOptions(opts); err != nil {
		return err
	}
	if err := validateExtractArchiveSecurityModes(r); err != nil {
		return err
	}
	return r.extractAllWithOptions(dir, normalizeExtractOptions(opts))
}

// ExtractAllStream extracts all entries from an ARJ stream under dir.
//
// It applies default resource limits and best-effort path safety checks.
// For fail-closed extraction semantics, call ExtractAllStreamWithOptions with
// StrictExtractOptions.
func ExtractAllStream(r io.Reader, dir string) error {
	return ExtractAllStreamWithOptions(r, dir, defaultExtractOptions())
}

// ExtractAllStreamWithOptions extracts all entries from an ARJ stream under
// dir with configurable limits.
func ExtractAllStreamWithOptions(r io.Reader, dir string, opts ExtractOptions) error {
	sr, err := NewStreamReader(r)
	if err != nil {
		return err
	}
	return sr.ExtractAllWithOptions(dir, opts)
}

// ExtractAll extracts all remaining stream entries under dir.
func (r *StreamReader) ExtractAll(dir string) error {
	return r.ExtractAllWithOptions(dir, defaultExtractOptions())
}

// ExtractAllWithOptions extracts all remaining stream entries under dir with
// configurable limits.
func (r *StreamReader) ExtractAllWithOptions(dir string, opts ExtractOptions) error {
	if err := validateExtractOptions(opts); err != nil {
		return err
	}
	if err := validateExtractStreamArchiveSecurityModes(r); err != nil {
		return err
	}
	return r.extractAllWithOptions(dir, normalizeExtractOptions(opts))
}

func validateExtractArchiveSecurityModes(r *Reader) error {
	if r == nil {
		return ErrFormat
	}
	return unsupportedMainSecurityFlagsError(r.ArchiveHeader.Flags, r.ArchiveHeader.EncryptionVersion())
}

func validateExtractStreamArchiveSecurityModes(r *StreamReader) error {
	if r == nil {
		return ErrFormat
	}
	return unsupportedMainSecurityFlagsError(r.ArchiveHeader.Flags, r.ArchiveHeader.EncryptionVersion())
}

// UnlimitedExtractOptions returns options that disable extraction quotas.
func UnlimitedExtractOptions() ExtractOptions {
	return ExtractOptions{
		MaxFiles:      ExtractUnlimitedFiles,
		MaxTotalBytes: ExtractUnlimitedBytes,
		MaxFileBytes:  ExtractUnlimitedBytes,
	}
}

// StrictExtractOptions returns default extraction limits with strict
// fail-closed behavior enabled.
func StrictExtractOptions() ExtractOptions {
	opts := defaultExtractOptions()
	opts.Strict = true
	return opts
}

func defaultExtractOptions() ExtractOptions {
	return ExtractOptions{
		MaxFiles:      DefaultExtractMaxFiles,
		MaxTotalBytes: DefaultExtractMaxTotalBytes,
		MaxFileBytes:  DefaultExtractMaxFileBytes,
	}
}

func normalizeExtractOptions(opts ExtractOptions) ExtractOptions {
	normalized := opts
	switch normalized.MaxFiles {
	case 0:
		normalized.MaxFiles = DefaultExtractMaxFiles
	case ExtractUnlimitedFiles:
		normalized.MaxFiles = 0
	}
	switch normalized.MaxTotalBytes {
	case 0:
		normalized.MaxTotalBytes = DefaultExtractMaxTotalBytes
	case ExtractUnlimitedBytes:
		normalized.MaxTotalBytes = 0
	}
	switch normalized.MaxFileBytes {
	case 0:
		normalized.MaxFileBytes = DefaultExtractMaxFileBytes
	case ExtractUnlimitedBytes:
		normalized.MaxFileBytes = 0
	}
	return normalized
}

func runExtractTestHookBeforeCreate(name string) {
	if extractTestHookBeforeCreate != nil {
		extractTestHookBeforeCreate(name)
	}
}

func runExtractTestHookBeforeCommit(name string, staged *os.File) error {
	if extractTestHookBeforeCommit != nil {
		return extractTestHookBeforeCommit(name, staged)
	}
	return nil
}

func runExtractTestHookBeforeNonUnixFallbackCommit(name, destination, stagedPath, backupPath string) error {
	if extractTestHookBeforeNonUnixFallbackCommit != nil {
		return extractTestHookBeforeNonUnixFallbackCommit(name, destination, stagedPath, backupPath)
	}
	return nil
}

func runExtractTestHookAfterNonUnixCommitRename(name, destination string) error {
	if extractTestHookAfterNonUnixCommitRename != nil {
		return extractTestHookAfterNonUnixCommitRename(name, destination)
	}
	return nil
}

func runExtractTestHookPathMetadataApply(path, entryName string) {
	if extractTestHookPathMetadataApply != nil {
		extractTestHookPathMetadataApply(path, entryName)
	}
}

func runExtractTestHookBeforePathMetadataTimestamp(path, entryName string) error {
	if extractTestHookBeforePathMetadataTimestamp != nil {
		return extractTestHookBeforePathMetadataTimestamp(path, entryName)
	}
	return nil
}

func runExtractTestHookBeforeTempPathMetadataTimestamp(path, entryName string, staged *os.File) error {
	if extractTestHookBeforeTempPathMetadataTimestamp != nil {
		return extractTestHookBeforeTempPathMetadataTimestamp(path, entryName, staged)
	}
	return nil
}

func runExtractTestHookBeforePathMetadataTimestampCall(path, entryName string) error {
	if extractTestHookBeforePathMetadataTimestampCall != nil {
		return extractTestHookBeforePathMetadataTimestampCall(path, entryName)
	}
	return nil
}

func runExtractTestHookAfterPathMetadataTimestampCall(path, entryName string) error {
	if extractTestHookAfterPathMetadataTimestampCall != nil {
		return extractTestHookAfterPathMetadataTimestampCall(path, entryName)
	}
	return nil
}

func runExtractTestHookBeforeTempPathMetadataTimestampCall(path, entryName string, staged *os.File) error {
	if extractTestHookBeforeTempPathMetadataTimestampCall != nil {
		return extractTestHookBeforeTempPathMetadataTimestampCall(path, entryName, staged)
	}
	return nil
}

func runExtractTestHookAfterTempPathMetadataTimestampCall(path, entryName string, staged *os.File) error {
	if extractTestHookAfterTempPathMetadataTimestampCall != nil {
		return extractTestHookAfterTempPathMetadataTimestampCall(path, entryName, staged)
	}
	return nil
}

func copyExtractData(dst io.Writer, src io.Reader) (int64, error) {
	buf := extractCopyBufferPool.Get().([]byte)
	defer extractCopyBufferPool.Put(buf)
	return io.CopyBuffer(dst, src, buf)
}

func (w *extractQuotaWriter) Write(p []byte) (int, error) {
	allowed := len(p)
	var limitErr error

	if maxFile := w.quota.opts.MaxFileBytes; maxFile > 0 {
		remaining := maxFile - w.fileBytes
		if remaining <= 0 {
			return 0, fmt.Errorf("arj: max file bytes exceeded (limit: %d)", maxFile)
		}
		if int64(allowed) > remaining {
			allowed = int(remaining)
			limitErr = fmt.Errorf("arj: max file bytes exceeded (limit: %d)", maxFile)
		}
	}

	if maxTotal := w.quota.opts.MaxTotalBytes; maxTotal > 0 {
		remaining := maxTotal - w.quota.totalBytes
		if remaining <= 0 {
			return 0, fmt.Errorf("arj: max total bytes exceeded (limit: %d)", maxTotal)
		}
		if int64(allowed) > remaining {
			allowed = int(remaining)
			limitErr = fmt.Errorf("arj: max total bytes exceeded (limit: %d)", maxTotal)
		}
	}

	n, err := w.dst.Write(p[:allowed])
	if n > 0 {
		w.fileBytes += int64(n)
		w.quota.totalBytes += int64(n)
	}
	if err != nil {
		return n, err
	}
	if n < allowed {
		return n, io.ErrShortWrite
	}
	if limitErr != nil {
		return n, limitErr
	}
	return n, nil
}

func (q *extractQuota) reserveFile() error {
	if maxFiles := q.opts.MaxFiles; maxFiles > 0 {
		if q.fileCount >= maxFiles {
			return fmt.Errorf("arj: max files exceeded (limit: %d)", maxFiles)
		}
	}
	q.fileCount++
	return nil
}

func (q *extractQuota) reserveFileWithHeaderSize(uncompressedSize uint64) error {
	if err := q.reserveFile(); err != nil {
		return err
	}
	if maxFile := q.opts.MaxFileBytes; maxFile > 0 {
		if uncompressedSize > uint64(maxFile) {
			return fmt.Errorf("arj: max file bytes exceeded (limit: %d)", maxFile)
		}
	}
	if maxTotal := q.opts.MaxTotalBytes; maxTotal > 0 {
		remaining := maxTotal - q.totalBytes
		if remaining <= 0 || uncompressedSize > uint64(remaining) {
			return fmt.Errorf("arj: max total bytes exceeded (limit: %d)", maxTotal)
		}
	}
	return nil
}

func ensureExtractRoot(dir string) (string, error) {
	absDir, err := filepath.Abs(filepath.Clean(dir))
	if err != nil {
		return "", extractPathError(dir, err)
	}

	anchor, err := extractPathAnchor(absDir)
	if err != nil {
		return "", extractPathError(dir, err)
	}

	if err := ensureExistingExtractDir(anchor, absDir, dir); err != nil {
		return "", err
	}

	info, err := os.Lstat(absDir)
	if err != nil {
		return "", extractPathError(dir, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return "", insecureExtractPathError(dir)
	}
	if !info.IsDir() {
		return "", extractPathError(dir, fs.ErrInvalid)
	}
	return absDir, nil
}

func extractPathAnchor(absPath string) (string, error) {
	clean := filepath.Clean(absPath)
	volume := filepath.VolumeName(clean)
	rest := strings.TrimPrefix(clean, volume)
	if !strings.HasPrefix(rest, string(os.PathSeparator)) {
		return "", fs.ErrInvalid
	}
	return volume + string(os.PathSeparator), nil
}

func validateExtractOptions(opts ExtractOptions) error {
	if opts.MaxFiles < 0 && opts.MaxFiles != ExtractUnlimitedFiles {
		return fmt.Errorf("arj: MaxFiles must be >= 0 or ExtractUnlimitedFiles")
	}
	if opts.MaxTotalBytes < 0 && opts.MaxTotalBytes != ExtractUnlimitedBytes {
		return fmt.Errorf("arj: MaxTotalBytes must be >= 0 or ExtractUnlimitedBytes")
	}
	if opts.MaxFileBytes < 0 && opts.MaxFileBytes != ExtractUnlimitedBytes {
		return fmt.Errorf("arj: MaxFileBytes must be >= 0 or ExtractUnlimitedBytes")
	}
	return nil
}

func lstatExtractTarget(path string) (fs.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, extractPathError(path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, insecureExtractPathError(path)
	}
	return info, nil
}

func recheckExtractTarget(path string, before fs.FileInfo) (fs.FileInfo, error) {
	after, err := lstatExtractTarget(path)
	if err != nil {
		return nil, err
	}
	if !os.SameFile(before, after) {
		return nil, insecureExtractPathError(path)
	}
	return after, nil
}

const extractTimestampMatchTolerance = 2 * time.Second

func extractModTimeMatches(got, want time.Time) bool {
	diff := got.Sub(want)
	if diff < 0 {
		diff = -diff
	}
	return diff <= extractTimestampMatchTolerance
}

func verifyPinnedExtractTimestampApplied(target *os.File, entryName string, pinned fs.FileInfo, modTime time.Time) error {
	info, err := target.Stat()
	if err != nil {
		return extractPathError(entryName, err)
	}
	if pinned != nil && !os.SameFile(pinned, info) {
		return insecureExtractPathError(entryName)
	}
	// Non-unix extraction has no portable fd-based utimens equivalent in os,
	// so path-based Chtimes must be verified against the pinned open handle.
	// This is fail-closed detection; if a race redirects Chtimes, extraction
	// aborts even though the redirected target may already have been touched.
	if !extractModTimeMatches(info.ModTime(), modTime) {
		return insecureExtractPathError(entryName)
	}
	return nil
}

func applyExtractMetadata(path string, mode fs.FileMode, modTime time.Time) error {
	runExtractTestHookPathMetadataApply(path, path)

	info, err := lstatExtractTarget(path)
	if err != nil {
		return err
	}

	target, err := os.Open(path)
	if err != nil {
		return extractPathError(path, err)
	}
	defer func() {
		_ = target.Close()
	}()
	pinnedInfo, err := target.Stat()
	if err != nil {
		return extractPathError(path, err)
	}
	if !os.SameFile(info, pinnedInfo) {
		return insecureExtractPathError(path)
	}

	// Apply mode via opened handle so a path swap cannot redirect chmod.
	if err := target.Chmod(mode.Perm()); err != nil {
		return extractPathError(path, err)
	}
	if _, err := recheckExtractTarget(path, info); err != nil {
		return err
	}

	if !modTime.IsZero() {
		if extractModTimeMatches(pinnedInfo.ModTime(), modTime) {
			return nil
		}
		if _, err := recheckExtractTarget(path, info); err != nil {
			return err
		}
		if err := runExtractTestHookBeforePathMetadataTimestamp(path, path); err != nil {
			return err
		}
		if _, err := recheckExtractTarget(path, info); err != nil {
			return err
		}
		if err := runExtractTestHookBeforePathMetadataTimestampCall(path, path); err != nil {
			return err
		}
		if err := os.Chtimes(path, modTime, modTime); err != nil {
			return extractPathError(path, err)
		}
		if err := runExtractTestHookAfterPathMetadataTimestampCall(path, path); err != nil {
			return err
		}
		if _, err := recheckExtractTarget(path, info); err != nil {
			return err
		}
		if err := verifyPinnedExtractTimestampApplied(target, path, info, modTime); err != nil {
			return err
		}
	}
	return nil
}

func captureStagedTempPathIdentity(staged *os.File, stagedPath, entryName string) (fs.FileInfo, error) {
	stagedInfo, err := staged.Stat()
	if err != nil {
		return nil, extractPathError(entryName, err)
	}
	if !stagedInfo.Mode().IsRegular() {
		return nil, insecureExtractPathError(entryName)
	}
	if err := verifyStagedTempPathIdentity(stagedPath, entryName, stagedInfo); err != nil {
		return nil, err
	}
	return stagedInfo, nil
}

func verifyStagedTempPathIdentity(stagedPath, entryName string, expected fs.FileInfo) error {
	info, err := os.Lstat(stagedPath)
	if err != nil {
		return extractPathError(stagedPath, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return insecureExtractPathError(entryName)
	}
	if !info.Mode().IsRegular() {
		return insecureExtractPathError(entryName)
	}
	if expected != nil && !os.SameFile(expected, info) {
		return insecureExtractPathError(entryName)
	}
	return nil
}

func applyExtractMetadataToTempFile(staged *os.File, path, entryName string, mode fs.FileMode, modTime time.Time) error {
	runExtractTestHookPathMetadataApply(path, entryName)

	if err := staged.Chmod(mode.Perm()); err != nil {
		return extractPathError(entryName, err)
	}
	if !modTime.IsZero() {
		stagedInfo, err := captureStagedTempPathIdentity(staged, path, entryName)
		if err != nil {
			return err
		}
		if extractModTimeMatches(stagedInfo.ModTime(), modTime) {
			return nil
		}
		if err := runExtractTestHookBeforeTempPathMetadataTimestamp(path, entryName, staged); err != nil {
			return err
		}
		if err := verifyStagedTempPathIdentity(path, entryName, stagedInfo); err != nil {
			return err
		}
		if err := runExtractTestHookBeforeTempPathMetadataTimestampCall(path, entryName, staged); err != nil {
			return err
		}
		if err := os.Chtimes(path, modTime, modTime); err != nil {
			return extractPathError(entryName, err)
		}
		if err := runExtractTestHookAfterTempPathMetadataTimestampCall(path, entryName, staged); err != nil {
			return err
		}
		if err := verifyStagedTempPathIdentity(path, entryName, stagedInfo); err != nil {
			return err
		}
		if err := verifyPinnedExtractTimestampApplied(staged, entryName, stagedInfo, modTime); err != nil {
			return err
		}
	}
	return nil
}

func extractPathError(path string, err error) error {
	if err == nil {
		return nil
	}
	var pathErr *fs.PathError
	if errors.As(err, &pathErr) {
		return err
	}
	return &fs.PathError{Op: "extract", Path: path, Err: err}
}

func insecureExtractPathError(path string) error {
	return &fs.PathError{Op: "extract", Path: path, Err: ErrInsecurePath}
}
