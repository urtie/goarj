//go:build !unix

package arj

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func (r *Reader) extractAllWithOptions(dir string, opts ExtractOptions) error {
	if opts.Strict {
		return extractPathError(dir, ErrStrictModeUnsupported)
	}

	root, err := ensureExtractRoot(dir)
	if err != nil {
		return err
	}

	quota := &extractQuota{opts: opts}
	dirs := make(map[string]extractedDir)
	password := r.passwordBytes()
	defer clearBytes(password)
	for _, f := range r.File {
		target, err := SafeExtractPath(root, f.Name)
		if err != nil {
			return err
		}

		if f.isDir() {
			if err := f.unsupportedOpenModeError(password); err != nil {
				return err
			}
			if err := ensureExistingExtractDir(root, target, f.Name); err != nil {
				return err
			}
			dirs[target] = extractedDir{
				path:    target,
				name:    f.Name,
				mode:    f.Mode(),
				modTime: f.ModTime(),
			}
			continue
		}

		parent := filepath.Dir(target)
		if err := ensureExistingExtractDir(root, parent, f.Name); err != nil {
			return err
		}
		if err := extractOneFilePath(root, target, f.Name, f, quota); err != nil {
			return fmt.Errorf("arj: extract %q: %w", f.Name, err)
		}
	}

	orderedDirs := make([]extractedDir, 0, len(dirs))
	for _, d := range dirs {
		orderedDirs = append(orderedDirs, d)
	}
	sort.Slice(orderedDirs, func(i, j int) bool {
		depthI := strings.Count(orderedDirs[i].path, string(os.PathSeparator))
		depthJ := strings.Count(orderedDirs[j].path, string(os.PathSeparator))
		if depthI != depthJ {
			return depthI > depthJ
		}
		return orderedDirs[i].path > orderedDirs[j].path
	})
	for _, d := range orderedDirs {
		if err := applyExtractMetadata(d.path, d.mode, d.modTime); err != nil {
			return err
		}
	}
	return nil
}

func (r *StreamReader) extractAllWithOptions(dir string, opts ExtractOptions) error {
	if opts.Strict {
		return extractPathError(dir, ErrStrictModeUnsupported)
	}

	root, err := ensureExtractRoot(dir)
	if err != nil {
		return err
	}

	quota := &extractQuota{opts: opts}
	dirs := make(map[string]extractedDir)
	for {
		h, rc, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if h == nil || rc == nil {
			return ErrFormat
		}

		target, err := SafeExtractPath(root, h.Name)
		if err != nil {
			abortStreamReadCloser(rc)
			return err
		}

		if h.isDir() {
			if err := ensureExistingExtractDir(root, target, h.Name); err != nil {
				abortStreamReadCloser(rc)
				return err
			}
			dirs[target] = extractedDir{
				path:    target,
				name:    h.Name,
				mode:    h.Mode(),
				modTime: h.ModTime(),
			}
			abortStreamReadCloser(rc)
			continue
		}

		if err := quota.reserveFileWithHeaderSize(h.UncompressedSize64); err != nil {
			abortStreamReadCloser(rc)
			return fmt.Errorf("arj: extract %q: %w", h.Name, err)
		}

		parent := filepath.Dir(target)
		if err := ensureExistingExtractDir(root, parent, h.Name); err != nil {
			abortStreamReadCloser(rc)
			return err
		}
		if err := extractOneStreamFilePath(root, target, h.Name, h, rc, quota); err != nil {
			return fmt.Errorf("arj: extract %q: %w", h.Name, err)
		}
	}

	orderedDirs := make([]extractedDir, 0, len(dirs))
	for _, d := range dirs {
		orderedDirs = append(orderedDirs, d)
	}
	sort.Slice(orderedDirs, func(i, j int) bool {
		depthI := strings.Count(orderedDirs[i].path, string(os.PathSeparator))
		depthJ := strings.Count(orderedDirs[j].path, string(os.PathSeparator))
		if depthI != depthJ {
			return depthI > depthJ
		}
		return orderedDirs[i].path > orderedDirs[j].path
	})
	for _, d := range orderedDirs {
		if err := applyExtractMetadata(d.path, d.mode, d.modTime); err != nil {
			return err
		}
	}
	return nil
}

func extractOneFilePath(root, path, name string, f *File, quota *extractQuota) (err error) {
	if err := quota.reserveFileWithHeaderSize(f.UncompressedSize64); err != nil {
		return err
	}

	rc, err := f.Open()
	if err != nil {
		return err
	}
	defer rc.Close()

	if err := ensureNoSymlinkComponents(root, path, name, true); err != nil {
		return err
	}
	runExtractTestHookBeforeCreate(name)

	tmp, err := os.CreateTemp(root, ".arj-extract-*")
	if err != nil {
		return extractPathError(path, err)
	}
	tmpPath := tmp.Name()
	removeTemp := true
	defer func() {
		_ = tmp.Close()
		if removeTemp {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := io.Copy(&extractQuotaWriter{dst: tmp, quota: quota}, rc); err != nil {
		return err
	}

	if err := applyExtractMetadataToTempFile(tmp, tmpPath, name, f.Mode(), f.ModTime()); err != nil {
		return err
	}
	if err := runExtractTestHookBeforeCommit(name, tmp); err != nil {
		return err
	}
	stagedInfo, err := captureStagedTempPathIdentity(tmp, tmpPath, name)
	if err != nil {
		return err
	}

	if err := tmp.Close(); err != nil {
		return extractPathError(path, err)
	}

	if err := ensureNoSymlinkComponents(root, path, name, true); err != nil {
		return err
	}
	if err := verifyStagedTempPathIdentity(tmpPath, name, stagedInfo); err != nil {
		return err
	}

	if err := commitExtractTempFile(root, path, name, tmpPath, stagedInfo); err != nil {
		return err
	}
	removeTemp = false
	return nil
}

func extractOneStreamFilePath(root, path, name string, h *FileHeader, rc io.ReadCloser, quota *extractQuota) (err error) {
	if h == nil || rc == nil {
		return ErrFormat
	}
	success := false
	defer func() {
		if !success {
			abortStreamReadCloser(rc)
		}
	}()

	if err := ensureNoSymlinkComponents(root, path, name, true); err != nil {
		return err
	}
	runExtractTestHookBeforeCreate(name)

	tmp, err := os.CreateTemp(root, ".arj-extract-*")
	if err != nil {
		return extractPathError(path, err)
	}
	tmpPath := tmp.Name()
	removeTemp := true
	defer func() {
		_ = tmp.Close()
		if removeTemp {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := io.Copy(&extractQuotaWriter{dst: tmp, quota: quota}, rc); err != nil {
		return err
	}
	if err := rc.Close(); err != nil {
		return err
	}
	success = true

	if err := applyExtractMetadataToTempFile(tmp, tmpPath, name, h.Mode(), h.ModTime()); err != nil {
		return err
	}
	if err := runExtractTestHookBeforeCommit(name, tmp); err != nil {
		return err
	}
	stagedInfo, err := captureStagedTempPathIdentity(tmp, tmpPath, name)
	if err != nil {
		return err
	}

	if err := tmp.Close(); err != nil {
		return extractPathError(path, err)
	}

	if err := ensureNoSymlinkComponents(root, path, name, true); err != nil {
		return err
	}
	if err := verifyStagedTempPathIdentity(tmpPath, name, stagedInfo); err != nil {
		return err
	}

	if err := commitExtractTempFile(root, path, name, tmpPath, stagedInfo); err != nil {
		return err
	}
	removeTemp = false
	return nil
}

func commitExtractTempFile(root, path, name, stagedPath string, stagedInfo fs.FileInfo) error {
	if err := verifyStagedTempPathIdentity(stagedPath, name, stagedInfo); err != nil {
		return err
	}
	if err := os.Rename(stagedPath, path); err != nil {
		if !isRenameDestinationExistsError(err) {
			return extractPathError(path, err)
		}
		if err := retryCommitReplaceExisting(root, path, name, stagedPath, stagedInfo, err); err != nil {
			return err
		}
		return nil
	}
	if err := runExtractTestHookAfterNonUnixCommitRename(name, path); err != nil {
		return err
	}
	if err := verifyPostCommitExtractPath(root, path, name, stagedInfo); err != nil {
		if rollbackErr := rollbackPostCommitExtractPath(root, path, name, stagedInfo); rollbackErr != nil {
			err = fmt.Errorf("%w (rollback failed: %v)", err, rollbackErr)
		}
		return err
	}
	return nil
}

func retryCommitReplaceExisting(root, path, name, stagedPath string, stagedInfo fs.FileInfo, renameErr error) error {
	if err := ensureNoSymlinkComponents(root, path, name, true); err != nil {
		return err
	}

	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return extractPathError(path, renameErr)
		}
		return extractPathError(path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return insecureExtractPathError(name)
	}
	if info.IsDir() {
		return extractPathError(path, renameErr)
	}

	backupPath, err := reserveExtractCommitBackupPath(path)
	if err != nil {
		return err
	}
	if err := os.Rename(path, backupPath); err != nil {
		return extractPathError(path, err)
	}

	committed := false
	defer func() {
		if committed {
			_ = os.Remove(backupPath)
		}
	}()

	if err := runExtractTestHookBeforeNonUnixFallbackCommit(name, path, stagedPath, backupPath); err != nil {
		if restoreErr := restoreExtractCommitBackup(path, name, backupPath); restoreErr != nil {
			err = fmt.Errorf("%w (rollback failed: %v)", err, restoreErr)
		}
		return err
	}
	if err := ensureNoSymlinkComponents(root, path, name, false); err != nil {
		if restoreErr := restoreExtractCommitBackup(path, name, backupPath); restoreErr != nil {
			err = fmt.Errorf("%w (rollback failed: %v)", err, restoreErr)
		}
		return err
	}
	if err := verifyStagedTempPathIdentity(stagedPath, name, stagedInfo); err != nil {
		if restoreErr := restoreExtractCommitBackup(path, name, backupPath); restoreErr != nil {
			err = fmt.Errorf("%w (rollback failed: %v)", err, restoreErr)
		}
		return err
	}
	if err := os.Rename(stagedPath, path); err != nil {
		err = extractPathError(path, err)
		if restoreErr := restoreExtractCommitBackup(path, name, backupPath); restoreErr != nil {
			err = fmt.Errorf("%w (rollback failed: %v)", err, restoreErr)
		}
		return err
	}
	if err := runExtractTestHookAfterNonUnixCommitRename(name, path); err != nil {
		if restoreErr := restoreExtractCommitBackup(path, name, backupPath); restoreErr != nil {
			err = fmt.Errorf("%w (rollback failed: %v)", err, restoreErr)
		} else {
			err = extractPathError(path, err)
		}
		return err
	}
	if err := verifyPostCommitExtractPath(root, path, name, stagedInfo); err != nil {
		if restoreErr := restoreExtractCommitBackup(path, name, backupPath); restoreErr != nil {
			err = fmt.Errorf("%w (rollback failed: %v)", err, restoreErr)
		}
		return err
	}
	committed = true
	return nil
}

func isRenameDestinationExistsError(err error) bool {
	return os.IsExist(err) || errors.Is(err, fs.ErrExist) || errors.Is(err, os.ErrExist)
}

func verifyPostCommitExtractPath(root, path, name string, stagedInfo fs.FileInfo) error {
	if err := ensureNoSymlinkComponents(root, path, name, true); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return extractPathError(path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return insecureExtractPathError(name)
	}
	if !info.Mode().IsRegular() {
		return insecureExtractPathError(name)
	}
	if stagedInfo != nil && !os.SameFile(stagedInfo, info) {
		return insecureExtractPathError(name)
	}
	return nil
}

func rollbackPostCommitExtractPath(_ string, path, name string, stagedInfo fs.FileInfo) error {
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return extractPathError(path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return insecureExtractPathError(name)
	}
	if !info.Mode().IsRegular() {
		return insecureExtractPathError(name)
	}
	if stagedInfo != nil && !os.SameFile(stagedInfo, info) {
		return insecureExtractPathError(name)
	}
	if err := os.Remove(path); err != nil {
		return extractPathError(path, err)
	}
	return nil
}

func reserveExtractCommitBackupPath(path string) (string, error) {
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	f, err := os.CreateTemp(dir, "."+base+".arj-backup-*")
	if err != nil {
		return "", extractPathError(path, err)
	}
	backupPath := f.Name()
	if err := f.Close(); err != nil {
		_ = os.Remove(backupPath)
		return "", extractPathError(path, err)
	}
	if err := os.Remove(backupPath); err != nil {
		return "", extractPathError(path, err)
	}
	return backupPath, nil
}

func restoreExtractCommitBackup(path, entryName, backupPath string) error {
	if err := os.Rename(backupPath, path); err != nil {
		if !isRenameDestinationExistsError(err) {
			return extractPathError(path, err)
		}
		info, statErr := os.Lstat(path)
		if statErr != nil {
			return extractPathError(path, statErr)
		}
		if info.Mode()&os.ModeSymlink != 0 || info.IsDir() {
			return insecureExtractPathError(entryName)
		}
		if removeErr := os.Remove(path); removeErr != nil {
			return extractPathError(path, removeErr)
		}
		if retryErr := os.Rename(backupPath, path); retryErr != nil {
			return extractPathError(path, retryErr)
		}
	}
	return nil
}
