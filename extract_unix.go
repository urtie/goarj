//go:build unix

package arj

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

const unixExtractOpenDirFlags = syscall.O_RDONLY | syscall.O_DIRECTORY | syscall.O_CLOEXEC | syscall.O_NOFOLLOW

type unixExtractRoot struct {
	fd int
}

func (r *Reader) extractAllWithOptions(dir string, opts ExtractOptions) error {
	root, err := openUnixExtractRoot(dir)
	if err != nil {
		return err
	}
	defer func() {
		_ = root.close()
	}()

	quota := &extractQuota{opts: opts}
	dirs := make(map[string]extractedDir)
	password := r.passwordBytes()
	defer clearBytes(password)
	for _, f := range r.File {
		rel, err := safeExtractRelativePath(f.Name)
		if err != nil {
			return err
		}

		if f.isDir() {
			if err := f.unsupportedOpenModeError(password); err != nil {
				return err
			}
			if err := root.mkdirAll(rel, f.Name); err != nil {
				return err
			}
			dirs[rel] = extractedDir{
				path:    rel,
				name:    f.Name,
				mode:    f.Mode(),
				modTime: f.ModTime(),
			}
			continue
		}

		parent := filepath.Dir(rel)
		if parent == "." {
			parent = ""
		}
		if err := root.mkdirAll(parent, f.Name); err != nil {
			return err
		}
		if err := root.extractOneFile(rel, f.Name, f, quota); err != nil {
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
		if err := root.applyDirMetadata(d.path, d.name, d.mode, d.modTime); err != nil {
			return err
		}
	}
	return nil
}

func openUnixExtractRoot(dir string) (*unixExtractRoot, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, extractPathError(dir, err)
	}

	rootFD, err := syscall.Open(string(os.PathSeparator), unixExtractOpenDirFlags, 0)
	if err != nil {
		return nil, extractPathError(dir, err)
	}

	for _, part := range splitAbsPath(abs) {
		nextFD, openErr := openDirAt(rootFD, part)
		if openErr != nil && errors.Is(openErr, syscall.ENOENT) {
			mkdirErr := syscall.Mkdirat(rootFD, part, 0o755)
			if mkdirErr != nil && !errors.Is(mkdirErr, syscall.EEXIST) {
				_ = syscall.Close(rootFD)
				return nil, wrapExtractSyscallErr(dir, mkdirErr)
			}
			nextFD, openErr = openDirAt(rootFD, part)
		}
		if openErr != nil {
			_ = syscall.Close(rootFD)
			return nil, wrapExtractSyscallErr(dir, openErr)
		}
		_ = syscall.Close(rootFD)
		rootFD = nextFD
	}

	return &unixExtractRoot{fd: rootFD}, nil
}

func (r *unixExtractRoot) close() error {
	if r == nil || r.fd < 0 {
		return nil
	}
	err := syscall.Close(r.fd)
	r.fd = -1
	if err != nil {
		return extractPathError(".", err)
	}
	return nil
}

func (r *unixExtractRoot) mkdirAll(rel, entryName string) error {
	dirFD, err := r.openDir(rel, entryName, true)
	if err != nil {
		return err
	}
	if err := syscall.Close(dirFD); err != nil {
		return extractPathError(entryName, err)
	}
	return nil
}

func (r *unixExtractRoot) openDir(rel, entryName string, create bool) (int, error) {
	fd, err := syscall.Dup(r.fd)
	if err != nil {
		return -1, extractPathError(entryName, err)
	}

	for _, part := range splitRelativePath(rel) {
		nextFD, openErr := openDirAt(fd, part)
		if openErr != nil && create && errors.Is(openErr, syscall.ENOENT) {
			mkdirErr := syscall.Mkdirat(fd, part, 0o755)
			if mkdirErr != nil && !errors.Is(mkdirErr, syscall.EEXIST) {
				_ = syscall.Close(fd)
				return -1, wrapExtractSyscallErr(entryName, mkdirErr)
			}
			nextFD, openErr = openDirAt(fd, part)
		}
		if openErr != nil {
			_ = syscall.Close(fd)
			return -1, wrapExtractSyscallErr(entryName, openErr)
		}
		_ = syscall.Close(fd)
		fd = nextFD
	}

	return fd, nil
}

func (r *unixExtractRoot) extractOneFile(relPath, entryName string, f *File, quota *extractQuota) (err error) {
	if err := quota.reserveFileWithHeaderSize(f.UncompressedSize64); err != nil {
		return err
	}

	rc, err := f.Open()
	if err != nil {
		return err
	}
	defer rc.Close()

	runExtractTestHookBeforeCreate(entryName)

	parent := filepath.Dir(relPath)
	if parent == "." {
		parent = ""
	}
	parentFD, err := r.openDir(parent, entryName, true)
	if err != nil {
		return err
	}
	defer func() {
		_ = syscall.Close(parentFD)
	}()

	tmpFile, tmpName, err := createTempFileAt(parentFD, entryName)
	if err != nil {
		return err
	}
	removeTemp := true
	defer func() {
		_ = tmpFile.Close()
		if removeTemp {
			_ = syscall.Unlinkat(parentFD, tmpName)
		}
	}()

	if _, err := io.Copy(&extractQuotaWriter{dst: tmpFile, quota: quota}, rc); err != nil {
		return err
	}
	if err := applyExtractMetadataToFD(int(tmpFile.Fd()), entryName, f.Mode(), f.ModTime()); err != nil {
		return err
	}
	if err := runExtractTestHookBeforeCommit(entryName, tmpFile); err != nil {
		return err
	}
	stagedInfo, err := captureStagedTempIdentityAt(parentFD, tmpFile, tmpName, entryName)
	if err != nil {
		return err
	}
	if err := verifyStagedTempIdentityAt(parentFD, tmpName, entryName, stagedInfo); err != nil {
		return err
	}

	base := filepath.Base(relPath)
	if err := syscall.Renameat(parentFD, tmpName, parentFD, base); err != nil {
		return wrapExtractSyscallErr(entryName, err)
	}
	removeTemp = false
	if err := tmpFile.Close(); err != nil {
		return extractPathError(entryName, err)
	}
	return nil
}

func (r *unixExtractRoot) applyDirMetadata(relPath, entryName string, mode fs.FileMode, modTime time.Time) error {
	fd, err := r.openDir(relPath, entryName, false)
	if err != nil {
		return err
	}
	defer func() {
		_ = syscall.Close(fd)
	}()
	return applyExtractMetadataToFD(fd, entryName, mode, modTime)
}

func createTempFileAt(parentFD int, entryName string) (*os.File, string, error) {
	for i := 0; i < 1024; i++ {
		suffix, err := randomExtractTempNameSuffix()
		if err != nil {
			return nil, "", extractPathError(entryName, err)
		}
		name := ".arj-extract-" + suffix
		fd, err := syscall.Openat(
			parentFD,
			name,
			syscall.O_RDWR|syscall.O_CREAT|syscall.O_EXCL|syscall.O_CLOEXEC|syscall.O_NOFOLLOW,
			0o600,
		)
		if errors.Is(err, syscall.EEXIST) {
			continue
		}
		if err != nil {
			return nil, "", wrapExtractSyscallErr(entryName, err)
		}
		return os.NewFile(uintptr(fd), name), name, nil
	}
	return nil, "", extractPathError(entryName, syscall.EEXIST)
}

func randomExtractTempNameSuffix() (string, error) {
	var raw [12]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func captureStagedTempIdentityAt(parentFD int, staged *os.File, stagedName, entryName string) (fs.FileInfo, error) {
	stagedInfo, err := staged.Stat()
	if err != nil {
		return nil, extractPathError(entryName, err)
	}
	if !stagedInfo.Mode().IsRegular() {
		return nil, insecureExtractPathError(entryName)
	}
	if err := verifyStagedTempIdentityAt(parentFD, stagedName, entryName, stagedInfo); err != nil {
		return nil, err
	}
	return stagedInfo, nil
}

func verifyStagedTempIdentityAt(parentFD int, stagedName, entryName string, expected fs.FileInfo) error {
	currentInfo, err := statExtractFileNoFollowAt(parentFD, stagedName, entryName)
	if err != nil {
		return err
	}
	if !currentInfo.Mode().IsRegular() {
		return insecureExtractPathError(entryName)
	}
	if expected != nil && !os.SameFile(expected, currentInfo) {
		return insecureExtractPathError(entryName)
	}
	return nil
}

func statExtractFileNoFollowAt(parentFD int, name, entryName string) (fs.FileInfo, error) {
	fd, err := syscall.Openat(parentFD, name, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, wrapExtractSyscallErr(entryName, err)
	}
	f := os.NewFile(uintptr(fd), name)
	info, statErr := f.Stat()
	closeErr := f.Close()
	if statErr != nil {
		return nil, extractPathError(entryName, statErr)
	}
	if closeErr != nil {
		return nil, extractPathError(entryName, closeErr)
	}
	return info, nil
}

func applyExtractMetadataAt(parentFD int, name, entryName string, mode fs.FileMode, modTime time.Time) error {
	fd, err := syscall.Openat(parentFD, name, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return wrapExtractSyscallErr(entryName, err)
	}
	defer func() {
		_ = syscall.Close(fd)
	}()
	return applyExtractMetadataToFD(fd, entryName, mode, modTime)
}

func applyExtractMetadataToFD(fd int, entryName string, mode fs.FileMode, modTime time.Time) error {
	if err := syscall.Fchmod(fd, uint32(mode.Perm())); err != nil {
		return extractPathError(entryName, err)
	}
	if !modTime.IsZero() {
		tv := []syscall.Timeval{
			syscall.NsecToTimeval(modTime.UnixNano()),
			syscall.NsecToTimeval(modTime.UnixNano()),
		}
		if err := syscall.Futimes(fd, tv); err != nil {
			return extractPathError(entryName, err)
		}
	}
	return nil
}

func openDirAt(parentFD int, name string) (int, error) {
	return syscall.Openat(parentFD, name, unixExtractOpenDirFlags, 0)
}

func splitAbsPath(path string) []string {
	parts := strings.Split(filepath.Clean(path), string(os.PathSeparator))
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}
		out = append(out, part)
	}
	return out
}

func splitRelativePath(path string) []string {
	if path == "" || path == "." {
		return nil
	}
	return strings.Split(path, string(os.PathSeparator))
}

func wrapExtractSyscallErr(path string, err error) error {
	if errors.Is(err, syscall.ELOOP) || errors.Is(err, syscall.ENOTDIR) {
		return insecureExtractPathError(path)
	}
	return extractPathError(path, err)
}
