package arj

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// SafeExtractPath returns the local filesystem path for archive entry name
// rooted at dir.
//
// It rejects empty names, ".", names containing backslashes, absolute paths,
// and path traversal ("..") segments. Returned errors are *fs.PathError values
// that wrap ErrInsecurePath.
func SafeExtractPath(dir, name string) (string, error) {
	localName, err := safeExtractRelativePath(name)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, localName), nil
}

func safeExtractRelativePath(name string) (string, error) {
	cleanName := strings.TrimSuffix(name, "/")
	if cleanName == "" || cleanName == "." || strings.ContainsRune(cleanName, '\\') {
		return "", insecureExtractPathError(name)
	}
	if !fs.ValidPath(cleanName) {
		return "", insecureExtractPathError(name)
	}

	localName := filepath.FromSlash(cleanName)
	if !filepath.IsLocal(localName) {
		return "", insecureExtractPathError(name)
	}
	return localName, nil
}

// ensureNoSymlinkComponents rejects extraction paths where any existing
// component under root is a symlink.
func ensureNoSymlinkComponents(root, target, name string, includeTarget bool) error {
	limit := target
	if !includeTarget {
		limit = filepath.Dir(target)
	}

	root = filepath.Clean(root)
	limit = filepath.Clean(limit)

	rootInfo, err := os.Lstat(root)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return extractPathError(root, err)
	}
	if err == nil && rootInfo.Mode()&os.ModeSymlink != 0 {
		return insecureExtractPathError(name)
	}

	rel, err := filepath.Rel(root, limit)
	if err != nil {
		return insecureExtractPathError(name)
	}
	if rel == "." {
		return nil
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return insecureExtractPathError(name)
	}

	current := root
	for _, part := range strings.Split(rel, string(os.PathSeparator)) {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return extractPathError(current, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return insecureExtractPathError(name)
		}
	}
	return nil
}

func ensureExistingExtractDir(root, dirPath, entryName string) error {
	if err := ensureNoSymlinkComponents(root, dirPath, entryName, true); err != nil {
		return err
	}

	cleanRoot := filepath.Clean(root)
	cleanDir := filepath.Clean(dirPath)
	rel, err := filepath.Rel(cleanRoot, cleanDir)
	if err != nil {
		return insecureExtractPathError(entryName)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return insecureExtractPathError(entryName)
	}
	if rel == "." {
		return nil
	}

	current := cleanRoot
	for _, part := range strings.Split(rel, string(os.PathSeparator)) {
		if part == "" || part == "." {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				return extractPathError(current, err)
			}
			if err := os.Mkdir(current, 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
				return extractPathError(current, err)
			}
			info, err = os.Lstat(current)
			if err != nil {
				return extractPathError(current, err)
			}
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return insecureExtractPathError(entryName)
		}
	}

	// Recheck the chain after lookup/create to reduce check/use skew.
	if err := ensureNoSymlinkComponents(cleanRoot, cleanDir, entryName, true); err != nil {
		return err
	}
	return nil
}
