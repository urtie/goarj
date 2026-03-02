package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	goarj "github.com/urtie/goarj"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "goarj: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		usage(stderr)
		return errors.New("missing command")
	}

	switch args[0] {
	case "archive":
		return runArchive(args[1:], stdout)
	case "extract":
		return runExtract(args[1:], stdout)
	case "help", "-h", "--help":
		usage(stdout)
		return nil
	default:
		usage(stderr)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runArchive(args []string, stdout io.Writer) error {
	if len(args) != 2 {
		return errors.New("usage: goarj archive <archive.arj> <source>")
	}

	archivePath := filepath.Clean(args[0])
	sourcePath := filepath.Clean(args[1])

	sourceInfo, err := os.Stat(sourcePath)
	if err != nil {
		return fmt.Errorf("stat source %q: %w", sourcePath, err)
	}
	if !sourceInfo.IsDir() && !sourceInfo.Mode().IsRegular() {
		return fmt.Errorf("source %q: must be a directory or regular file", sourcePath)
	}
	if err := validateArchiveDestinationPath(archivePath, sourcePath, sourceInfo); err != nil {
		return err
	}

	out, err := os.Create(archivePath)
	if err != nil {
		return fmt.Errorf("create archive %q: %w", archivePath, err)
	}

	writer := goarj.NewWriter(out)
	if err := writer.SetArchiveName(filepath.Base(archivePath)); err != nil {
		_ = out.Close()
		return fmt.Errorf("set archive name: %w", err)
	}

	writeErr := addSource(writer, sourcePath, sourceInfo)
	closeErr := writer.Close()
	fileCloseErr := out.Close()
	if writeErr != nil {
		return errors.Join(writeErr, closeErr, fileCloseErr)
	}
	if err := errors.Join(closeErr, fileCloseErr); err != nil {
		return err
	}

	_, _ = fmt.Fprintf(stdout, "created %s from %s\n", archivePath, sourcePath)
	return nil
}

func validateArchiveDestinationPath(archivePath, sourcePath string, sourceInfo fs.FileInfo) error {
	absArchivePath, err := filepath.Abs(filepath.Clean(archivePath))
	if err != nil {
		return fmt.Errorf("resolve archive path %q: %w", archivePath, err)
	}
	absSourcePath, err := filepath.Abs(filepath.Clean(sourcePath))
	if err != nil {
		return fmt.Errorf("resolve source path %q: %w", sourcePath, err)
	}

	if sourceInfo.IsDir() {
		insideSource, err := pathContains(absSourcePath, absArchivePath)
		if err != nil {
			return fmt.Errorf("compare archive/source paths: %w", err)
		}
		if insideSource {
			return fmt.Errorf("archive path %q must be outside source directory %q", archivePath, sourcePath)
		}
		return nil
	}

	if absArchivePath == absSourcePath {
		return fmt.Errorf("archive path %q must differ from source file %q", archivePath, sourcePath)
	}

	archiveInfo, err := os.Stat(absArchivePath)
	if err == nil {
		if os.SameFile(sourceInfo, archiveInfo) {
			return fmt.Errorf("archive path %q resolves to source file %q", archivePath, sourcePath)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat archive %q: %w", archivePath, err)
	}
	return nil
}

func pathContains(rootPath, path string) (bool, error) {
	rel, err := filepath.Rel(rootPath, path)
	if err != nil {
		return false, err
	}
	if rel == "." {
		return true, nil
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return false, nil
	}
	return true, nil
}

func addSource(writer *goarj.Writer, sourcePath string, sourceInfo fs.FileInfo) error {
	if sourceInfo.IsDir() {
		return addDirectory(writer, sourcePath)
	}
	return addSingleFile(writer, sourcePath, sourceInfo)
}

func addDirectory(writer *goarj.Writer, sourcePath string) error {
	fsys := os.DirFS(sourcePath)
	return fs.WalkDir(fsys, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if name == "." {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}
		if !d.IsDir() && !info.Mode().IsRegular() {
			return fmt.Errorf("source %q: cannot add non-regular file %q", sourcePath, name)
		}

		header, err := goarj.FileInfoHeader(info)
		if err != nil {
			return fmt.Errorf("build file header for %q: %w", name, err)
		}
		header.Name = name
		header.Method = goarj.Method4
		if d.IsDir() {
			header.Name += "/"
			header.Method = goarj.Store
		}

		fw, err := writer.CreateHeader(header)
		if err != nil {
			return fmt.Errorf("create archive entry for %q: %w", name, err)
		}
		if d.IsDir() {
			return closeWriterIfPossible(fw)
		}

		source, err := fsys.Open(name)
		if err != nil {
			return errors.Join(fmt.Errorf("open source %q: %w", name, err), closeWriterIfPossible(fw))
		}
		_, copyErr := io.Copy(fw, source)
		sourceCloseErr := source.Close()
		entryCloseErr := closeWriterIfPossible(fw)
		if err := errors.Join(copyErr, sourceCloseErr, entryCloseErr); err != nil {
			return fmt.Errorf("write source %q: %w", name, err)
		}
		return nil
	})
}

func addSingleFile(writer *goarj.Writer, sourcePath string, sourceInfo fs.FileInfo) error {
	header, err := goarj.FileInfoHeader(sourceInfo)
	if err != nil {
		return fmt.Errorf("build file header for %q: %w", sourcePath, err)
	}
	header.Name = filepath.Base(sourcePath)
	header.Method = goarj.Method4

	fw, err := writer.CreateHeader(header)
	if err != nil {
		return fmt.Errorf("create archive entry for %q: %w", sourcePath, err)
	}

	source, err := os.Open(sourcePath)
	if err != nil {
		return errors.Join(fmt.Errorf("open source %q: %w", sourcePath, err), closeWriterIfPossible(fw))
	}

	_, copyErr := io.Copy(fw, source)
	sourceCloseErr := source.Close()
	entryCloseErr := closeWriterIfPossible(fw)
	if err := errors.Join(copyErr, sourceCloseErr, entryCloseErr); err != nil {
		return fmt.Errorf("write source %q: %w", sourcePath, err)
	}
	return nil
}

func closeWriterIfPossible(w io.Writer) error {
	if closer, ok := w.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func runExtract(args []string, stdout io.Writer) error {
	if len(args) < 1 || len(args) > 2 {
		return errors.New("usage: goarj extract <archive.arj> [destination]")
	}

	archivePath := filepath.Clean(args[0])
	destination := "."
	if len(args) == 2 {
		destination = filepath.Clean(args[1])
	}

	if err := extractWithGoARJ(archivePath, destination); err != nil {
		return fmt.Errorf("extract %q into %q: %w", archivePath, destination, err)
	}

	_, _ = fmt.Fprintf(stdout, "extracted %s into %s\n", archivePath, destination)
	return nil
}

func extractWithGoARJ(archivePath, destination string) error {
	reader, err := goarj.OpenReader(archivePath)
	if err != nil {
		return fmt.Errorf("open archive %q: %w", archivePath, err)
	}

	extractErr := reader.ExtractAll(destination)
	closeErr := reader.Close()
	return errors.Join(extractErr, closeErr)
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  goarj archive <archive.arj> <source>")
	fmt.Fprintln(w, "  goarj extract <archive.arj> [destination]")
}
