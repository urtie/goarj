package arj

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sort"
	"strings"
)

// Open opens the named file in the ARJ archive using fs.FS path semantics.
func (r *Reader) Open(name string) (fs.File, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
	}

	idx := r.fsIndexSnapshot()
	if f, ok := idx.files[name]; ok {
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}
		return &openFile{ReadCloser: rc, fh: &f.FileHeader}, nil
	}

	if dir, ok := idx.dirs[name]; ok {
		return &openDir{fh: dir.header, path: name, entries: dir.entries}, nil
	}

	return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
}

type openFile struct {
	io.ReadCloser
	fh *FileHeader
}

func (f *openFile) Stat() (fs.FileInfo, error) {
	return headerFileInfo{f.fh}, nil
}

type openDir struct {
	fh      *FileHeader
	path    string
	entries []fs.DirEntry
	offset  int
}

func (d *openDir) Close() error {
	return nil
}

func (d *openDir) Stat() (fs.FileInfo, error) {
	return headerFileInfo{d.fh}, nil
}

func (d *openDir) Read([]byte) (int, error) {
	return 0, &fs.PathError{Op: "read", Path: d.path, Err: errReadOnDirectory}
}

func (d *openDir) ReadDir(count int) ([]fs.DirEntry, error) {
	n := len(d.entries) - d.offset
	if count > 0 && n > count {
		n = count
	}
	if n == 0 {
		if count <= 0 {
			return nil, nil
		}
		return nil, io.EOF
	}

	list := make([]fs.DirEntry, n)
	copy(list, d.entries[d.offset:d.offset+n])
	d.offset += n
	return list, nil
}

type dirEntryCandidate struct {
	entry    fs.DirEntry
	isDir    bool
	explicit bool
}

type readerFSIndex struct {
	files map[string]*File
	dirs  map[string]readerDirIndex
}

type readerDirIndex struct {
	header  *FileHeader
	entries []fs.DirEntry
}

func addDirEntryCandidate(children map[string]dirEntryCandidate, name string, entry fs.DirEntry, isDir, explicit bool) {
	current, ok := children[name]
	if !ok {
		children[name] = dirEntryCandidate{
			entry:    entry,
			isDir:    isDir,
			explicit: explicit,
		}
		return
	}

	// Keep a deterministic collision policy: exact file paths win over
	// implicit/explicit directory placeholders.
	if current.isDir && !isDir {
		children[name] = dirEntryCandidate{
			entry:    entry,
			isDir:    isDir,
			explicit: explicit,
		}
		return
	}
	if current.isDir == isDir && isDir && !current.explicit && explicit {
		children[name] = dirEntryCandidate{
			entry:    entry,
			isDir:    isDir,
			explicit: explicit,
		}
	}
}

func fsParentPath(name string) string {
	if idx := strings.LastIndexByte(name, '/'); idx >= 0 {
		return name[:idx]
	}
	return "."
}

func splitFSPath(name string) (parent, base string) {
	if idx := strings.LastIndexByte(name, '/'); idx >= 0 {
		return name[:idx], name[idx+1:]
	}
	return ".", name
}

func hasFileAncestor(name string, files map[string]*File) bool {
	for parent := fsParentPath(name); parent != "."; parent = fsParentPath(parent) {
		if _, ok := files[parent]; ok {
			return true
		}
	}
	return false
}

func ensureAncestorDirs(dirByName map[string]*FileHeader, name string, files map[string]*File, explicitDirs map[string]*FileHeader) {
	for parent := fsParentPath(name); parent != "."; parent = fsParentPath(parent) {
		if _, blocked := files[parent]; blocked {
			return
		}
		if _, exists := dirByName[parent]; exists {
			continue
		}
		if header, ok := explicitDirs[parent]; ok {
			dirByName[parent] = header
			continue
		}
		dirByName[parent] = syntheticDirHeader(parent)
	}
}

func (r *Reader) fsIndexSnapshot() *readerFSIndex {
	r.stateMu.RLock()
	idx := r.fsIndex
	r.stateMu.RUnlock()
	if idx != nil {
		return idx
	}

	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	if r.fsIndex == nil {
		r.fsIndex = buildReaderFSIndex(r.File)
	}
	return r.fsIndex
}

func buildReaderFSIndex(files []*File) *readerFSIndex {
	fileByName := make(map[string]*File, len(files))
	explicitDirs := make(map[string]*FileHeader, len(files))
	for _, f := range files {
		fullName := strings.TrimSuffix(f.Name, "/")
		if fullName == "" || fullName == "." || !fs.ValidPath(fullName) {
			continue
		}
		if f.isDir() {
			if _, exists := explicitDirs[fullName]; !exists {
				explicitDirs[fullName] = &f.FileHeader
			}
			continue
		}
		if _, exists := fileByName[fullName]; !exists {
			fileByName[fullName] = f
		}
	}

	for name := range fileByName {
		if hasFileAncestor(name, fileByName) {
			delete(fileByName, name)
		}
	}

	dirByName := map[string]*FileHeader{
		".": syntheticDirHeader("."),
	}
	for name, header := range explicitDirs {
		if _, blocked := fileByName[name]; blocked || hasFileAncestor(name, fileByName) {
			continue
		}
		if _, exists := dirByName[name]; !exists {
			dirByName[name] = header
		}
		ensureAncestorDirs(dirByName, name, fileByName, explicitDirs)
	}
	for name := range fileByName {
		ensureAncestorDirs(dirByName, name, fileByName, explicitDirs)
	}

	childrenByDir := make(map[string]map[string]dirEntryCandidate, len(dirByName))
	ensureChildren := func(name string) map[string]dirEntryCandidate {
		children, ok := childrenByDir[name]
		if !ok {
			children = make(map[string]dirEntryCandidate)
			childrenByDir[name] = children
		}
		return children
	}

	for name, header := range dirByName {
		if name == "." {
			continue
		}
		parent, child := splitFSPath(name)
		if _, ok := dirByName[parent]; !ok {
			continue
		}
		_, explicit := explicitDirs[name]
		addDirEntryCandidate(ensureChildren(parent), child, headerFileInfo{header}, true, explicit)
	}
	for name, f := range fileByName {
		parent, child := splitFSPath(name)
		if _, ok := dirByName[parent]; !ok {
			continue
		}
		addDirEntryCandidate(ensureChildren(parent), child, headerFileInfo{&f.FileHeader}, false, true)
	}

	dirs := make(map[string]readerDirIndex, len(dirByName))
	for name, header := range dirByName {
		if header == nil {
			header = syntheticDirHeader(name)
		}

		children := childrenByDir[name]
		entries := make([]fs.DirEntry, 0, len(children))
		for _, child := range children {
			entries = append(entries, child.entry)
		}
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].Name() < entries[j].Name()
		})
		dirs[name] = readerDirIndex{
			header:  header,
			entries: entries,
		}
	}

	return &readerFSIndex{
		files: fileByName,
		dirs:  dirs,
	}
}

func (r *Reader) lookupDir(name string) (*FileHeader, []fs.DirEntry, bool) {
	idx := r.fsIndexSnapshot()
	dir, ok := idx.dirs[name]
	if !ok {
		return nil, nil, false
	}
	return dir.header, dir.entries, true
}

func syntheticDirHeader(name string) *FileHeader {
	if name == "." {
		return &FileHeader{Name: "./", fileType: arjFileTypeDirectory}
	}
	return &FileHeader{Name: name + "/", fileType: arjFileTypeDirectory}
}

// OpenRaw returns a reader for the file's raw compressed bytes.
func (f *File) OpenRaw() (io.Reader, error) {
	const maxInt64 = int64(^uint64(0) >> 1)

	if err := f.validateOpenState(); err != nil {
		return nil, err
	}

	segments := f.segmentList()
	readers := make([]io.Reader, 0, len(segments))
	for _, segment := range segments {
		if segment.dataOffset < 0 || segment.compressedSize > uint64(maxInt64) {
			return nil, ErrFormat
		}
		readers = append(readers, io.NewSectionReader(f.arj.r, segment.dataOffset, int64(segment.compressedSize)))
	}
	if len(readers) == 1 {
		return readers[0], nil
	}
	return io.MultiReader(readers...), nil
}

// CreateRaw adds a file to the archive by writing caller-provided local-header
// metadata and raw entry bytes without compression.
//
// CreateRaw normalizes/defaults missing local-header fields, but it does not
// derive or verify CRC/size values from the written payload.
func (w *Writer) CreateRaw(fh *FileHeader) (io.Writer, error) {
	if err := w.prepare(); err != nil {
		return nil, err
	}
	if fh == nil {
		return nil, errNilFileHeader
	}

	h := *fh
	h.LocalExtendedHeaders = cloneLocalExtendedHeaders(fh.LocalExtendedHeaders)
	h.firstHeaderExtra = append([]byte(nil), fh.firstHeaderExtra...)
	if err := unsupportedSecurityFlagsError(h.Flags, h.EncryptionVersion()); err != nil {
		return nil, err
	}
	freshLocalHeader := h.FirstHeaderSize == 0
	if h.FirstHeaderSize == 0 {
		h.FirstHeaderSize = arjMinFirstHeaderSize
	}
	if err := normalizeLocalFirstHeaderExtra(&h); err != nil {
		return nil, err
	}
	syncFileHeaderExtMetadata(&h)
	if h.Method > 0xff {
		return nil, ErrAlgorithm
	}
	if h.ArchiverVersion == 0 && freshLocalHeader {
		h.ArchiverVersion = arjVersionCurrent
	}
	if h.MinVersion == 0 && freshLocalHeader {
		h.MinVersion = arjVersionNeeded
	}
	if h.HostOS == 0 && freshLocalHeader {
		h.HostOS = currentHostOS()
	}
	if h.isDir() {
		h.fileType = arjFileTypeDirectory
		if !strings.HasSuffix(h.Name, "/") {
			h.Name += "/"
		}
		h.CompressedSize64 = 0
		h.UncompressedSize64 = 0
		h.CRC32 = 0
	} else if h.fileType == arjFileTypeMain {
		h.fileType = arjFileTypeBinary
	}
	if h.fileMode == 0 && freshLocalHeader {
		if h.isDir() {
			h.fileMode = uint16(fileModeToUnixMode(fs.ModeDir | 0o755))
		} else {
			h.fileMode = uint16(fileModeToUnixMode(0o644))
		}
	}
	if h.modifiedDOS == 0 && freshLocalHeader {
		h.modifiedDOS = timeToDosDateTime(h.Modified)
	}

	if err := validateLocalHeaderLengths(&h); err != nil {
		return nil, err
	}
	if err := validateLocalExtendedHeaders(&h); err != nil {
		return nil, err
	}
	if h.UncompressedSize64 > 0xffffffff || h.CompressedSize64 > 0xffffffff {
		return nil, errFileTooLarge
	}
	if !w.wroteMain {
		if err := w.writeMainHeader(); err != nil {
			return nil, err
		}
	}
	if err := writeLocalFileHeader(w.cw, &h); err != nil {
		return nil, err
	}

	if h.isDir() {
		return rawDirWriter{}, nil
	}
	return rawFileWriter{w: w.cw}, nil
}

func writeLocalFileHeader(w io.Writer, h *FileHeader) error {
	syncFileHeaderExtMetadata(h)
	if err := unsupportedSecurityFlagsError(h.Flags, h.EncryptionVersion()); err != nil {
		return err
	}
	if h.FirstHeaderSize == 0 {
		h.FirstHeaderSize = arjMinFirstHeaderSize
	}
	if err := validateLocalHeaderLengths(h); err != nil {
		return err
	}
	if err := validateLocalExtendedHeaders(h); err != nil {
		return err
	}
	if h.UncompressedSize64 > maxARJFileSize || h.CompressedSize64 > maxARJFileSize {
		return errFileTooLarge
	}

	basic := make([]byte, int(h.FirstHeaderSize))
	basic[0] = h.FirstHeaderSize
	basic[1] = h.ArchiverVersion
	basic[2] = h.MinVersion
	basic[3] = h.HostOS
	basic[4] = h.Flags
	basic[5] = byte(h.Method)
	basic[6] = h.fileType
	basic[7] = h.PasswordModifier
	binary.LittleEndian.PutUint32(basic[8:12], h.modifiedDOS)
	binary.LittleEndian.PutUint32(basic[12:16], uint32(h.CompressedSize64))
	binary.LittleEndian.PutUint32(basic[16:20], uint32(h.UncompressedSize64))
	binary.LittleEndian.PutUint32(basic[20:24], h.CRC32)
	binary.LittleEndian.PutUint16(basic[24:26], h.FilespecPos)
	binary.LittleEndian.PutUint16(basic[26:28], h.fileMode)
	basic[28] = h.ExtFlags
	basic[29] = h.ChapterNumber
	copy(basic[arjMinFirstHeaderSize:], h.firstHeaderExtra)

	var full []byte
	full = append(full, basic...)
	full = append(full, h.Name...)
	full = append(full, 0)
	full = append(full, h.Comment...)
	full = append(full, 0)

	if len(full) > arjMaxBasicHeaderSize {
		return errLongName
	}
	return writeHeaderBlockWithExt(w, full, h.LocalExtendedHeaders)
}

type rawFileWriter struct {
	w io.Writer
}

func (w rawFileWriter) Write(p []byte) (int, error) {
	return w.w.Write(p)
}

type rawDirWriter struct{}

func (rawDirWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return 0, errDirectoryFileData
}

func closeStagedWriterIfPossible(w io.Writer) error {
	if closer, ok := w.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

func abortStagedWriter(w io.Writer, cause error) error {
	if cause == nil {
		return closeStagedWriterIfPossible(w)
	}
	if latcher, ok := w.(writeErrLatcher); ok {
		latcher.latchWriteErr(cause)
	}
	return errors.Join(cause, closeStagedWriterIfPossible(w))
}

// Copy copies f into w by copying raw bytes and local-header metadata.
//
// For safety, raw-copy of logical entries reconstructed from multiple ARJ
// continuation segments (as returned by OpenMultiReader) is rejected.
func (w *Writer) Copy(f *File) error {
	if f == nil {
		return errNilFileHeader
	}
	if len(f.segments) > 1 {
		return errRawCopyMultisegment
	}
	if f.arj != nil {
		if err := unsupportedMainSecurityFlagsError(f.arj.ArchiveHeader.Flags, f.arj.ArchiveHeader.EncryptionVersion()); err != nil {
			return err
		}
	} else {
		return ErrFormat
	}
	r, err := f.OpenRaw()
	if err != nil {
		return err
	}
	fh := f.FileHeader
	fw, err := w.CreateRaw(&fh)
	if err != nil {
		return errors.Join(err, closeReaderIfPossible(r))
	}

	n, copyErr := io.Copy(fw, r)
	readCloseErr := closeReaderIfPossible(r)

	var cause error
	if uint64(n) != fh.CompressedSize64 {
		cause = errors.Join(cause, fmt.Errorf("%w: copied=%d header=%d", errRawCopySizeMismatch, n, fh.CompressedSize64))
	}
	cause = errors.Join(cause, copyErr, readCloseErr)
	if cause != nil {
		w.latchFailure(cause)
		return cause
	}
	return nil
}

// AddFS adds files from fsys to the archive while preserving the directory tree.
func (w *Writer) AddFS(fsys fs.FS) error {
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
			return errors.New("arj: cannot add non-regular file")
		}

		h, err := FileInfoHeader(info)
		if err != nil {
			return err
		}
		h.Name = name
		if d.IsDir() {
			h.Name += "/"
		} else {
			h.Method = Method1
		}

		fw, err := w.CreateHeader(h)
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		f, err := fsys.Open(name)
		if err != nil {
			cause := abortStagedWriter(fw, err)
			w.latchFailure(cause)
			return cause
		}
		_, copyErr := io.Copy(fw, f)
		sourceCloseErr := f.Close()
		if cause := errors.Join(copyErr, sourceCloseErr); cause != nil {
			cause = abortStagedWriter(fw, cause)
			w.latchFailure(cause)
			return cause
		}
		return nil
	})
}
