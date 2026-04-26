package arj

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrInvalidMultiVolumeSize indicates MultiVolumeWriterOptions.VolumeSize
	// is zero or negative.
	ErrInvalidMultiVolumeSize = errors.New("arj: invalid multi-volume size")
	// ErrInvalidMultiVolumePath indicates a multi-volume writer target path
	// does not resolve to a valid first-volume base path.
	ErrInvalidMultiVolumePath = errors.New("arj: multi-volume archive path must be extensionless or end with .arj")
	// ErrRawEntryTooLargeForVolume indicates a raw entry does not fit into a
	// single output volume.
	ErrRawEntryTooLargeForVolume = errors.New("arj: raw entry does not fit in a single volume")

	errInvalidMultiVolumeSize = ErrInvalidMultiVolumeSize
	errMultiVolumePath        = ErrInvalidMultiVolumePath
	errVolumeTooSmall         = errors.New("arj: volume size too small")
	errNoSegmentFit           = errors.New("arj: segment does not fit current volume")
	errCompressedProbeLimit   = errors.New("arj: compressed probe output limit exceeded")
	errRawCopySizeMismatch    = errors.New("arj: raw copy byte count mismatch")
	errRawPayloadSizeMismatch = errors.New("arj: raw payload size does not match local header compressed size")
	errRawStoreSizeMismatch   = errors.New("arj: raw store payload size does not match local header uncompressed size")
	checksumScratchPool       = sync.Pool{
		New: func() any {
			return make([]byte, 32<<10)
		},
	}
	compressedProbeCopyBufferPool = sync.Pool{
		New: func() any {
			return make([]byte, 32<<10)
		},
	}
)

const (
	maxCompressedChunkExhaustiveThreshold = 2048
	maxCompressedChunkProbeBudget         = 48
	maxCompressedChunkLocalRefineWindow   = 96
	maxCompressedChunkNativeRefineWindow  = 8 << 10
	multiVolumeCompressedPendingLimit     = DefaultMaxPlainEntryBufferSize
)

// MultiVolumeWriterOptions configures NewMultiVolumeWriter.
type MultiVolumeWriterOptions struct {
	// VolumeSize limits each output part size in bytes, including ARJ headers.
	VolumeSize int64

	// FileMode controls permissions for created part files.
	// If zero, 0o600 is used.
	FileMode fs.FileMode

	// BufferLimits overrides in-memory write-path buffering limits.
	// Any zero field keeps the corresponding package default.
	BufferLimits WriteBufferLimits
}

// MultiVolumeWriter emits split ARJ volumes (.arj, .a01, .a02, ...).
type MultiVolumeWriter struct {
	firstPath     string
	stem          string
	partExtPrefix string
	volumeSize    int64
	fileMode      fs.FileMode

	current           *Writer
	currentFile       *os.File
	currentHasEntries bool
	nextPart          int
	paths             []string

	last         multiVolumeEntryWriter
	closed       bool
	failed       error
	writeStarted atomic.Bool

	cfgMu        sync.RWMutex
	compressors  map[uint16]Compressor
	compressorV  atomic.Value // map[uint16]Compressor
	archiveName  string
	comment      string
	archiveHdr   *ArchiveHeader
	defaultTime  time.Time
	bufferLimit  WriteBufferLimits
	bufferLimitV atomic.Value // WriteBufferLimits
}

type multiVolumeEntryWriter interface {
	close() error
	isClosed() bool
}

type writeErrLatcher interface {
	latchWriteErr(error)
}

// NewMultiVolumeWriter creates a writer that emits split ARJ volume files.
func NewMultiVolumeWriter(name string, opts MultiVolumeWriterOptions) (*MultiVolumeWriter, error) {
	if opts.VolumeSize <= 0 {
		return nil, errInvalidMultiVolumeSize
	}

	first, stem, prefix, err := normalizeMultiVolumePath(name)
	if err != nil {
		return nil, err
	}

	mode := opts.FileMode
	if mode == 0 {
		mode = 0o600
	} else {
		mode = mode.Perm()
		if mode == 0 {
			mode = 0o600
		}
	}

	limits := normalizeWriteBufferLimits(opts.BufferLimits)
	now := time.Now().UTC()
	writer := &MultiVolumeWriter{
		firstPath:     first,
		stem:          stem,
		partExtPrefix: prefix,
		volumeSize:    opts.VolumeSize,
		fileMode:      mode,
		bufferLimit:   limits,
		defaultTime:   now,
	}
	writer.compressorV.Store((map[uint16]Compressor)(nil))
	writer.bufferLimitV.Store(limits)
	return writer, nil
}

// Parts returns output part paths in deterministic write order.
func (w *MultiVolumeWriter) Parts() []string {
	out := make([]string, len(w.paths))
	copy(out, w.paths)
	return out
}

// Flush flushes any buffered data to the active volume file.
func (w *MultiVolumeWriter) Flush() error {
	if w.failed != nil {
		return w.failed
	}
	if w.last != nil && !w.last.isClosed() {
		if flusher, ok := w.last.(interface{ flush() error }); ok {
			if err := flusher.flush(); err != nil {
				w.latchFailure(err)
				return err
			}
		}
	}
	if w.current == nil {
		return nil
	}
	if err := w.current.Flush(); err != nil {
		w.latchFailure(err)
		return err
	}
	return nil
}

// SetComment sets the archive-level comment for every emitted volume.
func (w *MultiVolumeWriter) SetComment(comment string) error {
	w.cfgMu.Lock()
	defer w.cfgMu.Unlock()

	if w.writeStarted.Load() {
		return errors.New("arj: MultiVolumeWriter.SetComment called after archive output started")
	}

	if w.archiveHdr != nil {
		h := cloneArchiveHeader(*w.archiveHdr)
		h.Comment = comment
		if err := validateMainHeaderLengths(&h); err != nil {
			return err
		}
		w.archiveHdr = &h
	} else {
		h := ArchiveHeader{
			FirstHeaderSize: arjMinFirstHeaderSize,
			Name:            w.archiveName,
			Comment:         comment,
		}
		if err := validateMainHeaderLengths(&h); err != nil {
			return err
		}
	}

	w.comment = comment
	return nil
}

// SetArchiveName sets the archive name for every emitted volume.
func (w *MultiVolumeWriter) SetArchiveName(name string) error {
	w.cfgMu.Lock()
	defer w.cfgMu.Unlock()

	if w.writeStarted.Load() {
		return errors.New("arj: MultiVolumeWriter.SetArchiveName called after archive output started")
	}

	if w.archiveHdr != nil {
		h := cloneArchiveHeader(*w.archiveHdr)
		h.Name = name
		if err := validateMainHeaderLengths(&h); err != nil {
			return err
		}
		w.archiveHdr = &h
	} else {
		h := ArchiveHeader{
			FirstHeaderSize: arjMinFirstHeaderSize,
			Name:            name,
			Comment:         w.comment,
		}
		if err := validateMainHeaderLengths(&h); err != nil {
			return err
		}
	}

	w.archiveName = name
	return nil
}

// SetArchiveHeader sets the main header model for every emitted volume.
func (w *MultiVolumeWriter) SetArchiveHeader(hdr *ArchiveHeader) error {
	w.cfgMu.Lock()
	defer w.cfgMu.Unlock()

	if w.writeStarted.Load() {
		return errors.New("arj: MultiVolumeWriter.SetArchiveHeader called after archive output started")
	}
	if hdr == nil {
		return errors.New("arj: nil ArchiveHeader")
	}

	h := cloneArchiveHeader(*hdr)
	if h.FirstHeaderSize == 0 {
		h.FirstHeaderSize = arjMinFirstHeaderSize
	}
	syncArchiveHeaderExtMetadata(&h)
	if h.FileType == 0 {
		h.FileType = arjFileTypeMain
	}
	if err := normalizeMainFirstHeaderExtra(&h); err != nil {
		return err
	}
	syncArchiveHeaderSecurityMetadata(&h)
	if err := validateMainHeader(&h); err != nil {
		return err
	}

	w.archiveName = h.Name
	w.comment = h.Comment
	w.archiveHdr = &h
	return nil
}

// RegisterCompressor registers or overrides a custom compressor for a
// specific method ID. If missing, Writer defaults to package registrations.
// Passing nil explicitly disables the method on this writer instance.
func (w *MultiVolumeWriter) RegisterCompressor(method uint16, comp Compressor) {
	w.cfgMu.Lock()
	defer w.cfgMu.Unlock()

	next := cloneCompressorOverrides(w.compressors)
	if next == nil {
		next = make(map[uint16]Compressor)
	}
	next[method] = comp
	w.compressors = next
	w.compressorV.Store(next)
}

// SetBufferLimits overrides write-path buffering limits for future entries.
// Any zero field keeps the corresponding package default.
func (w *MultiVolumeWriter) SetBufferLimits(limits WriteBufferLimits) {
	limits = normalizeWriteBufferLimits(limits)

	w.cfgMu.Lock()
	w.bufferLimit = limits
	w.bufferLimitV.Store(limits)
	w.cfgMu.Unlock()
}

func (w *MultiVolumeWriter) compressor(method uint16) Compressor {
	if snapshot := w.compressorSnapshot(); snapshot != nil {
		if comp, ok := snapshot[method]; ok {
			return comp
		}
	}
	return compressor(method)
}

// Create adds a file to the archive set using Method4 compression.
func (w *MultiVolumeWriter) Create(name string) (io.Writer, error) {
	return w.CreateHeader(&FileHeader{Name: name, Method: Method4})
}

// CreateHeader adds a file to the archive set using the provided FileHeader.
func (w *MultiVolumeWriter) CreateHeader(fh *FileHeader) (io.Writer, error) {
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
	if h.Method == 0 {
		h.Method = Store
	}
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
	if h.Modified.IsZero() {
		h.Modified = time.Now().UTC()
	}
	h.modifiedDOS = timeToDosDateTime(h.Modified)

	if h.isDir() {
		h.fileType = arjFileTypeDirectory
		if !strings.HasSuffix(h.Name, "/") {
			h.Name += "/"
		}
	} else if h.fileType == arjFileTypeMain {
		h.fileType = arjFileTypeBinary
	}

	if h.fileMode == 0 {
		if h.isDir() {
			h.fileMode = uint16(fileModeToUnixMode(fs.ModeDir | 0o755))
		} else {
			h.fileMode = uint16(fileModeToUnixMode(0o644))
		}
	}

	if err := validateLocalHeaderLengths(&h); err != nil {
		return nil, err
	}
	if err := validateLocalExtendedHeaders(&h); err != nil {
		return nil, err
	}
	comp := w.compressor(h.Method)
	if comp == nil {
		return nil, ErrAlgorithm
	}
	limits := w.writeBufferLimits()

	if h.Method == Store && !h.isDir() {
		sw := &multiVolumeStoreFileWriter{
			w: w,
			h: &h,
		}
		w.last = sw
		return sw, nil
	}

	if !h.isDir() {
		cw := &multiVolumeCompressedFileWriter{
			w:                  w,
			h:                  &h,
			compressor:         comp,
			method14InputLimit: limits.MaxMethod14InputBufferSize,
			pendingLimit:       multiVolumeCompressedPendingLimit,
		}
		w.last = cw
		return cw, nil
	}

	fw := &multiVolumeFileWriter{w: w, h: &h}
	fw.entryBufferLimit = limits.MaxPlainEntryBufferSize
	fw.method14InputLimit = limits.MaxMethod14InputBufferSize
	fw.compressor = comp
	fw.plain = newEntryBuffer(fw.entryBufferLimit, bufferScopeMultiEntryPlain)
	w.last = fw
	return fw, nil
}

// CreateRaw adds a file to the archive set by writing caller-provided raw
// bytes and local-header metadata without compression.
//
// Constraint: raw entries must fit in a single output volume. The fit check
// runs when the returned writer is closed, after payload size is known.
func (w *MultiVolumeWriter) CreateRaw(fh *FileHeader) (io.Writer, error) {
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

	fw := &multiVolumeRawFileWriter{w: w, h: &h}
	limits := w.writeBufferLimits()
	fw.entryBufferLimit = limits.MaxCompressedEntryBufferSize
	fw.raw = newEntryBuffer(fw.entryBufferLimit, bufferScopeWriterEntryCompressed)
	w.last = fw
	return fw, nil
}

func closeReaderIfPossible(r io.Reader) error {
	if closer, ok := r.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func closeStagedMultiVolumeEntryWriter(fw io.Writer) error {
	if closer, ok := fw.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

func abortStagedMultiVolumeEntryWriter(fw io.Writer, cause error) error {
	if cause == nil {
		return closeStagedMultiVolumeEntryWriter(fw)
	}
	if latcher, ok := fw.(writeErrLatcher); ok {
		latcher.latchWriteErr(cause)
	}
	return errors.Join(cause, closeStagedMultiVolumeEntryWriter(fw))
}

// Copy copies f into w by copying raw bytes and local-header metadata.
//
// Constraint: entries reconstructed from multiple continuation segments are
// rejected, and accepted raw copies must fit into one output volume.
func (w *MultiVolumeWriter) Copy(f *File) error {
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
	if n != int64(fh.CompressedSize64) {
		cause = errors.Join(cause, fmt.Errorf("%w: copied=%d header=%d", errRawCopySizeMismatch, n, fh.CompressedSize64))
	}
	cause = errors.Join(cause, copyErr, readCloseErr)
	if cause != nil {
		return abortStagedMultiVolumeEntryWriter(fw, cause)
	}
	return closeStagedMultiVolumeEntryWriter(fw)
}

// AddFS adds files from fsys to the archive set while preserving the
// directory tree.
func (w *MultiVolumeWriter) AddFS(fsys fs.FS) error {
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
			h.Method = Method4
		}

		fw, err := w.CreateHeader(h)
		if err != nil {
			return err
		}
		if d.IsDir() {
			return closeStagedMultiVolumeEntryWriter(fw)
		}

		f, err := fsys.Open(name)
		if err != nil {
			return abortStagedMultiVolumeEntryWriter(fw, err)
		}
		_, copyErr := io.Copy(fw, f)
		sourceCloseErr := f.Close()
		if cause := errors.Join(copyErr, sourceCloseErr); cause != nil {
			return abortStagedMultiVolumeEntryWriter(fw, cause)
		}
		return closeStagedMultiVolumeEntryWriter(fw)
	})
}

func (w *MultiVolumeWriter) prepare() error {
	if w.failed != nil {
		return w.failed
	}
	if w.closed {
		return errors.New("arj: write to closed writer")
	}
	if w.last != nil && !w.last.isClosed() {
		if err := w.last.close(); err != nil {
			if w.failed != nil {
				return w.failed
			}
			return err
		}
	}
	return nil
}

func (w *MultiVolumeWriter) writeBufferLimits() WriteBufferLimits {
	if snapshot := w.bufferLimitSnapshot(); snapshot != (WriteBufferLimits{}) {
		return snapshot
	}
	return normalizeWriteBufferLimits(WriteBufferLimits{})
}

// Close flushes all pending data and closes active output part files.
func (w *MultiVolumeWriter) Close() error {
	if w.failed != nil {
		if w.last != nil && !w.last.isClosed() {
			_ = w.last.close()
		}
		if w.current != nil {
			_ = w.closeCurrentVolume(false)
		}
		return w.failed
	}
	if w.last != nil && !w.last.isClosed() {
		if err := w.last.close(); err != nil {
			if w.failed != nil {
				return w.failed
			}
			closeErr := w.closeCurrentVolume(false)
			w.closed = true
			w.last = nil
			if closeErr != nil {
				return errors.Join(err, closeErr)
			}
			return err
		}
	}
	if w.closed {
		return errors.New("arj: writer closed twice")
	}
	w.closed = true

	if w.current == nil {
		if err := w.openNextVolume(); err != nil {
			if w.failed != nil {
				return w.failed
			}
			return err
		}
	}
	if err := w.closeCurrentVolume(false); err != nil {
		if w.failed != nil {
			return w.failed
		}
		return err
	}
	return nil
}

type multiVolumeFileWriter struct {
	w                  *MultiVolumeWriter
	h                  *FileHeader
	plain              *entryBuffer
	plainN             uint64
	entryBufferLimit   uint64
	method14InputLimit uint64
	compressor         Compressor
	writeErr           error
	closed             bool
}

func (w *multiVolumeFileWriter) latchWriteErr(err error) {
	if err == nil || w.writeErr != nil {
		return
	}
	w.writeErr = err
}

func (w *multiVolumeFileWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, errors.New("arj: write to closed file")
	}
	if w.writeErr != nil {
		return 0, w.writeErr
	}

	if w.plainN > maxARJFileSize {
		w.latchWriteErr(errFileTooLarge)
		return 0, w.writeErr
	}

	remaining := maxARJFileSize - w.plainN
	limited := false
	chunk := p
	if uint64(len(p)) > remaining {
		if remaining == 0 {
			w.latchWriteErr(errFileTooLarge)
			return 0, w.writeErr
		}
		chunk = p[:int(remaining)]
		limited = true
	}

	n, err := w.plain.Write(chunk)
	w.plainN += uint64(n)
	if err != nil {
		w.latchWriteErr(err)
	}

	sizeExceeded := false
	if limited && err == nil && n == len(chunk) {
		sizeExceeded = true
	}
	if w.plainN > maxARJFileSize {
		sizeExceeded = true
	}
	if sizeExceeded {
		sizeErr := errFileTooLarge
		w.latchWriteErr(sizeErr)
		if err == nil {
			err = sizeErr
		} else if !errors.Is(err, sizeErr) {
			err = errors.Join(err, sizeErr)
		}
	}

	return n, err
}

func (w *multiVolumeFileWriter) Close() error {
	return w.close()
}

func (w *multiVolumeFileWriter) isClosed() bool {
	return w.closed
}

func (w *multiVolumeFileWriter) close() (err error) {
	if w.closed {
		return nil
	}
	w.closed = true
	defer func() {
		if cleanupErr := w.plain.Close(); cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
	}()

	if w.h.isDir() && w.plainN != 0 {
		return errDirectoryFileData
	}
	if w.writeErr != nil {
		return w.writeErr
	}

	if err := w.w.writeEntry(w); err != nil {
		return err
	}
	w.w.last = nil
	return nil
}

type multiVolumeStoreSegment struct {
	header       FileHeader
	headerOffset int64
	plainN       uint64
	crc          uint32
}

type multiVolumeCompressedSegment struct {
	header       FileHeader
	headerOffset int64
	file         *os.File
}

type multiVolumeCompressedFileWriter struct {
	w                  *MultiVolumeWriter
	h                  *FileHeader
	compressor         Compressor
	method14InputLimit uint64
	plainN             uint64
	resumePos          uint64
	continued          bool
	lastSegment        *multiVolumeCompressedSegment
	pending            *entryBuffer
	pendingOff         uint64
	pendingLimit       uint64
	writeErr           error
	closed             bool
}

func (w *multiVolumeCompressedFileWriter) latchWriteErr(err error) {
	if err == nil || w.writeErr != nil {
		return
	}
	w.writeErr = err
}

func (w *multiVolumeCompressedFileWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, errors.New("arj: write to closed file")
	}
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	if w.plainN > maxARJFileSize {
		w.latchWriteErr(errFileTooLarge)
		return 0, w.writeErr
	}

	total := 0
	for len(p) > 0 {
		if w.plainN >= maxARJFileSize {
			sizeErr := errFileTooLarge
			w.latchWriteErr(sizeErr)
			return total, sizeErr
		}
		if err := w.compactPending(); err != nil {
			w.latchWriteErr(err)
			return total, err
		}
		if isNativeMethod14(w.h.Method) && w.pendingUnread() != 0 && w.pendingUnread()+uint64(len(p)) > w.method14InputLimit {
			if err := w.flushPending(true); err != nil {
				w.latchWriteErr(err)
				return total, err
			}
			continue
		}

		chunkN := len(p)
		remaining := maxARJFileSize - w.plainN
		if uint64(chunkN) > remaining {
			chunkN = int(remaining)
		}
		pendingRoom := w.pendingLimitOrDefault() - w.pendingUnread()
		if pendingRoom == 0 {
			if err := w.flushPendingForWrite(); err != nil {
				w.latchWriteErr(err)
				return total, err
			}
			continue
		}
		if uint64(chunkN) > pendingRoom {
			chunkN = int(pendingRoom)
		}
		if isNativeMethod14(w.h.Method) && w.pendingUnread() == 0 && uint64(chunkN) > w.method14InputLimit {
			limitErr := w.method14InputLimitErr(chunkN)
			w.latchWriteErr(limitErr)
			return total, limitErr
		}
		if chunkN <= 0 {
			sizeErr := errFileTooLarge
			w.latchWriteErr(sizeErr)
			return total, sizeErr
		}

		consumed, err := w.writePending(p[:chunkN])
		if consumed > 0 {
			w.plainN += uint64(consumed)
			total += consumed
			p = p[consumed:]
		}
		if err != nil {
			w.latchWriteErr(err)
			return total, err
		}
		if consumed < chunkN {
			w.latchWriteErr(io.ErrShortWrite)
			return total, io.ErrShortWrite
		}
		if err := w.flushPendingForWrite(); err != nil {
			w.latchWriteErr(err)
			return total, err
		}
	}

	return total, nil
}

func (w *multiVolumeCompressedFileWriter) flush() error {
	if w.closed {
		return nil
	}
	if w.writeErr != nil {
		return w.writeErr
	}
	if err := w.flushPending(true); err != nil {
		w.latchWriteErr(err)
		return err
	}
	return nil
}

func (w *multiVolumeCompressedFileWriter) flushPendingForWrite() error {
	for {
		unread := w.pendingUnread()
		if unread == 0 {
			return nil
		}
		force := unread >= w.pendingLimitOrDefault()
		if isNativeMethod14(w.h.Method) && unread > w.method14InputLimit {
			force = true
		}
		if !force {
			maxComp, err := w.prospectiveSegmentMaxComp()
			if err != nil {
				return err
			}
			if maxComp > 0 && unread < uint64(maxComp) {
				return nil
			}
		}

		wrote, err := w.writePendingSegment(force)
		if err != nil {
			return err
		}
		if !wrote {
			return nil
		}
	}
}

func (w *multiVolumeCompressedFileWriter) flushPending(force bool) error {
	for w.pendingUnread() > 0 {
		wrote, err := w.writePendingSegment(force)
		if err != nil {
			return err
		}
		if !wrote {
			if force {
				return io.ErrShortWrite
			}
			return nil
		}
	}
	return nil
}

func (w *multiVolumeCompressedFileWriter) writePendingSegment(force bool) (bool, error) {
	unread := w.pendingUnread()
	if unread == 0 {
		return false, nil
	}

	if !force {
		maxComp, err := w.prospectiveSegmentMaxComp()
		if err != nil {
			return false, err
		}
		if maxComp > 0 {
			n, _, _, err := w.selectPendingSegment(maxComp)
			if err != nil {
				return false, err
			}
			if uint64(n) == unread {
				return false, nil
			}
		}
	}

	for {
		if w.lastSegment != nil {
			if err := w.w.closeCurrentVolume(true); err != nil {
				return false, err
			}
			w.lastSegment = nil
		}
		if err := w.w.ensureCurrentVolume(); err != nil {
			return false, err
		}

		h, err := w.segmentHeaderTemplate(true)
		if err != nil {
			return false, err
		}
		overhead, err := rawSegmentOverhead(&h)
		if err != nil {
			return false, err
		}

		maxComp := w.w.currentRemaining() - int64(overhead)
		if maxComp <= 0 {
			if !w.w.currentHasEntries {
				return false, w.w.volumeTooSmallOnEmptyCurrent()
			}
			w.lastSegment = nil
			if err := w.w.closeCurrentVolume(true); err != nil {
				return false, err
			}
			continue
		}

		n, comp, crc, err := w.selectPendingSegment(maxComp)
		if err != nil {
			return false, err
		}
		if n == 0 {
			if !w.w.currentHasEntries {
				return false, w.w.volumeTooSmallOnEmptyCurrent()
			}
			w.lastSegment = nil
			if err := w.w.closeCurrentVolume(true); err != nil {
				return false, err
			}
			continue
		}
		if uint64(n) > unread {
			return false, io.ErrUnexpectedEOF
		}
		if !force && uint64(n) == unread {
			return false, nil
		}

		if err := w.writeSegment(h, uint64(n), comp, crc); err != nil {
			return false, err
		}
		w.pendingOff += uint64(n)
		if err := w.closePendingIfDrained(); err != nil {
			return true, err
		}
		return true, nil
	}
}

func (w *multiVolumeCompressedFileWriter) writePending(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if w.pending == nil {
		w.pending = newEntryBuffer(w.pendingLimitOrDefault(), bufferScopeMultiEntryPlain)
	}
	return w.pending.Write(p)
}

func (w *multiVolumeCompressedFileWriter) compactPending() error {
	if w.pending == nil || w.pendingOff == 0 {
		return nil
	}

	unread := w.pendingUnread()
	if unread == 0 {
		return w.closePending()
	}

	next := newEntryBuffer(w.pendingLimitOrDefault(), bufferScopeMultiEntryPlain)
	_, copyErr := io.CopyN(next, io.NewSectionReader(w.pending, int64(w.pendingOff), int64(unread)), int64(unread))
	closeErr := w.pending.Close()
	if copyErr != nil {
		return errors.Join(copyErr, closeErr)
	}
	w.pending = next
	w.pendingOff = 0
	return closeErr
}

func (w *multiVolumeCompressedFileWriter) closePendingIfDrained() error {
	if w.pendingUnread() != 0 {
		return nil
	}
	return w.closePending()
}

func (w *multiVolumeCompressedFileWriter) closePending() error {
	if w.pending == nil {
		w.pendingOff = 0
		return nil
	}
	err := w.pending.Close()
	w.pending = nil
	w.pendingOff = 0
	return err
}

func (w *multiVolumeCompressedFileWriter) pendingUnread() uint64 {
	if w.pending == nil {
		return 0
	}
	size := w.pending.Size()
	if w.pendingOff >= size {
		return 0
	}
	return size - w.pendingOff
}

func (w *multiVolumeCompressedFileWriter) pendingLimitOrDefault() uint64 {
	if w.pendingLimit != 0 {
		return w.pendingLimit
	}
	return multiVolumeCompressedPendingLimit
}

func (w *multiVolumeCompressedFileWriter) method14InputLimitErr(attempted int) *BufferLimitError {
	buffered := w.pendingUnread()
	if buffered < w.method14InputLimit {
		buffered = w.method14InputLimit
	}
	return &BufferLimitError{
		Scope:     bufferScopeMethod14Input,
		Limit:     w.method14InputLimit,
		Buffered:  buffered,
		Attempted: uint64(attempted),
	}
}

func (w *multiVolumeCompressedFileWriter) prospectiveSegmentMaxComp() (int64, error) {
	h, err := w.segmentHeaderTemplate(true)
	if err != nil {
		return 0, err
	}
	overhead, err := rawSegmentOverhead(&h)
	if err != nil {
		return 0, err
	}

	remaining := w.w.currentRemaining()
	if w.lastSegment != nil || w.w.current == nil {
		freshRemaining, err := w.w.freshVolumeRemaining()
		if err != nil {
			return 0, err
		}
		remaining = freshRemaining
	}
	return remaining - int64(overhead), nil
}

func (w *multiVolumeCompressedFileWriter) selectPendingSegment(maxComp int64) (int, []byte, uint32, error) {
	unread := w.pendingUnread()
	if unread == 0 {
		return 0, nil, crc32.ChecksumIEEE(nil), nil
	}
	maxInt := uint64(^uint(0) >> 1)
	if unread > maxInt {
		unread = maxInt
	}
	return w.w.maxCompressedChunkBufferedWithCompressorAndCRC(
		w.h.Method,
		w.pending,
		int64(w.pendingOff),
		int(unread),
		maxComp,
		w.compressor,
		w.method14InputLimit,
	)
}

func (w *multiVolumeCompressedFileWriter) writeSegment(base FileHeader, plainSize uint64, comp []byte, crc uint32) error {
	if plainSize > maxARJFileSize || uint64(len(comp)) > maxARJFileSize {
		return errFileTooLarge
	}

	h := base
	h.UncompressedSize64 = plainSize
	h.CompressedSize64 = uint64(len(comp))
	h.CRC32 = crc
	if plainSize == 0 {
		h.CRC32 = crc32.ChecksumIEEE(nil)
	}
	h.modifiedDOS = timeToDosDateTime(h.Modified)
	syncFileHeaderExtMetadata(&h)

	headerOffset := w.w.current.cw.Count()
	if err := writeLocalFileHeader(w.w.current.cw, &h); err != nil {
		w.w.latchFailure(err)
		return err
	}
	if len(comp) != 0 {
		if _, err := writeAll(w.w.current.cw, comp); err != nil {
			w.w.latchFailure(err)
			return err
		}
	}
	w.w.currentHasEntries = true
	w.resumePos += plainSize
	w.continued = true
	w.lastSegment = &multiVolumeCompressedSegment{
		header:       h,
		headerOffset: headerOffset,
		file:         w.w.currentFile,
	}
	return nil
}

func (w *multiVolumeCompressedFileWriter) segmentHeaderTemplate(hasMore bool) (FileHeader, error) {
	h := cloneFileHeader(*w.h)
	h.Flags &^= (FlagVolume | FlagExtFile)
	if hasMore {
		h.Flags |= FlagVolume
	}
	if w.continued {
		h.Flags |= FlagExtFile
		if err := prepareContinuationHeader(&h, w.resumePos); err != nil {
			return FileHeader{}, err
		}
	}
	h.UncompressedSize64 = 0
	h.CompressedSize64 = 0
	h.CRC32 = 0
	return h, nil
}

func (w *multiVolumeCompressedFileWriter) Close() error {
	return w.close()
}

func (w *multiVolumeCompressedFileWriter) isClosed() bool {
	return w.closed
}

func (w *multiVolumeCompressedFileWriter) close() (err error) {
	if w.closed {
		return nil
	}
	w.closed = true
	defer func() {
		if closeErr := w.closePending(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	if w.writeErr != nil {
		if w.plainN != 0 || w.lastSegment != nil {
			w.w.latchFailure(w.writeErr)
		}
		return w.writeErr
	}
	if w.plainN == 0 {
		if err := w.emitEmptySegment(); err != nil {
			return err
		}
	}
	if err := w.flushPending(true); err != nil {
		w.w.latchFailure(err)
		return err
	}
	if w.lastSegment == nil {
		return ErrFormat
	}
	if w.lastSegment.file == nil {
		return io.ErrClosedPipe
	}

	finalHeader := cloneFileHeader(w.lastSegment.header)
	finalHeader.Flags &^= FlagVolume
	finalHeader.modifiedDOS = timeToDosDateTime(finalHeader.Modified)
	syncFileHeaderExtMetadata(&finalHeader)

	if err := w.w.Flush(); err != nil {
		w.w.latchFailure(err)
		return err
	}
	if err := patchLocalFileHeader(w.lastSegment.file, w.lastSegment.headerOffset, &finalHeader); err != nil {
		w.w.latchFailure(err)
		return err
	}

	w.w.last = nil
	return nil
}

func (w *multiVolumeCompressedFileWriter) emitEmptySegment() error {
	for {
		if err := w.w.ensureCurrentVolume(); err != nil {
			return err
		}
		h, err := w.segmentHeaderTemplate(false)
		if err != nil {
			return err
		}
		overhead, err := rawSegmentOverhead(&h)
		if err != nil {
			return err
		}
		maxComp := w.w.currentRemaining() - int64(overhead)
		if maxComp < 0 {
			if !w.w.currentHasEntries {
				return w.w.volumeTooSmallOnEmptyCurrent()
			}
			w.lastSegment = nil
			if err := w.w.closeCurrentVolume(true); err != nil {
				return err
			}
			continue
		}
		comp, fit, crc, err := w.w.compressDataFromReaderWithCompressorAndCRC(
			h.Method,
			bytes.NewReader(nil),
			maxComp,
			w.compressor,
			w.method14InputLimit,
		)
		if err != nil {
			return err
		}
		if !fit {
			if !w.w.currentHasEntries {
				return w.w.volumeTooSmallOnEmptyCurrent()
			}
			w.lastSegment = nil
			if err := w.w.closeCurrentVolume(true); err != nil {
				return err
			}
			continue
		}
		return w.writeSegment(h, 0, comp, crc)
	}
}

type multiVolumeStoreFileWriter struct {
	w         *MultiVolumeWriter
	h         *FileHeader
	plainN    uint64
	resumePos uint64
	continued bool
	segment   *multiVolumeStoreSegment
	writeErr  error
	closed    bool
}

func (w *multiVolumeStoreFileWriter) latchWriteErr(err error) {
	if err == nil || w.writeErr != nil {
		return
	}
	w.writeErr = err
}

func (w *multiVolumeStoreFileWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, errors.New("arj: write to closed file")
	}
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	if w.plainN > maxARJFileSize {
		w.latchWriteErr(errFileTooLarge)
		return 0, w.writeErr
	}

	total := 0
	for len(p) > 0 {
		if w.segment == nil {
			if err := w.openSegment(true); err != nil {
				w.latchWriteErr(err)
				return total, err
			}
		}

		if w.plainN >= maxARJFileSize {
			sizeErr := errFileTooLarge
			w.latchWriteErr(sizeErr)
			return total, sizeErr
		}

		volumeRemaining := w.w.currentRemaining()
		if volumeRemaining <= 0 {
			if err := w.finishSegment(true); err != nil {
				w.latchWriteErr(err)
				return total, err
			}
			if err := w.w.closeCurrentVolume(true); err != nil {
				w.latchWriteErr(err)
				return total, err
			}
			continue
		}

		chunkN := len(p)
		fileRemaining := maxARJFileSize - w.plainN
		if uint64(chunkN) > fileRemaining {
			chunkN = int(fileRemaining)
		}
		if int64(chunkN) > volumeRemaining {
			chunkN = int(volumeRemaining)
		}
		if chunkN <= 0 {
			sizeErr := errFileTooLarge
			w.latchWriteErr(sizeErr)
			return total, sizeErr
		}

		n, err := w.w.current.cw.Write(p[:chunkN])
		if n > 0 {
			w.segment.crc = crc32.Update(w.segment.crc, crc32.IEEETable, p[:n])
			w.segment.plainN += uint64(n)
			w.plainN += uint64(n)
			total += n
			p = p[n:]
		}
		if err != nil {
			w.latchWriteErr(err)
			return total, err
		}
		if n < chunkN {
			w.latchWriteErr(io.ErrShortWrite)
			return total, io.ErrShortWrite
		}

		if len(p) == 0 {
			break
		}

		if w.w.currentRemaining() == 0 {
			if err := w.finishSegment(true); err != nil {
				w.latchWriteErr(err)
				return total, err
			}
			if err := w.w.closeCurrentVolume(true); err != nil {
				w.latchWriteErr(err)
				return total, err
			}
		}
	}

	return total, nil
}

func (w *multiVolumeStoreFileWriter) Close() error {
	return w.close()
}

func (w *multiVolumeStoreFileWriter) isClosed() bool {
	return w.closed
}

func (w *multiVolumeStoreFileWriter) close() (err error) {
	if w.closed {
		return nil
	}
	w.closed = true
	defer func() {
		if err != nil {
			w.w.latchFailure(err)
		}
	}()

	if w.writeErr != nil {
		return w.writeErr
	}

	if w.segment == nil {
		if err := w.openSegment(false); err != nil {
			return err
		}
	}
	if err := w.finishSegment(false); err != nil {
		return err
	}

	w.w.last = nil
	return nil
}

func (w *multiVolumeStoreFileWriter) openSegment(needPayload bool) error {
	h := cloneFileHeader(*w.h)
	h.Flags &^= (FlagVolume | FlagExtFile)
	if w.continued {
		h.Flags |= FlagExtFile
		if err := prepareContinuationHeader(&h, w.resumePos); err != nil {
			return err
		}
	}
	h.UncompressedSize64 = 0
	h.CompressedSize64 = 0
	h.CRC32 = 0

	for {
		if err := w.w.ensureCurrentVolume(); err != nil {
			return err
		}

		overhead, err := rawSegmentOverhead(&h)
		if err != nil {
			return err
		}
		remaining := w.w.currentRemaining()
		if int64(overhead) > remaining {
			if !w.w.currentHasEntries {
				return w.w.volumeTooSmallOnEmptyCurrent()
			}
			if err := w.w.closeCurrentVolume(true); err != nil {
				return err
			}
			continue
		}
		if needPayload && remaining-int64(overhead) <= 0 {
			if !w.w.currentHasEntries {
				return w.w.volumeTooSmallOnEmptyCurrent()
			}
			if err := w.w.closeCurrentVolume(true); err != nil {
				return err
			}
			continue
		}

		headerOffset := w.w.current.cw.Count()
		if err := writeLocalFileHeader(w.w.current.cw, &h); err != nil {
			w.w.latchFailure(err)
			return err
		}
		w.w.currentHasEntries = true
		w.segment = &multiVolumeStoreSegment{
			header:       h,
			headerOffset: headerOffset,
		}
		return nil
	}
}

func (w *multiVolumeStoreFileWriter) finishSegment(hasMore bool) error {
	if w.segment == nil {
		return nil
	}
	if w.w.currentFile == nil {
		return io.ErrClosedPipe
	}

	h := cloneFileHeader(w.segment.header)
	h.UncompressedSize64 = w.segment.plainN
	h.CompressedSize64 = w.segment.plainN
	if h.UncompressedSize64 > maxARJFileSize || h.CompressedSize64 > maxARJFileSize {
		return errFileTooLarge
	}
	h.CRC32 = w.segment.crc
	if w.segment.plainN == 0 {
		h.CRC32 = crc32.ChecksumIEEE(nil)
	}
	if hasMore {
		h.Flags |= FlagVolume
	} else {
		h.Flags &^= FlagVolume
	}
	h.modifiedDOS = timeToDosDateTime(h.Modified)
	syncFileHeaderExtMetadata(&h)

	if err := w.w.Flush(); err != nil {
		w.w.latchFailure(err)
		return err
	}
	if err := patchLocalFileHeader(w.w.currentFile, w.segment.headerOffset, &h); err != nil {
		w.w.latchFailure(err)
		return err
	}

	w.resumePos += w.segment.plainN
	if hasMore {
		w.continued = true
	}
	w.segment = nil
	return nil
}

type multiVolumeRawFileWriter struct {
	w                *MultiVolumeWriter
	h                *FileHeader
	raw              *entryBuffer
	rawN             uint64
	entryBufferLimit uint64
	writeErr         error
	closed           bool
}

func (w *multiVolumeRawFileWriter) latchWriteErr(err error) {
	if err == nil || w.writeErr != nil {
		return
	}
	w.writeErr = err
}

func (w *multiVolumeRawFileWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, errors.New("arj: write to closed file")
	}
	if w.writeErr != nil {
		return 0, w.writeErr
	}

	if w.rawN > maxARJFileSize {
		w.latchWriteErr(errFileTooLarge)
		return 0, w.writeErr
	}

	remaining := maxARJFileSize - w.rawN
	limited := false
	chunk := p
	if uint64(len(p)) > remaining {
		if remaining == 0 {
			w.latchWriteErr(errFileTooLarge)
			return 0, w.writeErr
		}
		chunk = p[:int(remaining)]
		limited = true
	}

	n, err := w.raw.Write(chunk)
	w.rawN += uint64(n)
	if err != nil {
		w.latchWriteErr(err)
	}

	sizeExceeded := false
	if limited && err == nil && n == len(chunk) {
		sizeExceeded = true
	}
	if w.rawN > maxARJFileSize {
		sizeExceeded = true
	}
	if sizeExceeded {
		sizeErr := errFileTooLarge
		w.latchWriteErr(sizeErr)
		if err == nil {
			err = sizeErr
		} else if !errors.Is(err, sizeErr) {
			err = errors.Join(err, sizeErr)
		}
	}

	return n, err
}

func (w *multiVolumeRawFileWriter) Close() error {
	return w.close()
}

func (w *multiVolumeRawFileWriter) isClosed() bool {
	return w.closed
}

func (w *multiVolumeRawFileWriter) close() (err error) {
	if w.closed {
		return nil
	}
	w.closed = true
	defer func() {
		if cleanupErr := w.raw.Close(); cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
	}()

	if w.h.isDir() && w.rawN != 0 {
		return errDirectoryFileData
	}
	if w.writeErr != nil {
		return w.writeErr
	}
	if err := validateRawEntrySizes(w.h, w.rawN); err != nil {
		return err
	}

	if err := w.w.writeRawEntry(w); err != nil {
		return err
	}
	w.w.last = nil
	return nil
}

func validateRawEntrySizes(h *FileHeader, payloadSize uint64) error {
	if h == nil || h.isDir() {
		return nil
	}

	var err error
	if payloadSize != h.CompressedSize64 {
		err = errors.Join(err, fmt.Errorf("%w: payload=%d header=%d", errRawPayloadSizeMismatch, payloadSize, h.CompressedSize64))
	}
	if h.Method == Store && payloadSize != h.UncompressedSize64 {
		err = errors.Join(err, fmt.Errorf("%w: payload=%d header=%d", errRawStoreSizeMismatch, payloadSize, h.UncompressedSize64))
	}
	return err
}

func (w *MultiVolumeWriter) writeRawEntry(rawEntry *multiVolumeRawFileWriter) error {
	h := cloneFileHeader(*rawEntry.h)
	raw := rawEntry.raw
	if h.isDir() {
		h.CompressedSize64 = 0
		h.UncompressedSize64 = 0
		h.CRC32 = 0
		raw = nil
	}

	rawSize := uint64(0)
	if raw != nil {
		rawSize = raw.Size()
	}
	if rawSize > maxARJFileSize {
		return errFileTooLarge
	}

	for {
		if err := w.ensureCurrentVolume(); err != nil {
			return err
		}

		overhead, err := rawSegmentOverhead(&h)
		if err != nil {
			return err
		}
		segmentSize := int64(overhead) + int64(rawSize)
		if segmentSize > w.currentRemaining() {
			if !w.currentHasEntries {
				return ErrRawEntryTooLargeForVolume
			}
			if err := w.closeCurrentVolume(true); err != nil {
				return err
			}
			continue
		}

		if err := writeLocalFileHeader(w.current.cw, &h); err != nil {
			w.latchFailure(err)
			return err
		}
		if rawSize != 0 {
			if _, err := raw.WriteTo(w.current.cw); err != nil {
				w.latchFailure(err)
				return err
			}
		}
		w.currentHasEntries = true
		return nil
	}
}

func (w *MultiVolumeWriter) writeEntry(entry *multiVolumeFileWriter) error {
	base := entry.h
	plain := entry.plain
	if base.isDir() {
		plain = nil
	}

	plainSize := uint64(0)
	if plain != nil {
		plainSize = plain.Size()
	}

	baseFlags := base.Flags &^ (FlagVolume | FlagExtFile)
	continued := false
	offset := uint64(0)

	for {
		if err := w.ensureCurrentVolume(); err != nil {
			return err
		}

		remainingCap := w.currentRemaining()
		chunkN, chunkComp, chunkCRC, chunkCRCKnown, err := w.selectSegmentData(entry, offset, continued, remainingCap)
		if errors.Is(err, errNoSegmentFit) {
			if !w.currentHasEntries {
				return w.volumeTooSmallOnEmptyCurrent()
			}
			if err := w.closeCurrentVolume(true); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}

		chunkNU64 := uint64(chunkN)
		hasMore := offset+chunkNU64 < plainSize
		flags := baseFlags
		if continued {
			flags |= FlagExtFile
		}
		if hasMore {
			flags |= FlagVolume
		}

		h := cloneFileHeader(*base)
		h.Flags = flags
		h.UncompressedSize64 = uint64(chunkN)
		h.CompressedSize64 = uint64(len(chunkComp))
		if h.Method == Store {
			h.CompressedSize64 = uint64(chunkN)
		}
		if chunkN > 0 {
			// Store segments still compute CRC from plain data because they do not
			// run through compressed probing.
			if chunkCRCKnown {
				h.CRC32 = chunkCRC
			} else {
				crc, err := checksumEntryBufferRange(plain, int64(offset), chunkN)
				if err != nil {
					return err
				}
				h.CRC32 = crc
			}
		} else {
			h.CRC32 = crc32.ChecksumIEEE(nil)
		}
		if h.isDir() {
			h.UncompressedSize64 = 0
			h.CompressedSize64 = 0
			h.CRC32 = 0
			chunkComp = nil
		}
		if err := prepareContinuationHeader(&h, offset); err != nil {
			return err
		}

		overhead, err := rawSegmentOverhead(&h)
		if err != nil {
			return err
		}
		payloadSize := int64(len(chunkComp))
		if !h.isDir() && h.Method == Store {
			payloadSize = int64(chunkN)
		}
		if int64(overhead)+payloadSize > w.currentRemaining() {
			if !w.currentHasEntries {
				return w.volumeTooSmallOnEmptyCurrent()
			}
			if err := w.closeCurrentVolume(true); err != nil {
				return err
			}
			continue
		}

		if err := writeLocalFileHeader(w.current.cw, &h); err != nil {
			w.latchFailure(err)
			return err
		}
		switch {
		case h.isDir():
		case h.Method == Store:
			if chunkN != 0 {
				r := io.NewSectionReader(plain, int64(offset), int64(chunkN))
				if _, err := io.CopyN(w.current.cw, r, int64(chunkN)); err != nil {
					w.latchFailure(err)
					return err
				}
			}
		default:
			if _, err := writeAll(w.current.cw, chunkComp); err != nil {
				w.latchFailure(err)
				return err
			}
		}
		w.currentHasEntries = true

		if !hasMore {
			return nil
		}

		offset += chunkNU64
		continued = true
		if err := w.closeCurrentVolume(true); err != nil {
			return err
		}
	}
}

func checksumEntryBufferRange(buf *entryBuffer, off int64, n int) (uint32, error) {
	if n == 0 {
		return crc32.ChecksumIEEE(nil), nil
	}
	if buf == nil {
		return 0, io.ErrUnexpectedEOF
	}

	sum := crc32.NewIEEE()
	scratch := checksumScratchPool.Get().([]byte)
	if len(scratch) == 0 {
		scratch = make([]byte, 32<<10)
	}
	defer func() {
		if cap(scratch) >= 32<<10 {
			checksumScratchPool.Put(scratch[:32<<10])
		}
	}()

	remaining := n
	for remaining > 0 {
		chunkN := len(scratch)
		if chunkN > remaining {
			chunkN = remaining
		}
		if err := readAtFull(buf, scratch[:chunkN], off); err != nil {
			return 0, err
		}
		_, _ = sum.Write(scratch[:chunkN])
		off += int64(chunkN)
		remaining -= chunkN
	}
	return sum.Sum32(), nil
}

func prepareContinuationHeader(h *FileHeader, resumePos uint64) error {
	if h.Flags&FlagExtFile == 0 {
		return nil
	}
	if resumePos > uint64(^uint32(0)) {
		return errFileTooLarge
	}

	if h.FirstHeaderSize < arjMinFirstHeaderSize+4 {
		h.FirstHeaderSize = arjMinFirstHeaderSize + 4
	}
	if err := normalizeLocalFirstHeaderExtra(h); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(h.firstHeaderExtra[:4], uint32(resumePos))
	return nil
}

func (w *MultiVolumeWriter) selectSegmentData(entry *multiVolumeFileWriter, offset uint64, continued bool, remainingCap int64) (int, []byte, uint32, bool, error) {
	if remainingCap <= 0 {
		return 0, nil, 0, false, errNoSegmentFit
	}

	base := entry.h
	plain := entry.plain

	h := cloneFileHeader(*base)
	h.Flags &^= FlagVolume | FlagExtFile
	if continued {
		h.Flags |= FlagExtFile
		if err := prepareContinuationHeader(&h, 0); err != nil {
			return 0, nil, 0, false, err
		}
	}
	overhead, err := rawSegmentOverhead(&h)
	if err != nil {
		return 0, nil, 0, false, err
	}
	maxComp := remainingCap - int64(overhead)
	if maxComp < 0 {
		return 0, nil, 0, false, errNoSegmentFit
	}

	if base.isDir() {
		return 0, nil, 0, false, nil
	}

	plainSize := uint64(0)
	if plain != nil {
		plainSize = plain.Size()
	}
	if plainSize < offset {
		return 0, nil, 0, false, io.ErrUnexpectedEOF
	}
	remainingPlain := plainSize - offset

	if remainingPlain == 0 {
		comp, err := w.compressDataWithCompressor(base.Method, nil, entry.compressor, entry.method14InputLimit)
		if err != nil {
			return 0, nil, 0, false, err
		}
		if int64(len(comp)) > maxComp {
			return 0, nil, 0, false, errNoSegmentFit
		}
		return 0, comp, crc32.ChecksumIEEE(nil), true, nil
	}

	if base.Method == Store {
		if maxComp <= 0 {
			return 0, nil, 0, false, errNoSegmentFit
		}
		n64 := int64(remainingPlain)
		if n64 > maxComp {
			n64 = maxComp
		}
		maxInt := int64(^uint(0) >> 1)
		if n64 > maxInt {
			n64 = maxInt
		}
		n := int(n64)
		if n <= 0 {
			return 0, nil, 0, false, errNoSegmentFit
		}
		return n, nil, 0, false, nil
	}

	remainingProbe := remainingPlain
	maxInt := uint64(^uint(0) >> 1)
	if remainingProbe > maxInt {
		remainingProbe = maxInt
	}
	n, comp, crc, err := w.maxCompressedChunkBufferedWithCompressorAndCRC(
		base.Method,
		plain,
		int64(offset),
		int(remainingProbe),
		maxComp,
		entry.compressor,
		entry.method14InputLimit,
	)
	if err != nil {
		return 0, nil, 0, false, err
	}
	if n == 0 {
		return 0, nil, 0, false, errNoSegmentFit
	}
	return n, comp, crc, true, nil
}

func (w *MultiVolumeWriter) maxCompressedChunk(method uint16, plain []byte, maxComp int64) (int, []byte, error) {
	if len(plain) == 0 {
		return 0, nil, nil
	}

	buf := entryBufferFromByteSliceView(plain, bufferScopeMultiEntryPlain)
	defer func() { _ = buf.Close() }()

	comp := w.compressor(method)
	if comp == nil {
		return 0, nil, ErrAlgorithm
	}
	limits := w.writeBufferLimits()
	return w.maxCompressedChunkBufferedWithCompressor(
		method,
		buf,
		0,
		len(plain),
		maxComp,
		comp,
		limits.MaxMethod14InputBufferSize,
	)
}

func (w *MultiVolumeWriter) maxCompressedChunkWithCompressor(
	method uint16,
	plain []byte,
	maxComp int64,
	comp Compressor,
	method14InputLimit uint64,
) (int, []byte, error) {
	if len(plain) == 0 {
		return 0, nil, nil
	}
	if method != Store && comp == nil {
		return 0, nil, ErrAlgorithm
	}

	buf := entryBufferFromByteSliceView(plain, bufferScopeMultiEntryPlain)
	defer func() { _ = buf.Close() }()

	return w.maxCompressedChunkBufferedWithCompressor(
		method,
		buf,
		0,
		len(plain),
		maxComp,
		comp,
		method14InputLimit,
	)
}

func entryBufferFromByteSliceView(src []byte, scope string) *entryBuffer {
	b := newEntryBuffer(uint64(len(src)), scope)
	b.mem = *bytes.NewBuffer(src)
	b.size = uint64(len(src))
	if b.memLimit < b.size {
		b.memLimit = b.size
	}
	return b
}

func (w *MultiVolumeWriter) maxCompressedChunkBuffered(method uint16, plain *entryBuffer, off int64, plainLen int, maxComp int64) (int, []byte, error) {
	comp := w.compressor(method)
	if comp == nil {
		return 0, nil, ErrAlgorithm
	}
	limits := w.writeBufferLimits()
	return w.maxCompressedChunkBufferedWithCompressor(
		method,
		plain,
		off,
		plainLen,
		maxComp,
		comp,
		limits.MaxMethod14InputBufferSize,
	)
}

func (w *MultiVolumeWriter) maxCompressedChunkBufferedWithCompressor(
	method uint16,
	plain *entryBuffer,
	off int64,
	plainLen int,
	maxComp int64,
	compFn Compressor,
	method14InputLimit uint64,
) (int, []byte, error) {
	n, comp, _, err := w.maxCompressedChunkBufferedWithCompressorAndCRC(
		method,
		plain,
		off,
		plainLen,
		maxComp,
		compFn,
		method14InputLimit,
	)
	return n, comp, err
}

func nextCompressedChunkProbeSize(bestN, hiN int, bestComp []byte, maxComp int64) int {
	n := bestN + (hiN-bestN)/2
	if bestN <= 0 || hiN-bestN <= 1 || len(bestComp) == 0 || maxComp <= int64(len(bestComp)) {
		return n
	}

	estimate := (int64(bestN) * maxComp) / int64(len(bestComp))
	maxInt := int64(^uint(0) >> 1)
	if estimate > maxInt {
		estimate = maxInt
	}
	if estimate > int64(bestN) && estimate < int64(hiN) {
		n = int(estimate)
	}
	return n
}

func initialCompressedChunkProbeSize(fullN int, maxComp int64) int {
	probeN := 1
	if maxComp > 0 {
		candidate := maxComp
		maxInt := int64(^uint(0) >> 1)
		if candidate > maxInt {
			candidate = maxInt
		}
		if candidate > 0 {
			probeN = int(candidate)
		}
	}
	if probeN >= fullN {
		probeN = fullN - 1
	}
	if probeN < 1 {
		probeN = 1
	}
	return probeN
}

func maxCompressedChunkCanStopRefining(method uint16, plainLen, remainingRange int) bool {
	// Native methods are expensive to re-run for adjacent lengths. Once the
	// fitting prefix is within a small window, prefer the known-good chunk over
	// exact byte-level packing.
	return isNativeMethod14(method) &&
		plainLen > maxCompressedChunkExhaustiveThreshold &&
		remainingRange <= maxCompressedChunkNativeRefineWindow
}

func maxCompressedChunkShouldDelayFullProbe(method uint16, plainLen int, maxComp int64) bool {
	return isNativeMethod14(method) &&
		plainLen > method14CompressorChunkSize &&
		maxComp > 0 &&
		int64(plainLen) > 2*maxComp
}

func compressedProbeSuggestsFullMayFit(probeN int, comp []byte, fullN int, maxComp int64) bool {
	if probeN <= 0 || fullN <= probeN || maxComp < 0 || len(comp) == 0 {
		return false
	}
	return float64(len(comp))*float64(fullN) <= float64(maxComp)*float64(probeN)
}

func (w *MultiVolumeWriter) maxCompressedChunkBufferedWithCompressorAndCRC(
	method uint16,
	plain *entryBuffer,
	off int64,
	plainLen int,
	maxComp int64,
	compFn Compressor,
	method14InputLimit uint64,
) (int, []byte, uint32, error) {
	if plainLen == 0 {
		return 0, nil, crc32.ChecksumIEEE(nil), nil
	}

	type chunkProbe struct {
		fit bool
		crc uint32
	}

	probes := make(map[int]chunkProbe)
	const maxBlobCacheEntries = 4
	blobCache := make(map[int][]byte, maxBlobCacheEntries)
	blobOrder := make([]int, 0, maxBlobCacheEntries)
	cacheBlob := func(n int, comp []byte) {
		if comp == nil {
			return
		}
		if _, ok := blobCache[n]; ok {
			blobCache[n] = comp
			return
		}
		if len(blobOrder) == maxBlobCacheEntries {
			evict := blobOrder[0]
			blobOrder = blobOrder[1:]
			delete(blobCache, evict)
		}
		blobOrder = append(blobOrder, n)
		blobCache[n] = comp
	}
	probe := func(n int) (chunkProbe, []byte, error) {
		if cached, ok := probes[n]; ok {
			return cached, blobCache[n], nil
		}
		comp, fit, crc, err := w.compressDataRangeWithCompressorAndCRC(method, plain, off, n, maxComp, compFn, method14InputLimit)
		if err != nil {
			return chunkProbe{}, nil, err
		}
		res := chunkProbe{
			fit: fit,
			crc: crc,
		}
		probes[n] = res
		if fit {
			cacheBlob(n, comp)
		}
		return res, comp, nil
	}

	exhaustive := func() (int, []byte, uint32, error) {
		for n := plainLen; n > 0; n-- {
			res, comp, err := probe(n)
			if err != nil {
				return 0, nil, 0, err
			}
			if !res.fit {
				continue
			}
			if comp == nil {
				comp, fit, crc, err := w.compressDataRangeWithCompressorAndCRC(method, plain, off, n, maxComp, compFn, method14InputLimit)
				if err != nil {
					return 0, nil, 0, err
				}
				if !fit {
					continue
				}
				res = chunkProbe{fit: true, crc: crc}
				probes[n] = res
				cacheBlob(n, comp)
			}
			return n, comp, res.crc, nil
		}
		return 0, nil, 0, nil
	}
	boundedFallback := func() (int, []byte, uint32, error) {
		if plainLen <= maxCompressedChunkExhaustiveThreshold {
			return exhaustive()
		}

		bestN := 0
		var bestComp []byte
		bestCRC := uint32(0)
		updateBest := func(n int, comp []byte, crc uint32) {
			if n < bestN {
				return
			}
			if n > bestN || bestComp == nil {
				bestN = n
				bestComp = comp
				bestCRC = crc
			}
		}

		step := (plainLen + maxCompressedChunkProbeBudget - 1) / maxCompressedChunkProbeBudget
		if step < 1 {
			step = 1
		}
		offsets := []int{0}
		if step > 1 {
			offsets = append(offsets, step/2)
		}
		for _, offset := range offsets {
			start := plainLen - offset
			if start < 1 {
				start = 1
			}
			for n := start; n >= 1; n -= step {
				res, comp, err := probe(n)
				if err != nil {
					return 0, nil, 0, err
				}
				if !res.fit {
					continue
				}
				updateBest(n, comp, res.crc)
			}
		}
		if bestN == 0 {
			// Sparse/non-monotonic compressors can fit only on unsampled lengths.
			// Preserve correctness by falling back to exhaustive probing.
			return exhaustive()
		}

		refineSpan := step
		if refineSpan < maxCompressedChunkLocalRefineWindow {
			refineSpan = maxCompressedChunkLocalRefineWindow
		}
		refineHi := bestN + refineSpan
		if refineHi > plainLen {
			refineHi = plainLen
		}
		for n := refineHi; n > bestN; n-- {
			res, comp, err := probe(n)
			if err != nil {
				return 0, nil, 0, err
			}
			if !res.fit {
				continue
			}
			updateBest(n, comp, res.crc)
			break
		}

		if bestComp == nil {
			comp, fit, crc, err := w.compressDataRangeWithCompressorAndCRC(method, plain, off, bestN, maxComp, compFn, method14InputLimit)
			if err != nil {
				return 0, nil, 0, err
			}
			if !fit {
				return 0, nil, 0, nil
			}
			bestComp = comp
			bestCRC = crc
			probes[bestN] = chunkProbe{fit: true, crc: crc}
			cacheBlob(bestN, bestComp)
		}
		return bestN, bestComp, bestCRC, nil
	}

	// First probe the full payload so non-monotonic compressors that shrink on
	// larger prefixes can still take the whole chunk in one step. Native
	// method 1-4 probes are expensive, so delay this full-size probe when the
	// volume budget is clearly much smaller than the available plain prefix.
	fullN := plainLen
	if fullN == 1 {
		fullProbe, fullComp, err := probe(fullN)
		if err != nil {
			return 0, nil, 0, err
		}
		if fullProbe.fit {
			return fullN, fullComp, fullProbe.crc, nil
		}
		return 0, nil, 0, nil
	}
	delayFullProbe := maxCompressedChunkShouldDelayFullProbe(method, fullN, maxComp)
	if !delayFullProbe {
		fullProbe, fullComp, err := probe(fullN)
		if err != nil {
			return 0, nil, 0, err
		}
		if fullProbe.fit {
			return fullN, fullComp, fullProbe.crc, nil
		}
	}

	maxFitN := 0
	minOverN := fullN
	observe := func(n int, fit bool) {
		if fit {
			if n > maxFitN {
				maxFitN = n
			}
			return
		}
		if minOverN == 0 || n < minOverN {
			minOverN = n
		}
	}
	violated := func() bool {
		return maxFitN != 0 && minOverN != 0 && maxFitN > minOverN
	}
	observe(fullN, false)

	probeN := initialCompressedChunkProbeSize(fullN, maxComp)

	probeRes, probeComp, err := probe(probeN)
	if err != nil {
		return 0, nil, 0, err
	}
	probeFits := probeRes.fit
	observe(probeN, probeFits)
	if violated() {
		return boundedFallback()
	}
	if delayFullProbe && probeFits && compressedProbeSuggestsFullMayFit(probeN, probeComp, fullN, maxComp) {
		fullProbe, fullComp, err := probe(fullN)
		if err != nil {
			return 0, nil, 0, err
		}
		if fullProbe.fit {
			return fullN, fullComp, fullProbe.crc, nil
		}
	}

	bestN := 0
	var bestComp []byte
	bestCRC := uint32(0)
	hiN := fullN // known oversize by the full payload probe above
	if probeFits {
		bestN = probeN
		bestComp = probeComp
		bestCRC = probeRes.crc
	} else {
		for n := probeN / 2; ; n /= 2 {
			if n < 1 {
				n = 1
			}
			res, comp, err := probe(n)
			if err != nil {
				return 0, nil, 0, err
			}
			fit := res.fit
			observe(n, fit)
			if violated() {
				return boundedFallback()
			}
			if fit {
				if comp == nil {
					comp, fit, crc, err := w.compressDataRangeWithCompressorAndCRC(method, plain, off, n, maxComp, compFn, method14InputLimit)
					if err != nil {
						return 0, nil, 0, err
					}
					if !fit {
						return boundedFallback()
					}
					res = chunkProbe{fit: true, crc: crc}
					probes[n] = res
					cacheBlob(n, comp)
				}
				bestN = n
				bestComp = comp
				bestCRC = res.crc
				break
			}
			if n == 1 {
				break
			}
		}
		if bestN == 0 {
			return boundedFallback()
		}
	}

	if minOverN > bestN && minOverN < hiN {
		hiN = minOverN
	}
	for growN := bestN * 2; bestN > 0 && growN > bestN && growN < hiN; {
		res, comp, err := probe(growN)
		if err != nil {
			return 0, nil, 0, err
		}
		fit := res.fit
		observe(growN, fit)
		if violated() {
			return boundedFallback()
		}
		if !fit {
			hiN = growN
			break
		}
		bestN = growN
		bestComp = comp
		bestCRC = res.crc
		if growN > hiN/2 {
			break
		}
		growN *= 2
	}
	for hiN-bestN > 1 {
		if maxCompressedChunkCanStopRefining(method, plainLen, hiN-bestN) {
			break
		}
		n := nextCompressedChunkProbeSize(bestN, hiN, bestComp, maxComp)
		res, comp, err := probe(n)
		if err != nil {
			return 0, nil, 0, err
		}
		fit := res.fit
		observe(n, fit)
		if violated() {
			return boundedFallback()
		}
		if fit {
			if comp == nil {
				comp, fit, crc, err := w.compressDataRangeWithCompressorAndCRC(method, plain, off, n, maxComp, compFn, method14InputLimit)
				if err != nil {
					return 0, nil, 0, err
				}
				if !fit {
					hiN = n
					continue
				}
				res = chunkProbe{fit: true, crc: crc}
				probes[n] = res
				cacheBlob(n, comp)
			}
			bestN = n
			bestComp = comp
			bestCRC = res.crc
			continue
		}
		hiN = n
	}
	if bestComp == nil && bestN > 0 {
		var fit bool
		bestComp, fit, bestCRC, err = w.compressDataRangeWithCompressorAndCRC(method, plain, off, bestN, maxComp, compFn, method14InputLimit)
		if err != nil {
			return 0, nil, 0, err
		}
		if !fit {
			return boundedFallback()
		}
		if int64(len(bestComp)) > maxComp {
			return boundedFallback()
		}
		probes[bestN] = chunkProbe{fit: true, crc: bestCRC}
		cacheBlob(bestN, bestComp)
	}
	return bestN, bestComp, bestCRC, nil
}

func (w *MultiVolumeWriter) compressData(method uint16, plain []byte) ([]byte, error) {
	compFn := w.compressor(method)
	if method != Store && compFn == nil {
		return nil, ErrAlgorithm
	}
	limits := w.writeBufferLimits()
	return w.compressDataWithCompressor(method, plain, compFn, limits.MaxMethod14InputBufferSize)
}

func (w *MultiVolumeWriter) compressDataWithCompressor(
	method uint16,
	plain []byte,
	compFn Compressor,
	method14InputLimit uint64,
) ([]byte, error) {
	if method == Store {
		return append([]byte(nil), plain...), nil
	}
	if compFn == nil {
		return nil, ErrAlgorithm
	}

	comp, fit, err := w.compressDataFromReaderWithCompressor(
		method,
		bytes.NewReader(plain),
		-1,
		compFn,
		method14InputLimit,
	)
	if err != nil {
		return nil, err
	}
	if !fit {
		return nil, errNoSegmentFit
	}
	return comp, nil
}

func (w *MultiVolumeWriter) compressDataRange(method uint16, plain *entryBuffer, off int64, n int, maxComp int64) ([]byte, bool, error) {
	compFn := w.compressor(method)
	if method != Store && compFn == nil {
		return nil, false, ErrAlgorithm
	}
	limits := w.writeBufferLimits()
	return w.compressDataRangeWithCompressor(
		method,
		plain,
		off,
		n,
		maxComp,
		compFn,
		limits.MaxMethod14InputBufferSize,
	)
}

func (w *MultiVolumeWriter) compressDataRangeWithCompressor(
	method uint16,
	plain *entryBuffer,
	off int64,
	n int,
	maxComp int64,
	compFn Compressor,
	method14InputLimit uint64,
) ([]byte, bool, error) {
	comp, fit, _, err := w.compressDataRangeWithCompressorAndCRC(
		method,
		plain,
		off,
		n,
		maxComp,
		compFn,
		method14InputLimit,
	)
	return comp, fit, err
}

func (w *MultiVolumeWriter) compressDataRangeWithCompressorAndCRC(
	method uint16,
	plain *entryBuffer,
	off int64,
	n int,
	maxComp int64,
	compFn Compressor,
	method14InputLimit uint64,
) ([]byte, bool, uint32, error) {
	if n == 0 {
		return w.compressDataFromReaderWithCompressorAndCRC(
			method,
			bytes.NewReader(nil),
			maxComp,
			compFn,
			method14InputLimit,
		)
	}
	if plain == nil {
		return nil, false, 0, io.ErrUnexpectedEOF
	}
	reader := io.NewSectionReader(plain, off, int64(n))
	return w.compressDataFromReaderWithCompressorAndCRC(method, reader, maxComp, compFn, method14InputLimit)
}

func (w *MultiVolumeWriter) compressDataFromReader(method uint16, reader io.Reader, maxComp int64) ([]byte, bool, error) {
	compFn := w.compressor(method)
	if method != Store && compFn == nil {
		return nil, false, ErrAlgorithm
	}
	limits := w.writeBufferLimits()
	return w.compressDataFromReaderWithCompressor(method, reader, maxComp, compFn, limits.MaxMethod14InputBufferSize)
}

func (w *MultiVolumeWriter) compressDataFromReaderWithCompressor(
	method uint16,
	reader io.Reader,
	maxComp int64,
	compFn Compressor,
	method14InputLimit uint64,
) ([]byte, bool, error) {
	comp, fit, _, err := w.compressDataFromReaderWithCompressorAndCRC(
		method,
		reader,
		maxComp,
		compFn,
		method14InputLimit,
	)
	return comp, fit, err
}

func (w *MultiVolumeWriter) compressDataFromReaderWithCompressorAndCRC(
	method uint16,
	reader io.Reader,
	maxComp int64,
	compFn Compressor,
	method14InputLimit uint64,
) ([]byte, bool, uint32, error) {
	if method == Store {
		return nil, false, 0, errors.New("arj: store method not supported for compressed probe")
	}

	if compFn == nil {
		return nil, false, 0, ErrAlgorithm
	}

	out := &limitedCaptureBuffer{limit: maxComp}
	cw, err := compFn(out)
	if err != nil {
		return nil, false, 0, err
	}
	if setter, ok := cw.(method14InputLimitSetter); ok {
		setter.setMethod14InputBufferLimit(method14InputLimit)
	}

	sum := crc32.NewIEEE()
	tee := io.TeeReader(reader, sum)
	copyBuf := compressedProbeCopyBufferPool.Get().([]byte)
	if len(copyBuf) == 0 {
		copyBuf = make([]byte, 32<<10)
	}
	_, copyErr := io.CopyBuffer(cw, tee, copyBuf)
	if cap(copyBuf) >= 32<<10 {
		compressedProbeCopyBufferPool.Put(copyBuf[:32<<10])
	}
	if copyErr != nil && !errors.Is(copyErr, errCompressedProbeLimit) {
		_ = cw.Close()
		return nil, false, 0, copyErr
	}
	closeErr := cw.Close()
	if closeErr != nil && !errors.Is(closeErr, errCompressedProbeLimit) {
		return nil, false, 0, closeErr
	}

	if errors.Is(copyErr, errCompressedProbeLimit) || errors.Is(closeErr, errCompressedProbeLimit) || out.exceeded {
		return nil, false, 0, nil
	}
	return out.Bytes(), true, sum.Sum32(), nil
}

type limitedCaptureBuffer struct {
	limit    int64
	buf      bytes.Buffer
	exceeded bool
}

func (w *limitedCaptureBuffer) Write(p []byte) (int, error) {
	if w.limit < 0 {
		return w.buf.Write(p)
	}
	if int64(w.buf.Len()) >= w.limit {
		w.exceeded = true
		return 0, errCompressedProbeLimit
	}

	remaining := w.limit - int64(w.buf.Len())
	if int64(len(p)) > remaining {
		n, _ := w.buf.Write(p[:int(remaining)])
		w.exceeded = true
		return n, errCompressedProbeLimit
	}
	return w.buf.Write(p)
}

func (w *limitedCaptureBuffer) Bytes() []byte {
	return w.buf.Bytes()
}

func rawSegmentOverhead(base *FileHeader) (int, error) {
	if base == nil {
		return 0, ErrFormat
	}
	if err := validateLocalHeaderLengths(base); err != nil {
		return 0, err
	}
	if err := validateLocalExtendedHeaders(base); err != nil {
		return 0, err
	}

	basicLen := int(base.FirstHeaderSize) + len(base.Name) + 1 + len(base.Comment) + 1
	total := 4 + basicLen + 4 // marker + basic + basic CRC32
	for _, ext := range base.LocalExtendedHeaders {
		total += 2 + len(ext) + 4 // ext size + payload + ext CRC32
	}
	total += 2 // ext terminator
	return total, nil
}

func (w *MultiVolumeWriter) ensureCurrentVolume() error {
	if w.current != nil {
		return nil
	}
	return w.openNextVolume()
}

func (w *MultiVolumeWriter) openNextVolume() error {
	path := w.partPath(w.nextPart)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, w.fileMode)
	if err != nil {
		w.latchFailure(err)
		return err
	}

	vw := NewWriter(f)
	for method, comp := range w.compressorSnapshot() {
		vw.RegisterCompressor(method, comp)
	}

	hdr := w.mainHeaderForVolume()
	hdr.Flags &^= FlagVolume
	if err := vw.SetArchiveHeader(&hdr); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return err
	}
	if err := vw.writeMainHeader(); err != nil {
		w.latchFailure(err)
		_ = f.Close()
		_ = os.Remove(path)
		return err
	}
	if vw.cw.Count()+4 > w.volumeSize {
		_ = f.Close()
		_ = os.Remove(path)
		return errVolumeTooSmall
	}
	w.markWriteStarted()

	w.current = vw
	w.currentFile = f
	w.currentHasEntries = false
	w.nextPart++
	w.paths = append(w.paths, path)
	return nil
}

func (w *MultiVolumeWriter) closeCurrentVolume(nonLast bool) error {
	if w.current == nil {
		return nil
	}

	var err error
	if closeErr := w.current.Close(); closeErr != nil {
		w.latchFailure(closeErr)
		err = errors.Join(err, closeErr)
	}
	if nonLast && err == nil {
		if patchErr := patchMainVolumeFlag(w.currentFile, true); patchErr != nil {
			w.latchFailure(patchErr)
			err = errors.Join(err, patchErr)
		}
	}
	if closeErr := w.currentFile.Close(); closeErr != nil {
		w.latchFailure(closeErr)
		err = errors.Join(err, closeErr)
	}

	w.current = nil
	w.currentFile = nil
	w.currentHasEntries = false
	return err
}

func (w *MultiVolumeWriter) cleanupEmptyCurrentVolume() error {
	if w.current == nil || w.currentHasEntries {
		return nil
	}

	path := ""
	if n := len(w.paths); n > 0 {
		path = w.paths[n-1]
	}
	if err := w.closeCurrentVolume(false); err != nil {
		return err
	}

	if path != "" {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if n := len(w.paths); n > 0 && w.paths[n-1] == path {
			w.paths = w.paths[:n-1]
		}
	}
	if w.nextPart > 0 {
		w.nextPart--
	}
	return nil
}

func (w *MultiVolumeWriter) volumeTooSmallOnEmptyCurrent() error {
	if err := w.cleanupEmptyCurrentVolume(); err != nil {
		w.latchFailure(err)
		return err
	}
	return errVolumeTooSmall
}

func patchMainVolumeFlag(f *os.File, set bool) error {
	var prefix [4]byte
	if err := readAtFull(f, prefix[:], 0); err != nil {
		return err
	}
	if prefix[0] != arjHeaderID1 || prefix[1] != arjHeaderID2 {
		return ErrFormat
	}

	basicSize := int(binary.LittleEndian.Uint16(prefix[2:4]))
	if basicSize < arjMinFirstHeaderSize || basicSize > arjMaxBasicHeaderSize {
		return ErrFormat
	}

	basic := make([]byte, basicSize)
	if err := readAtFull(f, basic, 4); err != nil {
		return err
	}
	if set {
		basic[4] |= FlagVolume
	} else {
		basic[4] &^= FlagVolume
	}
	if _, err := f.WriteAt(basic, 4); err != nil {
		return err
	}

	var crc [4]byte
	binary.LittleEndian.PutUint32(crc[:], crc32.ChecksumIEEE(basic))
	if _, err := f.WriteAt(crc[:], int64(4+basicSize)); err != nil {
		return err
	}
	return nil
}

func (w *MultiVolumeWriter) currentRemaining() int64 {
	if w.current == nil {
		return w.volumeSize
	}
	return w.volumeSize - w.current.cw.Count() - 4
}

func (w *MultiVolumeWriter) freshVolumeRemaining() (int64, error) {
	vw := NewWriter(io.Discard)
	hdr := w.mainHeaderForVolume()
	if err := vw.SetArchiveHeader(&hdr); err != nil {
		return 0, err
	}
	if err := vw.writeMainHeader(); err != nil {
		return 0, err
	}
	return w.volumeSize - vw.cw.Count() - 4, nil
}

func (w *MultiVolumeWriter) mainHeaderForVolume() ArchiveHeader {
	w.cfgMu.RLock()
	defer w.cfgMu.RUnlock()
	return w.mainHeaderForVolumeLocked()
}

func (w *MultiVolumeWriter) mainHeaderForVolumeLocked() ArchiveHeader {
	if w.archiveHdr != nil {
		return cloneArchiveHeader(*w.archiveHdr)
	}

	now := w.defaultTime
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return ArchiveHeader{
		FirstHeaderSize: arjMinFirstHeaderSize,
		ArchiverVersion: arjVersionCurrent,
		MinVersion:      arjVersionNeeded,
		HostOS:          currentHostOS(),
		SecurityVersion: 0,
		FileType:        arjFileTypeMain,
		Created:         now,
		Modified:        now,
		ArchiveSize:     0,
		Name:            w.archiveName,
		Comment:         w.comment,
	}
}

func (w *MultiVolumeWriter) latchFailure(err error) {
	if err == nil || w.failed != nil {
		return
	}
	w.failed = err
}

func (w *MultiVolumeWriter) compressorSnapshot() map[uint16]Compressor {
	if v := w.compressorV.Load(); v != nil {
		return v.(map[uint16]Compressor)
	}

	w.cfgMu.RLock()
	snapshot := w.compressors
	w.cfgMu.RUnlock()
	return snapshot
}

func (w *MultiVolumeWriter) bufferLimitSnapshot() WriteBufferLimits {
	if v := w.bufferLimitV.Load(); v != nil {
		return v.(WriteBufferLimits)
	}

	w.cfgMu.RLock()
	limits := normalizeWriteBufferLimits(w.bufferLimit)
	w.cfgMu.RUnlock()
	return limits
}

func (w *MultiVolumeWriter) markWriteStarted() {
	w.cfgMu.RLock()
	w.writeStarted.Store(true)
	w.cfgMu.RUnlock()
}

func (w *MultiVolumeWriter) partPath(part int) string {
	if part <= 0 {
		return w.firstPath
	}
	return fmt.Sprintf("%s%s%02d", w.stem, w.partExtPrefix, part)
}

func normalizeMultiVolumePath(name string) (first, stem, partPrefix string, err error) {
	if name == "" {
		return "", "", "", errMultiVolumePath
	}

	ext := filepath.Ext(name)
	if ext == "" {
		return name + ".arj", name, ".a", nil
	}
	if !strings.EqualFold(ext, ".arj") {
		return "", "", "", errMultiVolumePath
	}

	stem = strings.TrimSuffix(name, ext)
	partPrefix = ".a"
	if ext == strings.ToUpper(ext) {
		partPrefix = ".A"
	}
	return name, stem, partPrefix, nil
}
