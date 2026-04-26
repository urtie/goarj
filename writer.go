package arj

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	errLongName           = errors.New("arj: FileHeader.Name too long")
	errLongComment        = errors.New("arj: FileHeader.Comment too long")
	errLongArchiveName    = errors.New("arj: Writer.ArchiveName too long")
	errLongArchiveComment = errors.New("arj: Writer.Comment too long")
	errReadOnDirectory    = errors.New("is a directory")
	errDirectoryFileData  = errors.New("arj: directory entries cannot have file data")

	// ErrNilFileHeader indicates a nil file-header argument.
	ErrNilFileHeader = errors.New("arj: nil FileHeader")
	// ErrFileTooLarge indicates an entry exceeds ARJ 32-bit size limits.
	ErrFileTooLarge = errors.New("arj: file too large")
	// ErrRawCopyMultisegment indicates raw-copy is not supported for entries
	// reconstructed from multiple continuation segments.
	ErrRawCopyMultisegment = errors.New("arj: cannot raw-copy multi-segment entry")
	// ErrRawCopyMultiSegment is an alias for ErrRawCopyMultisegment.
	ErrRawCopyMultiSegment = ErrRawCopyMultisegment

	errNilFileHeader       = ErrNilFileHeader
	errFileTooLarge        = ErrFileTooLarge
	errRawCopyMultisegment = ErrRawCopyMultisegment

	// ErrBufferLimitExceeded indicates a writer-side buffering limit was exceeded.
	ErrBufferLimitExceeded = errors.New("arj: buffer limit exceeded")
)

const maxARJFileSize = uint64(^uint32(0))

const (
	// DefaultMaxEntryBufferSize is the default per-entry buffering cap in bytes.
	// Exceeding it returns ErrBufferLimitExceeded.
	DefaultMaxEntryBufferSize uint64 = 256 << 20
	// DefaultMaxCompressedEntryBufferSize is the default per-entry compressed
	// buffering cap in bytes.
	DefaultMaxCompressedEntryBufferSize = DefaultMaxEntryBufferSize
	// DefaultMaxPlainEntryBufferSize is the default per-entry plain buffering
	// cap in bytes.
	DefaultMaxPlainEntryBufferSize = DefaultMaxEntryBufferSize
	// DefaultMaxMethod14InputBufferSize is the default max in-memory buffered
	// method 1-4 compressor input size in bytes.
	DefaultMaxMethod14InputBufferSize uint64 = 256 << 20
)

const (
	bufferScopeWriterEntryCompressed = "writer entry compressed buffer"
	bufferScopeMultiEntryPlain       = "multi-volume entry plain buffer"
	bufferScopeMethod14Input         = "method14 compressor input buffer"
	// maxInMemoryEntrySpoolSize caps in-memory bytes before entry data spills
	// to a temp file.
	maxInMemoryEntrySpoolSize = 8 << 20
)

// WriteBufferLimits controls write-path buffering limits.
// Zero values fall back to package defaults.
type WriteBufferLimits struct {
	// MaxCompressedEntryBufferSize limits compressed bytes buffered per entry
	// before returning ErrBufferLimitExceeded.
	//
	// Writer uses this for Create/CreateHeader.
	// MultiVolumeWriter uses this for CreateRaw.
	MaxCompressedEntryBufferSize uint64

	// MaxPlainEntryBufferSize limits plain bytes buffered per entry before
	// returning ErrBufferLimitExceeded.
	//
	// MultiVolumeWriter uses this for Create/CreateHeader.
	MaxPlainEntryBufferSize uint64

	// MaxEntryBufferSize is a legacy fallback used when the explicit
	// compressed/plain field for an operation is zero.
	//
	// Deprecated: prefer MaxCompressedEntryBufferSize and
	// MaxPlainEntryBufferSize.
	MaxEntryBufferSize uint64

	// MaxMethod14InputBufferSize limits method 1-4 compressor input buffering.
	MaxMethod14InputBufferSize uint64
}

// BufferLimitError describes a buffered-memory limit violation.
type BufferLimitError struct {
	Scope     string
	Limit     uint64
	Buffered  uint64
	Attempted uint64
}

func (e *BufferLimitError) Error() string {
	if e == nil {
		return "arj: buffer limit exceeded"
	}
	return fmt.Sprintf("arj: %s exceeded: limit=%d buffered=%d attempted=%d",
		e.Scope, e.Limit, e.Buffered, e.Attempted)
}

func (e *BufferLimitError) Unwrap() error {
	if e == nil {
		return nil
	}
	return ErrBufferLimitExceeded
}

type method14InputLimitSetter interface {
	setMethod14InputBufferLimit(limit uint64)
}

// Writer implements an ARJ file writer.
type Writer struct {
	cw           *countWriter
	streamSeeker io.WriteSeeker
	last         writerEntryWriter
	closed       bool
	failed       error
	wroteMain    bool
	cfgMu        sync.RWMutex
	compressors  map[uint16]Compressor
	compressorV  atomic.Value // map[uint16]Compressor
	archiveName  string
	comment      string
	archiveHdr   *ArchiveHeader
	defaultTime  time.Time
	bufferLimit  WriteBufferLimits
	bufferLimitV atomic.Value // WriteBufferLimits
	writeStarted atomic.Bool
}

type writerEntryWriter interface {
	close() error
	isClosed() bool
	writeError() error
}

// NewWriter returns a new Writer writing an ARJ archive to w.
func NewWriter(w io.Writer) *Writer {
	limits := normalizeWriteBufferLimits(WriteBufferLimits{})
	now := time.Now().UTC()
	cw := &countWriter{}
	var streamSeeker io.WriteSeeker
	if seeker, ok := w.(io.WriteSeeker); ok {
		cw.w = seeker
		streamSeeker = seeker
	} else {
		cw.w = bufio.NewWriter(w)
	}
	writer := &Writer{
		cw:           cw,
		streamSeeker: streamSeeker,
		bufferLimit:  limits,
		defaultTime:  now,
	}
	writer.compressorV.Store((map[uint16]Compressor)(nil))
	writer.bufferLimitV.Store(limits)
	return writer
}

// SetBufferLimits overrides write-path buffering limits for future entries.
// Any zero field keeps the corresponding package default.
func (w *Writer) SetBufferLimits(limits WriteBufferLimits) {
	limits = normalizeWriteBufferLimits(limits)

	w.cfgMu.Lock()
	w.bufferLimit = limits
	w.bufferLimitV.Store(limits)
	w.cfgMu.Unlock()
}

// Flush flushes any buffered data to the underlying writer.
func (w *Writer) Flush() error {
	bw, ok := w.cw.w.(*bufio.Writer)
	if !ok {
		return nil
	}
	return bw.Flush()
}

// SetComment sets the archive-level comment field.
// It must be called before any file data is written.
func (w *Writer) SetComment(comment string) error {
	w.cfgMu.Lock()
	defer w.cfgMu.Unlock()

	if w.writeStarted.Load() || w.bytesWritten() != 0 {
		return errors.New("arj: Writer.SetComment called after data was written")
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

// SetArchiveName sets the archive name in the main header.
// It must be called before any file data is written.
func (w *Writer) SetArchiveName(name string) error {
	w.cfgMu.Lock()
	defer w.cfgMu.Unlock()

	if w.writeStarted.Load() || w.bytesWritten() != 0 {
		return errors.New("arj: Writer.SetArchiveName called after data was written")
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

// SetArchiveHeader sets the full archive main header model.
// It must be called before any file data is written.
func (w *Writer) SetArchiveHeader(hdr *ArchiveHeader) error {
	w.cfgMu.Lock()
	defer w.cfgMu.Unlock()

	if w.writeStarted.Load() || w.bytesWritten() != 0 {
		return errors.New("arj: Writer.SetArchiveHeader called after data was written")
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

// Close finishes writing the ARJ archive.
// It does not close the underlying writer.
func (w *Writer) Close() error {
	if w.failed != nil {
		// Best-effort cleanup for a poisoned staged entry. writeErr indicates
		// close cannot emit/commit local header+data.
		if w.last != nil && !w.last.isClosed() && w.last.writeError() != nil {
			_ = w.last.close()
		}
		return w.failed
	}
	if w.last != nil && !w.last.isClosed() {
		if err := w.last.close(); err != nil {
			w.latchFailure(err)
			return err
		}
	}
	if w.closed {
		return errors.New("arj: writer closed twice")
	}
	w.closed = true

	if !w.wroteMain {
		if err := w.writeMainHeader(); err != nil {
			return err
		}
	}

	var end [4]byte
	end[0] = arjHeaderID1
	end[1] = arjHeaderID2
	// u16 size = 0
	if _, err := w.cw.Write(end[:]); err != nil {
		return err
	}

	return w.Flush()
}

// Create adds a file to the archive using the provided name.
// The file is written using Method4 compression.
func (w *Writer) Create(name string) (io.Writer, error) {
	header := &FileHeader{
		Name:   name,
		Method: Method4,
	}
	return w.CreateHeader(header)
}

// CreateHeader adds a file to the archive using the provided FileHeader.
func (w *Writer) CreateHeader(fh *FileHeader) (io.Writer, error) {
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
	if !w.wroteMain {
		if err := w.writeMainHeader(); err != nil {
			return nil, err
		}
	}

	fw := &fileWriter{
		w:   w,
		h:   &h,
		crc: crc32.NewIEEE(),
	}
	limits := w.writeBufferLimits()
	fw.entryBufferLimit = limits.MaxCompressedEntryBufferSize
	fw.method14InputLimit = limits.MaxMethod14InputBufferSize

	if w.streamSeeker != nil && !h.isDir() {
		fw.streaming = true
		fw.streamSink = newEntryStreamSink(w.cw, 0, bufferScopeWriterEntryCompressed)
		cw, err := comp(fw.streamSink)
		if err != nil {
			return nil, err
		}
		if setter, ok := cw.(method14InputLimitSetter); ok {
			setter.setMethod14InputBufferLimit(fw.method14InputLimit)
		}
		fw.localHeaderOffset = w.cw.Count()

		fw.h.UncompressedSize64 = 0
		fw.h.CompressedSize64 = 0
		fw.h.CRC32 = 0
		if err := writeLocalFileHeader(w.cw, fw.h); err != nil {
			_ = cw.Close()
			return nil, err
		}
		fw.cw = cw
	} else {
		fw.comp = newEntryBuffer(fw.entryBufferLimit, bufferScopeWriterEntryCompressed)
		cw, err := comp(fw.comp)
		if err != nil {
			return nil, err
		}
		if setter, ok := cw.(method14InputLimitSetter); ok {
			setter.setMethod14InputBufferLimit(fw.method14InputLimit)
		}
		fw.cw = cw
	}

	w.last = fw
	return fw, nil
}

// RegisterCompressor registers or overrides a custom compressor for a
// specific method ID. If a compressor for a given method is not found,
// Writer defaults to the package-level registration.
// Passing nil explicitly disables the method on this Writer instance.
func (w *Writer) RegisterCompressor(method uint16, comp Compressor) {
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

func (w *Writer) compressor(method uint16) Compressor {
	if snapshot := w.compressorSnapshot(); snapshot != nil {
		if comp, ok := snapshot[method]; ok {
			return comp
		}
	}
	return compressor(method)
}

func (w *Writer) writeBufferLimits() WriteBufferLimits {
	if snapshot := w.bufferLimitSnapshot(); snapshot != (WriteBufferLimits{}) {
		return snapshot
	}
	return normalizeWriteBufferLimits(WriteBufferLimits{})
}

func (w *Writer) latchFailure(err error) {
	if err == nil || w.failed != nil {
		return
	}
	w.failed = err
}

func (w *Writer) prepare() error {
	if w.failed != nil {
		return w.failed
	}
	if w.closed {
		return errors.New("arj: write to closed writer")
	}
	if w.last != nil && !w.last.isClosed() {
		if err := w.last.close(); err != nil {
			w.latchFailure(err)
			return err
		}
	}
	return nil
}

func (w *Writer) writeMainHeader() error {
	h := w.mainHeaderForWrite()
	syncArchiveHeaderExtMetadata(&h)
	if err := normalizeMainFirstHeaderExtra(&h); err != nil {
		return err
	}
	syncArchiveHeaderSecurityMetadata(&h)
	if err := validateMainHeader(&h); err != nil {
		return err
	}

	basic := make([]byte, int(h.FirstHeaderSize))
	basic[0] = h.FirstHeaderSize
	basic[1] = h.ArchiverVersion
	basic[2] = h.MinVersion
	basic[3] = h.HostOS
	basic[4] = h.Flags
	basic[5] = h.SecurityVersion
	basic[6] = h.FileType
	basic[7] = h.Reserved
	binary.LittleEndian.PutUint32(basic[8:12], timeToDosDateTime(h.Created))
	binary.LittleEndian.PutUint32(basic[12:16], timeToDosDateTime(h.Modified))
	binary.LittleEndian.PutUint32(basic[16:20], h.ArchiveSize)
	binary.LittleEndian.PutUint32(basic[20:24], h.SecurityEnvelopePos)
	binary.LittleEndian.PutUint16(basic[24:26], h.FilespecPos)
	binary.LittleEndian.PutUint16(basic[26:28], h.SecurityEnvelopeSize)
	basic[28] = h.ExtFlags
	basic[29] = h.ChapterNumber
	copy(basic[arjMinFirstHeaderSize:], h.FirstHeaderExtra)

	var full []byte
	full = append(full, basic...)
	full = append(full, h.Name...)
	full = append(full, 0)
	full = append(full, h.Comment...)
	full = append(full, 0)

	w.markWriteStarted()
	if err := writeHeaderBlockWithExt(w.cw, full, h.MainExtendedHeaders); err != nil {
		return err
	}
	w.wroteMain = true
	return nil
}

func (w *Writer) mainHeaderForWrite() ArchiveHeader {
	w.cfgMu.RLock()
	defer w.cfgMu.RUnlock()
	return w.mainHeaderForWriteLocked()
}

func (w *Writer) mainHeaderForWriteLocked() ArchiveHeader {
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

func (w *Writer) bytesWritten() int64 {
	if w == nil || w.cw == nil {
		return 0
	}
	return w.cw.Count()
}

func (w *Writer) markWriteStarted() {
	w.cfgMu.RLock()
	w.writeStarted.Store(true)
	w.cfgMu.RUnlock()
}

func (w *Writer) compressorSnapshot() map[uint16]Compressor {
	if v := w.compressorV.Load(); v != nil {
		return v.(map[uint16]Compressor)
	}

	w.cfgMu.RLock()
	snapshot := w.compressors
	w.cfgMu.RUnlock()
	return snapshot
}

func (w *Writer) bufferLimitSnapshot() WriteBufferLimits {
	if v := w.bufferLimitV.Load(); v != nil {
		return v.(WriteBufferLimits)
	}

	w.cfgMu.RLock()
	limits := normalizeWriteBufferLimits(w.bufferLimit)
	w.cfgMu.RUnlock()
	return limits
}

type fileWriter struct {
	w                  *Writer
	h                  *FileHeader
	cw                 io.WriteCloser
	streamSink         *entryStreamSink
	comp               *entryBuffer
	streaming          bool
	localHeaderOffset  int64
	plainN             uint64
	entryBufferLimit   uint64
	method14InputLimit uint64
	crc                hash32
	writeErr           error
	closed             bool
}

type hash32 interface {
	Write(p []byte) (int, error)
	Sum32() uint32
}

func (w *fileWriter) latchWriteErr(err error) {
	if err == nil || w.writeErr != nil {
		return
	}
	w.writeErr = err
}

func (w *fileWriter) compressedSize() uint64 {
	if w == nil {
		return 0
	}
	if w.streaming {
		if w.streamSink != nil {
			return w.streamSink.Size()
		}
		return 0
	}
	if w.comp != nil {
		return w.comp.Size()
	}
	return 0
}

func (w *fileWriter) Write(p []byte) (int, error) {
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

	n, err := w.cw.Write(chunk)
	if n > 0 {
		_, _ = w.crc.Write(chunk[:n])
		w.plainN += uint64(n)
	}
	if err != nil {
		w.latchWriteErr(err)
	}

	sizeExceeded := false
	if limited && err == nil && n == len(chunk) {
		sizeExceeded = true
	}
	if w.plainN > maxARJFileSize || w.compressedSize() > maxARJFileSize {
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

func (w *fileWriter) Close() error {
	return w.close()
}

func (w *fileWriter) isClosed() bool {
	return w.closed
}

func (w *fileWriter) writeError() error {
	if w == nil {
		return nil
	}
	return w.writeErr
}

func (w *fileWriter) close() (err error) {
	if w.closed {
		return nil
	}
	w.closed = true
	defer func() {
		if err != nil && w.w != nil {
			w.w.latchFailure(err)
		}
	}()
	if w.comp != nil {
		defer func() {
			if cleanupErr := w.comp.Close(); cleanupErr != nil {
				err = errors.Join(err, cleanupErr)
			}
		}()
	}
	if err = w.cw.Close(); err != nil {
		return err
	}

	if w.h.isDir() && (w.plainN != 0 || w.compressedSize() != 0) {
		return errDirectoryFileData
	}
	if w.writeErr != nil {
		return w.writeErr
	}

	w.h.UncompressedSize64 = w.plainN
	w.h.CompressedSize64 = w.compressedSize()
	if w.h.UncompressedSize64 > maxARJFileSize || w.h.CompressedSize64 > maxARJFileSize {
		return errFileTooLarge
	}
	w.h.CRC32 = w.crc.Sum32()
	w.h.modifiedDOS = timeToDosDateTime(w.h.Modified)
	syncFileHeaderExtMetadata(w.h)

	if w.streaming {
		if err := w.w.Flush(); err != nil {
			return err
		}
		if err := patchLocalFileHeader(w.w.streamSeeker, w.localHeaderOffset, w.h); err != nil {
			return err
		}
	} else {
		if err := writeLocalFileHeader(w.w.cw, w.h); err != nil {
			return err
		}
		if _, err := w.comp.WriteTo(w.w.cw); err != nil {
			return err
		}
	}
	w.w.last = nil
	return nil
}

func writeHeaderBlock(w io.Writer, basic []byte) error {
	return writeHeaderBlockWithExt(w, basic, nil)
}

func writeHeaderBlockWithExt(w io.Writer, basic []byte, extHeaders [][]byte) error {
	if len(basic) > arjMaxBasicHeaderSize {
		return ErrFormat
	}
	if len(basic) < arjMinFirstHeaderSize {
		return ErrFormat
	}

	var prefix [4]byte
	prefix[0] = arjHeaderID1
	prefix[1] = arjHeaderID2
	binary.LittleEndian.PutUint16(prefix[2:4], uint16(len(basic)))
	if _, err := w.Write(prefix[:]); err != nil {
		return err
	}
	if _, err := w.Write(basic); err != nil {
		return err
	}

	var crc [4]byte
	binary.LittleEndian.PutUint32(crc[:], crc32.ChecksumIEEE(basic))
	if _, err := w.Write(crc[:]); err != nil {
		return err
	}

	for _, ext := range extHeaders {
		if len(ext) == 0 || len(ext) > 0xffff {
			return ErrFormat
		}

		var extSize [2]byte
		binary.LittleEndian.PutUint16(extSize[:], uint16(len(ext)))
		if _, err := w.Write(extSize[:]); err != nil {
			return err
		}
		if _, err := w.Write(ext); err != nil {
			return err
		}
		binary.LittleEndian.PutUint32(crc[:], crc32.ChecksumIEEE(ext))
		if _, err := w.Write(crc[:]); err != nil {
			return err
		}
	}

	var extEnd [2]byte
	if _, err := w.Write(extEnd[:]); err != nil {
		return err
	}
	return nil
}

func validateLocalHeaderLengths(h *FileHeader) error {
	firstSize := int(h.FirstHeaderSize)
	if firstSize == 0 {
		firstSize = arjMinFirstHeaderSize
	}
	if firstSize < arjMinFirstHeaderSize || firstSize > arjMaxBasicHeaderSize {
		return ErrFormat
	}
	if strings.IndexByte(h.Name, 0) >= 0 || strings.IndexByte(h.Comment, 0) >= 0 {
		return ErrFormat
	}
	if len(h.Name)+firstSize+2 > arjMaxBasicHeaderSize {
		return errLongName
	}
	if len(h.Comment)+firstSize+2 > arjMaxBasicHeaderSize {
		return errLongComment
	}
	if len(h.Name)+len(h.Comment)+firstSize+2 > arjMaxBasicHeaderSize {
		return errLongComment
	}
	return nil
}

func validateLocalExtendedHeaders(h *FileHeader) error {
	for _, ext := range h.LocalExtendedHeaders {
		if len(ext) == 0 || len(ext) > 0xffff {
			return ErrFormat
		}
	}
	return nil
}

func validateMainHeader(h *ArchiveHeader) error {
	if err := unsupportedMainSecurityFlagsError(h.Flags, h.EncryptionVersion()); err != nil {
		return err
	}
	if h.FileType != arjFileTypeMain {
		return ErrFormat
	}
	if err := validateMainHeaderLengths(h); err != nil {
		return err
	}
	if err := validateMainExtendedHeaders(h); err != nil {
		return err
	}
	return nil
}

func validateMainHeaderLengths(h *ArchiveHeader) error {
	firstSize := int(h.FirstHeaderSize)
	if firstSize == 0 {
		firstSize = arjMinFirstHeaderSize
	}
	if firstSize < arjMinFirstHeaderSize || firstSize > arjMaxBasicHeaderSize {
		return ErrFormat
	}
	if strings.IndexByte(h.Name, 0) >= 0 || strings.IndexByte(h.Comment, 0) >= 0 {
		return ErrFormat
	}
	if len(h.Name)+firstSize+2 > arjMaxBasicHeaderSize {
		return errLongArchiveName
	}
	if len(h.Comment)+firstSize+2 > arjMaxBasicHeaderSize {
		return errLongArchiveComment
	}
	if len(h.Name)+len(h.Comment)+firstSize+2 > arjMaxBasicHeaderSize {
		return errLongArchiveComment
	}
	return nil
}

func validateMainExtendedHeaders(h *ArchiveHeader) error {
	for _, ext := range h.MainExtendedHeaders {
		if len(ext) == 0 || len(ext) > 0xffff {
			return ErrFormat
		}
	}
	return nil
}

func normalizeMainFirstHeaderExtra(h *ArchiveHeader) error {
	if h.FirstHeaderSize == 0 {
		h.FirstHeaderSize = arjMinFirstHeaderSize
	}
	if h.FirstHeaderSize < arjMinFirstHeaderSize+archiveHeaderSecurityExtraSize &&
		(h.ProtectionBlocks != 0 || h.ProtectionFlags != 0 || h.ProtectionReserved != 0) {
		h.FirstHeaderSize = arjMinFirstHeaderSize + archiveHeaderSecurityExtraSize
	}
	firstSize := int(h.FirstHeaderSize)
	if firstSize < arjMinFirstHeaderSize || firstSize > arjMaxBasicHeaderSize {
		return ErrFormat
	}
	wantExtra := firstSize - arjMinFirstHeaderSize
	switch {
	case wantExtra == 0:
		h.FirstHeaderExtra = nil
	case len(h.FirstHeaderExtra) == wantExtra:
	case len(h.FirstHeaderExtra) > wantExtra:
		h.FirstHeaderExtra = append([]byte(nil), h.FirstHeaderExtra[:wantExtra]...)
	default:
		extra := make([]byte, wantExtra)
		copy(extra, h.FirstHeaderExtra)
		h.FirstHeaderExtra = extra
	}
	return nil
}

func cloneArchiveHeader(in ArchiveHeader) ArchiveHeader {
	out := in
	out.FirstHeaderExtra = append([]byte(nil), in.FirstHeaderExtra...)
	out.MainExtendedHeaders = cloneMainExtendedHeaders(in.MainExtendedHeaders)
	return out
}

func normalizeLocalFirstHeaderExtra(h *FileHeader) error {
	firstSize := int(h.FirstHeaderSize)
	if firstSize < arjMinFirstHeaderSize || firstSize > arjMaxBasicHeaderSize {
		return ErrFormat
	}
	wantExtra := firstSize - arjMinFirstHeaderSize
	switch {
	case wantExtra == 0:
		h.firstHeaderExtra = nil
	case len(h.firstHeaderExtra) == wantExtra:
	case len(h.firstHeaderExtra) > wantExtra:
		h.firstHeaderExtra = append([]byte(nil), h.firstHeaderExtra[:wantExtra]...)
	default:
		extra := make([]byte, wantExtra)
		copy(extra, h.firstHeaderExtra)
		h.firstHeaderExtra = extra
	}
	return nil
}

type entryBuffer struct {
	limit      uint64
	scope      string
	memLimit   uint64
	size       uint64
	mem        bytes.Buffer
	file       *os.File
	filePath   string
	removePath string
}

func newEntryBuffer(limit uint64, scope string) *entryBuffer {
	memLimit := limit
	if memLimit > maxInMemoryEntrySpoolSize {
		memLimit = maxInMemoryEntrySpoolSize
	}
	return &entryBuffer{
		limit:    limit,
		scope:    scope,
		memLimit: memLimit,
	}
}

func (b *entryBuffer) Size() uint64 {
	if b == nil {
		return 0
	}
	return b.size
}

func (b *entryBuffer) InMemorySize() uint64 {
	if b == nil {
		return 0
	}
	return uint64(b.mem.Len())
}

func (b *entryBuffer) TempPath() string {
	if b == nil {
		return ""
	}
	return b.filePath
}

func (b *entryBuffer) Spilled() bool {
	if b == nil {
		return false
	}
	return b.file != nil
}

func (b *entryBuffer) limitErr(attempted int) *BufferLimitError {
	return &BufferLimitError{
		Scope:     b.scope,
		Limit:     b.limit,
		Buffered:  b.size,
		Attempted: uint64(attempted),
	}
}

func (b *entryBuffer) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if b.size >= b.limit {
		return 0, b.limitErr(len(p))
	}

	remaining := b.limit - b.size
	chunk := p
	limited := false
	if uint64(len(chunk)) > remaining {
		chunk = chunk[:int(remaining)]
		limited = true
	}

	n, err := b.writeNoLimit(chunk)
	b.size += uint64(n)
	if err != nil {
		return n, err
	}
	if limited {
		return n, b.limitErr(len(p))
	}
	return n, nil
}

func (b *entryBuffer) writeNoLimit(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		if b.file == nil {
			memLen := uint64(b.mem.Len())
			if memLen < b.memLimit {
				space := b.memLimit - memLen
				if space > uint64(len(p)) {
					space = uint64(len(p))
				}
				n, _ := b.mem.Write(p[:int(space)])
				total += n
				p = p[n:]
				if len(p) == 0 {
					return total, nil
				}
			}
			if err := b.openSpillFile(); err != nil {
				return total, err
			}
		}

		n, err := b.file.Write(p)
		total += n
		p = p[n:]
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

func (b *entryBuffer) openSpillFile() error {
	if b.file != nil {
		return nil
	}

	f, err := os.CreateTemp("", "goarj-entry-*")
	if err != nil {
		return err
	}
	path := f.Name()
	if b.mem.Len() > 0 {
		if _, err := writeAll(f, b.mem.Bytes()); err != nil {
			_ = f.Close()
			_ = os.Remove(path)
			return err
		}
		b.mem.Reset()
	}

	b.file = f
	b.filePath = path
	b.removePath = path
	return nil
}

func (b *entryBuffer) WriteTo(dst io.Writer) (int64, error) {
	if b.file == nil {
		n, err := writeAll(dst, b.mem.Bytes())
		return int64(n), err
	}
	if _, err := b.file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	return io.CopyN(dst, b.file, int64(b.size))
}

func (b *entryBuffer) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, errors.New("arj: negative read offset")
	}
	if uint64(off) >= b.size {
		return 0, io.EOF
	}
	if b.file != nil {
		return b.file.ReadAt(p, off)
	}

	data := b.mem.Bytes()
	if off >= int64(len(data)) {
		return 0, io.EOF
	}
	n := copy(p, data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (b *entryBuffer) sliceAt(off int64, n int) ([]byte, error) {
	if n == 0 {
		return nil, nil
	}
	out := make([]byte, n)
	if err := readAtFull(b, out, off); err != nil {
		return nil, err
	}
	return out, nil
}

func (b *entryBuffer) Close() error {
	if b == nil {
		return nil
	}

	var err error
	if b.file != nil {
		if closeErr := b.file.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
		b.file = nil
	}
	if b.removePath != "" {
		if removeErr := os.Remove(b.removePath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, removeErr)
		}
		b.removePath = ""
	}
	b.mem.Reset()
	b.filePath = ""
	b.size = 0
	return err
}

type entryStreamSink struct {
	w     io.Writer
	limit uint64
	scope string
	size  uint64
}

func newEntryStreamSink(w io.Writer, limit uint64, scope string) *entryStreamSink {
	return &entryStreamSink{
		w:     w,
		limit: limit,
		scope: scope,
	}
}

func (w *entryStreamSink) Size() uint64 {
	if w == nil {
		return 0
	}
	return w.size
}

func (w *entryStreamSink) limitErr(attempted int) *BufferLimitError {
	return &BufferLimitError{
		Scope:     w.scope,
		Limit:     w.limit,
		Buffered:  w.size,
		Attempted: uint64(attempted),
	}
}

func (w *entryStreamSink) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if w.limit != 0 && w.size >= w.limit {
		return 0, w.limitErr(len(p))
	}

	chunk := p
	limited := false
	if w.limit != 0 {
		remaining := w.limit - w.size
		if uint64(len(chunk)) > remaining {
			chunk = chunk[:int(remaining)]
			limited = true
		}
	}

	n, err := w.w.Write(chunk)
	w.size += uint64(n)
	if err != nil {
		return n, err
	}
	if limited {
		return n, w.limitErr(len(p))
	}
	return n, nil
}

type countWriter struct {
	w     io.Writer
	count int64
}

func (w *countWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.count += int64(n)
	return n, err
}

func (w *countWriter) Count() int64 {
	if w == nil {
		return 0
	}
	return w.count
}

func cloneCompressorOverrides(src map[uint16]Compressor) map[uint16]Compressor {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[uint16]Compressor, len(src))
	for method, comp := range src {
		dst[method] = comp
	}
	return dst
}

func normalizeWriteBufferLimits(limits WriteBufferLimits) WriteBufferLimits {
	legacy := limits.MaxEntryBufferSize

	if limits.MaxCompressedEntryBufferSize == 0 {
		if legacy != 0 {
			limits.MaxCompressedEntryBufferSize = legacy
		} else {
			limits.MaxCompressedEntryBufferSize = DefaultMaxCompressedEntryBufferSize
		}
	}
	if limits.MaxPlainEntryBufferSize == 0 {
		if legacy != 0 {
			limits.MaxPlainEntryBufferSize = legacy
		} else {
			limits.MaxPlainEntryBufferSize = DefaultMaxPlainEntryBufferSize
		}
	}
	if limits.MaxEntryBufferSize == 0 &&
		limits.MaxCompressedEntryBufferSize == limits.MaxPlainEntryBufferSize {
		limits.MaxEntryBufferSize = limits.MaxCompressedEntryBufferSize
	}
	if limits.MaxMethod14InputBufferSize == 0 {
		limits.MaxMethod14InputBufferSize = DefaultMaxMethod14InputBufferSize
	}
	return limits
}

func newBufferLimitError(scope string, limit uint64, buf *bytes.Buffer, attempted int) *BufferLimitError {
	return &BufferLimitError{
		Scope:     scope,
		Limit:     limit,
		Buffered:  uint64(buf.Len()),
		Attempted: uint64(attempted),
	}
}

func writeBoundedBuffer(buf *bytes.Buffer, p []byte, limit uint64, scope string) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if uint64(buf.Len()) >= limit {
		return 0, newBufferLimitError(scope, limit, buf, len(p))
	}

	remaining := limit - uint64(buf.Len())
	if uint64(len(p)) > remaining {
		n, _ := buf.Write(p[:int(remaining)])
		return n, newBufferLimitError(scope, limit, buf, len(p))
	}
	return buf.Write(p)
}

type boundedBufferWriter struct {
	buf   *bytes.Buffer
	limit uint64
	scope string
}

func (w *boundedBufferWriter) Write(p []byte) (int, error) {
	return writeBoundedBuffer(w.buf, p, w.limit, w.scope)
}

func writeAll(w io.Writer, p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		n, err := w.Write(p)
		total += n
		p = p[n:]
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

func patchLocalFileHeader(ws io.WriteSeeker, off int64, h *FileHeader) error {
	if ws == nil {
		return errors.New("arj: nil seekable writer for local header patch")
	}
	if off < 0 {
		return errors.New("arj: negative local header patch offset")
	}

	var buf bytes.Buffer
	if err := writeLocalFileHeader(&buf, h); err != nil {
		return err
	}

	cur, err := ws.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if _, err := ws.Seek(off, io.SeekStart); err != nil {
		return err
	}

	writeErr := error(nil)
	if _, err := writeAll(ws, buf.Bytes()); err != nil {
		writeErr = errors.Join(writeErr, err)
	}
	if _, err := ws.Seek(cur, io.SeekStart); err != nil {
		writeErr = errors.Join(writeErr, err)
	}
	return writeErr
}
