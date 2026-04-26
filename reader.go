package arj

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sync"
)

const mainHeaderScanChunkSize = 64 << 10

const (
	mainHeaderProbeBudgetFloor      int64 = 1 << 20
	mainHeaderProbeBudgetMultiplier int64 = 32
	// DefaultMainHeaderProbeBudgetMax caps adaptive main-header probe work.
	DefaultMainHeaderProbeBudgetMax int64 = 128 << 20
)

const (
	// DefaultMethod14MaxCompressedSize is the default maximum compressed size
	// accepted for method 1-4 decode in bytes.
	DefaultMethod14MaxCompressedSize uint64 = 128 << 20
	// DefaultMethod14MaxUncompressedSize is the default maximum uncompressed
	// size accepted for method 1-4 decode in bytes.
	DefaultMethod14MaxUncompressedSize uint64 = 512 << 20
)

const (
	// DefaultParserMaxEntries is the default maximum number of local file
	// headers accepted while parsing an archive.
	DefaultParserMaxEntries = 1 << 16
	// DefaultParserMaxExtendedHeaders is the default maximum number of
	// extended-header records accepted in a single header block.
	DefaultParserMaxExtendedHeaders = 256
	// DefaultParserMaxExtendedHeaderBytes is the default maximum aggregate
	// extended-header payload bytes accepted in a single header block.
	DefaultParserMaxExtendedHeaderBytes int64 = 8 << 20
)

// Method14DecodeLimits controls resource limits for native method 1-4 decode.
// Zero values fall back to package defaults.
type Method14DecodeLimits struct {
	MaxCompressedSize   uint64
	MaxUncompressedSize uint64
}

// ParserLimits controls parser safety bounds.
// Zero values fall back to package defaults.
type ParserLimits struct {
	MaxEntries             int
	MaxExtendedHeaders     int
	MaxExtendedHeaderBytes int64
}

// ReaderOptions configures NewReaderWithOptions/OpenReaderWithOptions.
type ReaderOptions struct {
	ParserLimits ParserLimits
	// MainHeaderProbeBudget limits candidate-probing work while searching for
	// the archive main header offset. Zero uses an adaptive default capped at
	// DefaultMainHeaderProbeBudgetMax.
	MainHeaderProbeBudget int64
}

var mainHeaderScanBufPool = sync.Pool{
	New: func() any {
		return make([]byte, mainHeaderScanChunkSize)
	},
}

var (
	// ErrInvalidReaderSize indicates a negative Reader size argument.
	ErrInvalidReaderSize = errors.New("arj: size cannot be negative")
	// ErrInvalidParserMaxEntries indicates ParserLimits.MaxEntries is invalid.
	ErrInvalidParserMaxEntries = errors.New("arj: ParserLimits.MaxEntries must be >= 0")
	// ErrInvalidParserMaxExtendedHeaders indicates ParserLimits.MaxExtendedHeaders is invalid.
	ErrInvalidParserMaxExtendedHeaders = errors.New("arj: ParserLimits.MaxExtendedHeaders must be >= 0")
	// ErrInvalidParserMaxExtendedHeaderBytes indicates ParserLimits.MaxExtendedHeaderBytes is invalid.
	ErrInvalidParserMaxExtendedHeaderBytes = errors.New("arj: ParserLimits.MaxExtendedHeaderBytes must be >= 0")
	// ErrInvalidMainHeaderProbeBudget indicates ReaderOptions.MainHeaderProbeBudget is invalid.
	ErrInvalidMainHeaderProbeBudget = errors.New("arj: main header probe budget must be >= 0")

	errMainHeaderProbeBudgetExceeded = errors.New("arj: main header probe budget exceeded")
)

// A Reader serves content from an ARJ archive.
type Reader struct {
	r             io.ReaderAt
	File          []*File
	ArchiveHeader ArchiveHeader
	ArchiveName   string
	Comment       string

	stateMu sync.RWMutex

	decompressors map[uint16]Decompressor
	password      []byte
	method14Limit Method14DecodeLimits
	fsIndex       *readerFSIndex

	baseOffset int64
}

// A ReadCloser is a Reader that must be closed when no longer needed.
type ReadCloser struct {
	f *os.File
	Reader
}

// A File is a single file in an ARJ archive.
type File struct {
	FileHeader
	arj        *Reader
	dataOffset int64
	segments   []fileSegment
}

type fileSegment struct {
	dataOffset       int64
	method           uint16
	flags            uint8
	extFlags         uint8
	passwordModifier uint8
	compressedSize   uint64
	uncompressedSize uint64
	crc32            uint32
}

// OpenReader opens the ARJ file specified by name and returns a ReadCloser.
func OpenReader(name string) (*ReadCloser, error) {
	return OpenReaderWithOptions(name, ReaderOptions{})
}

// OpenReaderWithOptions opens the ARJ file specified by name and returns a
// ReadCloser with configurable reader limits.
func OpenReaderWithOptions(name string, opts ReaderOptions) (*ReadCloser, error) {
	if err := validateReaderOptions(opts); err != nil {
		return nil, err
	}
	opts = normalizeReaderOptions(opts)

	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	r := new(ReadCloser)
	if err := r.init(f, fi.Size(), opts.ParserLimits, opts.MainHeaderProbeBudget); err != nil {
		_ = f.Close()
		return nil, err
	}
	r.f = f
	return r, nil
}

// NewReader returns a new Reader reading from r, which is assumed to
// have the given size in bytes.
func NewReader(r io.ReaderAt, size int64) (*Reader, error) {
	return NewReaderWithOptions(r, size, ReaderOptions{})
}

// NewReaderWithOptions returns a new Reader reading from r, which is assumed
// to have the given size in bytes, with configurable reader limits.
func NewReaderWithOptions(r io.ReaderAt, size int64, opts ReaderOptions) (*Reader, error) {
	if r == nil {
		return nil, ErrFormat
	}
	if size < 0 {
		return nil, ErrInvalidReaderSize
	}
	if err := validateReaderOptions(opts); err != nil {
		return nil, err
	}
	opts = normalizeReaderOptions(opts)

	zr := new(Reader)
	if err := zr.init(r, size, opts.ParserLimits, opts.MainHeaderProbeBudget); err != nil {
		return nil, err
	}
	return zr, nil
}

func validateReaderOptions(opts ReaderOptions) error {
	if opts.MainHeaderProbeBudget < 0 {
		return ErrInvalidMainHeaderProbeBudget
	}
	return validateParserLimits(opts.ParserLimits)
}

func normalizeReaderOptions(opts ReaderOptions) ReaderOptions {
	opts.ParserLimits = normalizeParserLimits(opts.ParserLimits)
	return opts
}

func validateParserLimits(limits ParserLimits) error {
	if limits.MaxEntries < 0 {
		return ErrInvalidParserMaxEntries
	}
	if limits.MaxExtendedHeaders < 0 {
		return ErrInvalidParserMaxExtendedHeaders
	}
	if limits.MaxExtendedHeaderBytes < 0 {
		return ErrInvalidParserMaxExtendedHeaderBytes
	}
	return nil
}

func normalizeParserLimits(limits ParserLimits) ParserLimits {
	if limits.MaxEntries == 0 {
		limits.MaxEntries = DefaultParserMaxEntries
	}
	if limits.MaxExtendedHeaders == 0 {
		limits.MaxExtendedHeaders = DefaultParserMaxExtendedHeaders
	}
	if limits.MaxExtendedHeaderBytes == 0 {
		limits.MaxExtendedHeaderBytes = DefaultParserMaxExtendedHeaderBytes
	}
	return limits
}

func parserEntryLimitError(limit int) error {
	return fmt.Errorf("%w: max entries exceeded (limit: %d)", ErrFormat, limit)
}

func parserExtendedHeaderCountLimitError(limit int) error {
	return fmt.Errorf("%w: max extended headers exceeded (limit: %d)", ErrFormat, limit)
}

func parserExtendedHeaderBytesLimitError(limit int64) error {
	return fmt.Errorf("%w: max extended header bytes exceeded (limit: %d)", ErrFormat, limit)
}

func fitsRange(total, off, span int64) bool {
	if total < 0 || off < 0 || span < 0 {
		return false
	}
	if off > total {
		return false
	}
	return span <= total-off
}

func advanceOffsetWithinSize(off int64, delta uint64, total int64) (int64, bool) {
	if !fitsRange(total, off, 0) {
		return 0, false
	}
	remaining := uint64(total - off)
	if delta > remaining {
		return 0, false
	}
	return off + int64(delta), true
}

// SetPassword configures the default password for opening encrypted entries.
// It applies to File.Open; File.OpenWithPassword can override per file.
func (r *Reader) SetPassword(password string) {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	clearBytes(r.password)
	r.password = append(r.password[:0], password...)
}

// SetMethod14DecodeLimits overrides native method 1-4 decode limits.
// Any zero field keeps the corresponding package default.
func (r *Reader) SetMethod14DecodeLimits(limits Method14DecodeLimits) {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	r.method14Limit = normalizeMethod14DecodeLimits(limits)
}

func (r *Reader) passwordBytes() []byte {
	r.stateMu.RLock()
	defer r.stateMu.RUnlock()
	if len(r.password) == 0 {
		return nil
	}
	return append([]byte(nil), r.password...)
}

func (r *Reader) method14DecodeLimits() Method14DecodeLimits {
	r.stateMu.RLock()
	defer r.stateMu.RUnlock()
	return normalizeMethod14DecodeLimits(r.method14Limit)
}

// BaseOffset returns the offset where the first ARJ main header was found.
// It is zero for regular archives and non-zero for embedded/SFX layouts.
func (r *Reader) BaseOffset() int64 {
	return r.baseOffset
}

func (r *Reader) init(rdr io.ReaderAt, size int64, parserLimits ParserLimits, mainHeaderProbeBudget int64) error {
	limits := normalizeParserLimits(parserLimits)

	start, err := findMainHeaderOffsetWithBudgetAndLimits(
		rdr,
		size,
		newMainHeaderProbeBudget(size, mainHeaderProbeBudget),
		limits,
	)
	if err != nil {
		return err
	}
	r.r = rdr
	r.method14Limit = normalizeMethod14DecodeLimits(Method14DecodeLimits{})
	r.baseOffset = start

	off := start
	main, mainExt, next, err := readHeaderBlockWithLimits(rdr, size, off, limits)
	if err != nil {
		return err
	}
	if len(main) == 0 {
		return ErrFormat
	}
	archiveHeader, err := parseMainHeaderOwned(main, mainExt)
	if err != nil {
		return err
	}
	r.ArchiveHeader = archiveHeader
	r.ArchiveName = archiveHeader.Name
	r.Comment = archiveHeader.Comment
	off = next

	for {
		basic, extHeaders, afterHeader, err := readHeaderBlockWithLimits(rdr, size, off, limits)
		if err != nil {
			return err
		}
		if len(basic) == 0 {
			return nil
		}
		if len(r.File) >= limits.MaxEntries {
			return parserEntryLimitError(limits.MaxEntries)
		}

		f, err := parseLocalFileHeaderOwned(basic, extHeaders, r)
		if err != nil {
			return err
		}
		f.dataOffset = afterHeader
		nextOff, ok := advanceOffsetWithinSize(afterHeader, f.CompressedSize64, size)
		if !ok {
			return ErrFormat
		}

		r.File = append(r.File, f)
		off = nextOff
	}
}

// RegisterDecompressor registers or overrides a custom decompressor for a
// specific method ID. If a decompressor for a given method is not found,
// Reader defaults to the package-level registration.
// Passing nil explicitly disables the method on this Reader instance.
func (r *Reader) RegisterDecompressor(method uint16, dcomp Decompressor) {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	if r.decompressors == nil {
		r.decompressors = make(map[uint16]Decompressor)
	}
	r.decompressors[method] = dcomp
}

func (r *Reader) decompressor(method uint16) Decompressor {
	r.stateMu.RLock()
	defer r.stateMu.RUnlock()
	if r.decompressors != nil {
		if dcomp, ok := r.decompressors[method]; ok {
			return dcomp
		}
	}
	return decompressor(method)
}

// Close closes the ARJ file, rendering it unusable for I/O.
func (rc *ReadCloser) Close() error {
	if rc == nil {
		return nil
	}
	rc.stateMu.Lock()
	clearBytes(rc.password)
	rc.password = nil
	rc.stateMu.Unlock()
	if rc.f == nil {
		return nil
	}
	return rc.f.Close()
}

// DataOffset returns the offset of the file's compressed data relative to the
// beginning of the underlying stream. For embedded/SFX layouts, subtract
// Reader.BaseOffset to obtain an archive-relative data offset.
func (f *File) DataOffset() (int64, error) {
	if err := f.validateOpenState(); err != nil {
		return 0, err
	}
	return f.dataOffset, nil
}

// Open returns a ReadCloser that provides access to the File contents.
func (f *File) Open() (io.ReadCloser, error) {
	if err := f.validateOpenState(); err != nil {
		return nil, err
	}

	password := f.arj.passwordBytes()
	defer clearBytes(password)
	return f.openWithPassword(password)
}

// OpenWithPassword opens the file using a per-call password override.
func (f *File) OpenWithPassword(password string) (io.ReadCloser, error) {
	if err := f.validateOpenState(); err != nil {
		return nil, err
	}
	buf := []byte(password)
	defer clearBytes(buf)
	return f.openWithPassword(buf)
}

func (f *File) openWithPassword(password []byte) (io.ReadCloser, error) {
	if err := f.validateOpenState(); err != nil {
		return nil, err
	}
	if f.isDir() {
		if err := f.unsupportedOpenModeError(password); err != nil {
			return nil, err
		}
		return &dirReader{err: io.EOF}, nil
	}

	passwordCopy := append([]byte(nil), password...)
	if len(f.segments) == 0 {
		rc, err := f.openSegment(f.singleSegment(), passwordCopy)
		clearBytes(passwordCopy)
		return rc, err
	}
	segments := append([]fileSegment(nil), f.segments...)

	rc := &multiSegmentReadCloser{
		segments: segments,
		password: passwordCopy,
		openPart: func(segment fileSegment) (io.ReadCloser, error) {
			return f.openSegment(segment, passwordCopy)
		},
	}
	if err := rc.openCurrent(); err != nil {
		rc.scrubPassword()
		return nil, err
	}
	return rc, nil
}

func (f *File) validateOpenState() error {
	if f == nil || f.arj == nil || f.arj.r == nil {
		return ErrFormat
	}
	return nil
}

func (f *File) unsupportedOpenModeError(password []byte) error {
	return f.unsupportedOpenModeErrorForHeader(f.FileHeader, password)
}

func (f *File) unsupportedOpenModeErrorForHeader(h FileHeader, password []byte) error {
	if err := unsupportedEncryptionError(h.Flags, h.EncryptionVersion(), password); err != nil {
		return err
	}
	if err := unsupportedSecuredFlagsError(h.Flags); err != nil {
		return err
	}
	if f.arj != nil {
		if err := unsupportedMainSecurityFlagsError(f.arj.ArchiveHeader.Flags, f.arj.ArchiveHeader.EncryptionVersion()); err != nil {
			return err
		}
	}
	return nil
}

func (f *File) segmentList() []fileSegment {
	if len(f.segments) != 0 {
		return f.segments
	}
	return []fileSegment{f.singleSegment()}
}

func (f *File) singleSegment() fileSegment {
	return fileSegment{
		dataOffset:       f.dataOffset,
		method:           f.Method,
		flags:            f.Flags,
		extFlags:         f.ExtFlags,
		passwordModifier: f.PasswordModifier,
		compressedSize:   f.CompressedSize64,
		uncompressedSize: f.UncompressedSize64,
		crc32:            f.CRC32,
	}
}

func (f *File) openSegment(segment fileSegment, password []byte) (io.ReadCloser, error) {
	const maxInt64 = int64(^uint64(0) >> 1)
	if err := f.validateOpenState(); err != nil {
		return nil, err
	}

	header := FileHeader{
		Flags:            segment.flags,
		ExtFlags:         segment.extFlags,
		PasswordModifier: segment.passwordModifier,
	}
	if err := f.unsupportedOpenModeErrorForHeader(header, password); err != nil {
		return nil, err
	}
	if segment.compressedSize > uint64(maxInt64) {
		return nil, ErrFormat
	}

	limits := normalizeMethod14DecodeLimits(Method14DecodeLimits{})
	if f.arj != nil {
		limits = f.arj.method14DecodeLimits()
	}
	if isNativeMethod14(segment.method) {
		if err := validateMethod14DecodeSizes(limits, segment.compressedSize, segment.uncompressedSize); err != nil {
			return nil, err
		}
	}

	size := int64(segment.compressedSize)
	section := io.NewSectionReader(f.arj.r, segment.dataOffset, size)
	dcomp := f.arj.decompressor(segment.method)
	if dcomp == nil {
		return nil, ErrAlgorithm
	}

	in := io.Reader(section)
	var garbled *garbledReader
	if segment.flags&FlagGarbled != 0 {
		garbled = newGarbledReader(in, password, segment.passwordModifier)
		in = garbled
	}
	if isNativeMethod14(segment.method) {
		in = wrapMethod14DecompressorInput(in, size, segment.uncompressedSize, limits)
	}
	rc := dcomp(in)
	if rc == nil {
		if garbled != nil {
			garbled.clearSensitiveData()
		}
		return nil, ErrAlgorithm
	}
	if garbled != nil {
		rc = &garbledReaderCloser{
			ReadCloser: rc,
			garbled:    garbled,
		}
	}
	return &checksumReader{
		rc:      rc,
		wantCRC: segment.crc32,
		wantN:   segment.uncompressedSize,
	}, nil
}

type checksumReader struct {
	rc      io.ReadCloser
	crc     uint32
	readN   uint64
	wantN   uint64
	wantCRC uint32
	err     error
}

func (r *checksumReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}

	n, err := r.rc.Read(p)
	if n > 0 {
		r.crc = crc32.Update(r.crc, crc32.IEEETable, p[:n])
		r.readN += uint64(n)
	}
	if err == io.EOF {
		if r.readN != r.wantN {
			r.err = ErrFormat
			return n, r.err
		}
		if r.crc != r.wantCRC {
			r.err = ErrChecksum
			return n, r.err
		}
	}
	return n, err
}

func (r *checksumReader) Close() error {
	if r == nil {
		return nil
	}
	if r.rc == nil {
		return nil
	}
	return r.rc.Close()
}

type garbledReaderCloser struct {
	io.ReadCloser
	garbled *garbledReader
}

func (r *garbledReaderCloser) Close() error {
	if r.garbled != nil {
		r.garbled.clearSensitiveData()
		r.garbled = nil
	}
	if r.ReadCloser == nil {
		return nil
	}
	return r.ReadCloser.Close()
}

type multiSegmentReadCloser struct {
	segments []fileSegment
	password []byte
	openPart func(fileSegment) (io.ReadCloser, error)
	current  io.ReadCloser
	index    int
	err      error
}

func (r *multiSegmentReadCloser) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	for r.index < len(r.segments) {
		if r.current == nil {
			if err := r.openCurrent(); err != nil {
				return 0, r.fail(err)
			}
		}

		n, err := r.current.Read(p)
		if err == io.EOF {
			closeErr := r.closeCurrent()
			r.index++
			if closeErr != nil {
				return n, r.fail(closeErr)
			}
			if n > 0 {
				return n, nil
			}
			continue
		}
		if err != nil {
			return n, r.fail(err)
		}
		return n, err
	}
	r.scrubPassword()
	return 0, io.EOF
}

func (r *multiSegmentReadCloser) Close() error {
	err := errors.Join(r.err, r.closeCurrent())
	r.index = len(r.segments)
	r.scrubPassword()
	return err
}

func (r *multiSegmentReadCloser) openCurrent() error {
	if r.current != nil || r.index >= len(r.segments) {
		return nil
	}
	if r.openPart == nil {
		return ErrFormat
	}
	part, err := r.openPart(r.segments[r.index])
	if err != nil {
		return err
	}
	r.current = part
	return nil
}

func (r *multiSegmentReadCloser) closeCurrent() error {
	if r.current == nil {
		return nil
	}
	err := r.current.Close()
	r.current = nil
	return err
}

func (r *multiSegmentReadCloser) scrubPassword() {
	clearBytes(r.password)
	r.password = nil
}

func (r *multiSegmentReadCloser) fail(err error) error {
	if err == nil {
		return nil
	}
	if r.err == nil {
		r.err = err
	}
	r.scrubPassword()
	return err
}

type dirReader struct {
	err error
}

func (r *dirReader) Read([]byte) (int, error) {
	return 0, r.err
}

func (r *dirReader) Close() error {
	return nil
}

func findMainHeaderOffset(r io.ReaderAt, size int64) (int64, error) {
	return findMainHeaderOffsetWithBudgetAndLimits(
		r,
		size,
		newMainHeaderProbeBudget(size, 0),
		normalizeParserLimits(ParserLimits{}),
	)
}

func findMainHeaderOffsetWithLimits(r io.ReaderAt, size int64, limits ParserLimits) (int64, error) {
	return findMainHeaderOffsetWithBudgetAndLimits(r, size, newMainHeaderProbeBudget(size, 0), limits)
}

func findMainHeaderOffsetWithBudget(r io.ReaderAt, size int64, budget *mainHeaderProbeBudget) (int64, error) {
	return findMainHeaderOffsetWithBudgetAndLimits(r, size, budget, normalizeParserLimits(ParserLimits{}))
}

func findMainHeaderOffsetWithBudgetAndLimits(r io.ReaderAt, size int64, budget *mainHeaderProbeBudget, limits ParserLimits) (int64, error) {
	type headerCandidate struct {
		off   int64
		files int
		end   int64
	}

	var (
		best  headerCandidate
		found bool
	)
	if size < 2 {
		return 0, ErrFormat
	}
	limits = normalizeParserLimits(limits)
	if budget == nil {
		budget = newMainHeaderProbeBudget(size, 0)
	}
	scanReader := r
	probeReader := &budgetedReaderAt{r: r, budget: budget}

	files, end, ok, probeErr := probeArchiveLayoutWithLimits(probeReader, size, 0, limits)
	if probeErr != nil {
		return 0, normalizeMainHeaderProbeError(probeErr)
	}
	if ok && (files > 0 || end == size) {
		return 0, nil
	}

	buf := mainHeaderScanBufPool.Get().([]byte)
	defer mainHeaderScanBufPool.Put(buf)
	signature := [2]byte{arjHeaderID1, arjHeaderID2}

	var (
		prevLast byte
		havePrev bool
	)
	for off := int64(0); off < size; {
		n := int64(len(buf))
		if remain := size - off; remain < n {
			n = remain
		}
		chunk := buf[:n]
		if err := readAtFull(scanReader, chunk, off); err != nil {
			return 0, normalizeMainHeaderProbeError(normalizeHeaderReadError(err))
		}

		if havePrev && len(chunk) != 0 && prevLast == arjHeaderID1 && chunk[0] == arjHeaderID2 {
			candidateOff := off - 1
			prefilterPass := true
			if len(chunk) >= 3 {
				basicSize := int(binary.LittleEndian.Uint16(chunk[1:3]))
				prefilterPass = mainHeaderCandidateBasicSizePassesPrefilter(size, candidateOff, basicSize)
			}
			if prefilterPass {
				files, end, ok, probeErr := probeArchiveLayoutWithLimits(probeReader, size, candidateOff, limits)
				if probeErr != nil {
					return 0, normalizeMainHeaderProbeError(probeErr)
				}
				if ok && end == size {
					return candidateOff, nil
				}
				if ok && (!found || files > best.files || (files == best.files && end > best.end)) {
					best = headerCandidate{off: candidateOff, files: files, end: end}
					found = true
				}
			}
		}

		limit := len(chunk) - 1
		for i := 0; i < limit; {
			next := bytes.Index(chunk[i:], signature[:])
			if next < 0 {
				break
			}
			pos := i + next
			candidateOff := off + int64(pos)
			if pos+3 < len(chunk) {
				basicSize := int(binary.LittleEndian.Uint16(chunk[pos+2 : pos+4]))
				if !mainHeaderCandidateBasicSizePassesPrefilter(size, candidateOff, basicSize) {
					i = pos + 1
					continue
				}
			}
			files, end, ok, probeErr := probeArchiveLayoutWithLimits(probeReader, size, candidateOff, limits)
			if probeErr != nil {
				return 0, normalizeMainHeaderProbeError(probeErr)
			}
			if ok && end == size {
				return candidateOff, nil
			}
			if ok && (!found || files > best.files || (files == best.files && end > best.end)) {
				best = headerCandidate{off: candidateOff, files: files, end: end}
				found = true
			}
			i = pos + 1
		}

		if len(chunk) != 0 {
			prevLast = chunk[len(chunk)-1]
			havePrev = true
		}
		off += n
	}
	if !found {
		return 0, ErrFormat
	}
	return best.off, nil
}

func normalizeMainHeaderProbeError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, errMainHeaderProbeBudgetExceeded) {
		return ErrFormat
	}
	return err
}

type mainHeaderProbeBudget struct {
	remaining int64
}

func newMainHeaderProbeBudget(size int64, configured int64) *mainHeaderProbeBudget {
	if configured > 0 {
		return &mainHeaderProbeBudget{remaining: configured}
	}

	budget := mainHeaderProbeBudgetFloor
	if size > 0 {
		if size > math.MaxInt64/mainHeaderProbeBudgetMultiplier {
			budget = math.MaxInt64
		} else {
			scaled := size * mainHeaderProbeBudgetMultiplier
			if scaled > budget {
				budget = scaled
			}
		}
	}
	if budget > DefaultMainHeaderProbeBudgetMax {
		budget = DefaultMainHeaderProbeBudgetMax
	}
	return &mainHeaderProbeBudget{remaining: budget}
}

func mainHeaderCandidateBasicSizePassesPrefilter(size, off int64, basicSize int) bool {
	if basicSize < arjMinFirstHeaderSize || basicSize > arjMaxBasicHeaderSize {
		return false
	}
	return fitsRange(size, off+4, int64(basicSize)+4)
}

func (b *mainHeaderProbeBudget) consume(n int) error {
	if b == nil || n <= 0 {
		return nil
	}
	if b.remaining <= 0 {
		return errMainHeaderProbeBudgetExceeded
	}
	cost := int64(n)
	if cost > b.remaining {
		b.remaining = 0
		return errMainHeaderProbeBudgetExceeded
	}
	b.remaining -= cost
	return nil
}

type budgetedReaderAt struct {
	r      io.ReaderAt
	budget *mainHeaderProbeBudget
}

func (r *budgetedReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if err := r.budget.consume(len(p)); err != nil {
		return 0, err
	}
	return r.r.ReadAt(p, off)
}

func probeArchiveLayout(r io.ReaderAt, size, off int64) (files int, end int64, ok bool, err error) {
	return probeArchiveLayoutWithLimits(r, size, off, normalizeParserLimits(ParserLimits{}))
}

func probeArchiveLayoutWithLimits(r io.ReaderAt, size, off int64, limits ParserLimits) (files int, end int64, ok bool, err error) {
	limits = normalizeParserLimits(limits)

	mainBasic, mainExt, next, err := readHeaderBlockWithLimits(r, size, off, limits)
	if err != nil {
		if errors.Is(err, ErrFormat) {
			return 0, 0, false, nil
		}
		return 0, 0, false, err
	}
	if len(mainBasic) == 0 {
		return 0, 0, false, nil
	}
	_ = mainExt
	if !probeMainHeaderBasicValid(mainBasic) {
		return 0, 0, false, nil
	}

	off = next
	for {
		basic, extHeaders, afterHeader, err := readHeaderBlockWithLimits(r, size, off, limits)
		if err != nil {
			if errors.Is(err, ErrFormat) {
				return 0, 0, false, nil
			}
			return 0, 0, false, err
		}
		if len(basic) == 0 {
			return files, afterHeader, true, nil
		}
		if files >= limits.MaxEntries {
			return 0, 0, false, nil
		}

		_ = extHeaders
		compressedSize, ok := probeLocalHeaderCompressedSize(basic)
		if !ok {
			return 0, 0, false, nil
		}
		nextOff, ok := advanceOffsetWithinSize(afterHeader, compressedSize, size)
		if !ok {
			return 0, 0, false, nil
		}

		files++
		off = nextOff
	}
}

func probeMainHeaderBasicValid(basic []byte) bool {
	if len(basic) < arjMinFirstHeaderSize {
		return false
	}
	firstSize := int(basic[0])
	if firstSize < arjMinFirstHeaderSize || firstSize > len(basic) {
		return false
	}
	if basic[6] != arjFileTypeMain {
		return false
	}
	return probeHeaderNameCommentValid(basic, firstSize)
}

func probeLocalHeaderCompressedSize(basic []byte) (uint64, bool) {
	if len(basic) < arjMinFirstHeaderSize {
		return 0, false
	}
	firstSize := int(basic[0])
	if firstSize < arjMinFirstHeaderSize || firstSize > len(basic) {
		return 0, false
	}
	if !probeHeaderNameCommentValid(basic, firstSize) {
		return 0, false
	}
	return uint64(binary.LittleEndian.Uint32(basic[12:16])), true
}

func probeHeaderNameCommentValid(basic []byte, firstSize int) bool {
	rest := basic[firstSize:]
	i := bytes.IndexByte(rest, 0)
	if i < 0 {
		return false
	}
	rest = rest[i+1:]
	return bytes.IndexByte(rest, 0) >= 0
}

func readHeaderBlock(r io.ReaderAt, size, off int64) (basic []byte, extHeaders [][]byte, nextOff int64, err error) {
	return readHeaderBlockWithLimits(r, size, off, normalizeParserLimits(ParserLimits{}))
}

func readHeaderBlockWithLimits(r io.ReaderAt, size, off int64, limits ParserLimits) (basic []byte, extHeaders [][]byte, nextOff int64, err error) {
	limits = normalizeParserLimits(limits)

	var pre [4]byte
	if !fitsRange(size, off, int64(len(pre))) {
		return nil, nil, off, ErrFormat
	}
	if err := readAtFull(r, pre[:], off); err != nil {
		return nil, nil, off, normalizeHeaderReadError(err)
	}
	if pre[0] != arjHeaderID1 || pre[1] != arjHeaderID2 {
		return nil, nil, off, ErrFormat
	}

	basicSize := int(binary.LittleEndian.Uint16(pre[2:4]))
	off += 4
	if basicSize == 0 {
		return nil, nil, off, nil
	}
	if basicSize < arjMinFirstHeaderSize || basicSize > arjMaxBasicHeaderSize {
		return nil, nil, off, ErrFormat
	}
	if !fitsRange(size, off, int64(basicSize)+4) {
		return nil, nil, off, ErrFormat
	}

	basic = make([]byte, basicSize)
	if err := readAtFull(r, basic, off); err != nil {
		return nil, nil, off, normalizeHeaderReadError(err)
	}
	off += int64(basicSize)

	var crcBuf [4]byte
	if err := readAtFull(r, crcBuf[:], off); err != nil {
		return nil, nil, off, normalizeHeaderReadError(err)
	}
	wantCRC := binary.LittleEndian.Uint32(crcBuf[:])
	if crc32.ChecksumIEEE(basic) != wantCRC {
		return nil, nil, off, ErrFormat
	}
	off += 4

	extCount := 0
	var extBytes int64
	for {
		var sz [2]byte
		if !fitsRange(size, off, int64(len(sz))) {
			return nil, nil, off, ErrFormat
		}
		if err := readAtFull(r, sz[:], off); err != nil {
			return nil, nil, off, normalizeHeaderReadError(err)
		}
		off += 2
		extSize := int(binary.LittleEndian.Uint16(sz[:]))
		if extSize == 0 {
			break
		}
		extCount++
		if extCount > limits.MaxExtendedHeaders {
			return nil, nil, off, parserExtendedHeaderCountLimitError(limits.MaxExtendedHeaders)
		}
		extBytes += int64(extSize)
		if extBytes < 0 || extBytes > limits.MaxExtendedHeaderBytes {
			return nil, nil, off, parserExtendedHeaderBytesLimitError(limits.MaxExtendedHeaderBytes)
		}
		if !fitsRange(size, off, int64(extSize)+4) {
			return nil, nil, off, ErrFormat
		}
		ext := make([]byte, extSize)
		if err := readAtFull(r, ext, off); err != nil {
			return nil, nil, off, normalizeHeaderReadError(err)
		}
		off += int64(extSize)

		if err := readAtFull(r, crcBuf[:], off); err != nil {
			return nil, nil, off, normalizeHeaderReadError(err)
		}
		if crc32.ChecksumIEEE(ext) != binary.LittleEndian.Uint32(crcBuf[:]) {
			return nil, nil, off, ErrFormat
		}
		off += 4
		extHeaders = append(extHeaders, ext)
	}

	return basic, extHeaders, off, nil
}

func parseMainHeader(basic []byte, extHeaders [][]byte) (ArchiveHeader, error) {
	return parseMainHeaderWithCloneMode(basic, extHeaders, true)
}

func parseMainHeaderOwned(basic []byte, extHeaders [][]byte) (ArchiveHeader, error) {
	return parseMainHeaderWithCloneMode(basic, extHeaders, false)
}

func parseMainHeaderWithCloneMode(basic []byte, extHeaders [][]byte, cloneExt bool) (ArchiveHeader, error) {
	if len(basic) < arjMinFirstHeaderSize {
		return ArchiveHeader{}, ErrFormat
	}
	firstSize := int(basic[0])
	if firstSize < arjMinFirstHeaderSize || firstSize > len(basic) {
		return ArchiveHeader{}, ErrFormat
	}
	if basic[6] != arjFileTypeMain {
		return ArchiveHeader{}, ErrFormat
	}
	hostData := binary.LittleEndian.Uint16(basic[28:30])
	extFlags, chapterNumber := unpackExtMetadata(hostData)
	firstHeaderExtra := append([]byte(nil), basic[arjMinFirstHeaderSize:firstSize]...)
	protectionBlocks, protectionFlags, protectionReserved := unpackArchiveSecurityExtra(firstHeaderExtra)

	rest := basic[firstSize:]
	archiveName, rest, ok := cutCString(rest)
	if !ok {
		return ArchiveHeader{}, ErrFormat
	}
	comment, _, ok := cutCString(rest)
	if !ok {
		return ArchiveHeader{}, ErrFormat
	}

	mainExtendedHeaders := extHeaders
	if cloneExt {
		mainExtendedHeaders = cloneMainExtendedHeaders(extHeaders)
	}

	return ArchiveHeader{
		FirstHeaderSize:      uint8(firstSize),
		ArchiverVersion:      basic[1],
		MinVersion:           basic[2],
		HostOS:               basic[3],
		Flags:                basic[4],
		SecurityVersion:      basic[5],
		FileType:             basic[6],
		Reserved:             basic[7],
		Created:              dosDateTimeToTime(binary.LittleEndian.Uint32(basic[8:12])),
		Modified:             dosDateTimeToTime(binary.LittleEndian.Uint32(basic[12:16])),
		ArchiveSize:          binary.LittleEndian.Uint32(basic[16:20]),
		SecurityEnvelopePos:  binary.LittleEndian.Uint32(basic[20:24]),
		FilespecPos:          binary.LittleEndian.Uint16(basic[24:26]),
		SecurityEnvelopeSize: binary.LittleEndian.Uint16(basic[26:28]),
		HostData:             hostData,
		ExtFlags:             extFlags,
		ChapterNumber:        chapterNumber,
		ProtectionBlocks:     protectionBlocks,
		ProtectionFlags:      protectionFlags,
		ProtectionReserved:   protectionReserved,
		Name:                 archiveName,
		Comment:              comment,
		MainExtendedHeaders:  mainExtendedHeaders,
		FirstHeaderExtra:     firstHeaderExtra,
	}, nil
}

func parseLocalFileHeader(basic []byte, extHeaders [][]byte, r *Reader) (*File, error) {
	return parseLocalFileHeaderWithCloneMode(basic, extHeaders, r, true)
}

func parseLocalFileHeaderOwned(basic []byte, extHeaders [][]byte, r *Reader) (*File, error) {
	return parseLocalFileHeaderWithCloneMode(basic, extHeaders, r, false)
}

func parseLocalFileHeaderWithCloneMode(basic []byte, extHeaders [][]byte, r *Reader, cloneExt bool) (*File, error) {
	if len(basic) < arjMinFirstHeaderSize {
		return nil, ErrFormat
	}
	firstSize := int(basic[0])
	if firstSize < arjMinFirstHeaderSize || firstSize > len(basic) {
		return nil, ErrFormat
	}

	method := uint16(basic[5])
	fileType := basic[6]
	modifiedDOS := binary.LittleEndian.Uint32(basic[8:12])
	compressedSize := binary.LittleEndian.Uint32(basic[12:16])
	uncompressedSize := binary.LittleEndian.Uint32(basic[16:20])
	crc := binary.LittleEndian.Uint32(basic[20:24])
	filespecPos := binary.LittleEndian.Uint16(basic[24:26])
	mode := binary.LittleEndian.Uint16(basic[26:28])
	hostData := binary.LittleEndian.Uint16(basic[28:30])
	extFlags, chapterNumber := unpackExtMetadata(hostData)
	hostOS := basic[3]
	flags := basic[4]
	firstHeaderExtra := append([]byte(nil), basic[arjMinFirstHeaderSize:firstSize]...)

	rest := basic[firstSize:]
	name, rest, ok := cutCString(rest)
	if !ok {
		return nil, ErrFormat
	}
	comment, _, ok := cutCString(rest)
	if !ok {
		return nil, ErrFormat
	}

	localExtendedHeaders := extHeaders
	if cloneExt {
		localExtendedHeaders = cloneLocalExtendedHeaders(extHeaders)
	}

	fh := FileHeader{
		Name:                 name,
		Comment:              comment,
		Method:               method,
		Modified:             dosDateTimeToTime(modifiedDOS),
		CRC32:                crc,
		CompressedSize64:     uint64(compressedSize),
		UncompressedSize64:   uint64(uncompressedSize),
		HostOS:               hostOS,
		Flags:                flags,
		PasswordModifier:     basic[7],
		FirstHeaderSize:      uint8(firstSize),
		ArchiverVersion:      basic[1],
		MinVersion:           basic[2],
		FilespecPos:          filespecPos,
		HostData:             hostData,
		ExtFlags:             extFlags,
		ChapterNumber:        chapterNumber,
		LocalExtendedHeaders: localExtendedHeaders,
		fileMode:             mode,
		fileType:             fileType,
		modifiedDOS:          modifiedDOS,
		firstHeaderExtra:     firstHeaderExtra,
	}
	return &File{
		FileHeader: fh,
		arj:        r,
	}, nil
}

func cloneLocalExtendedHeaders(in [][]byte) [][]byte {
	return cloneExtendedHeaders(in)
}

func cloneMainExtendedHeaders(in [][]byte) [][]byte {
	return cloneExtendedHeaders(in)
}

func cloneExtendedHeaders(in [][]byte) [][]byte {
	if len(in) == 0 {
		return nil
	}
	out := make([][]byte, len(in))
	for i := range in {
		if len(in[i]) == 0 {
			out[i] = []byte{}
			continue
		}
		out[i] = append([]byte(nil), in[i]...)
	}
	return out
}

func cutCString(b []byte) (s string, tail []byte, ok bool) {
	i := bytes.IndexByte(b, 0)
	if i < 0 {
		return "", nil, false
	}
	return string(b[:i]), b[i+1:], true
}

func readAtFull(r io.ReaderAt, p []byte, off int64) error {
	n, err := r.ReadAt(p, off)
	if n == len(p) {
		return nil
	}
	if err == nil {
		return io.ErrUnexpectedEOF
	}
	return err
}

func normalizeHeaderReadError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return ErrFormat
	}
	return err
}

func clearBytes(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
