package arj

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"sync"
)

const streamHeaderScanChunkSize = 32 << 10

// StreamReaderOptions configures NewStreamReaderWithOptions.
type StreamReaderOptions struct {
	ParserLimits ParserLimits
	// MaxHeaderScanBytes limits how many leading bytes may be scanned while
	// looking for the main ARJ header. Zero means unlimited.
	MaxHeaderScanBytes int64
}

var (
	// ErrInvalidStreamHeaderScanLimit indicates an invalid stream header scan limit.
	ErrInvalidStreamHeaderScanLimit = errors.New("arj: stream header scan limit must be >= 0")
	// ErrStreamHeaderScanLimitExceeded indicates a stream header was not found
	// within StreamReaderOptions.MaxHeaderScanBytes.
	ErrStreamHeaderScanLimitExceeded = errors.New("arj: stream header scan limit exceeded")
)

// A StreamReader serves ARJ content sequentially from an io.Reader.
type StreamReader struct {
	br            *bufio.Reader
	ArchiveHeader ArchiveHeader
	ArchiveName   string
	Comment       string

	stateMu sync.RWMutex

	decompressors map[uint16]Decompressor
	password      []byte
	method14Limit Method14DecodeLimits

	parserLimits ParserLimits
	entryCount   int
	baseOffset   int64
	done         bool
	current      *streamEntryReadCloser
}

// NewStreamReader returns a sequential ARJ stream reader.
func NewStreamReader(r io.Reader) (*StreamReader, error) {
	return NewStreamReaderWithOptions(r, StreamReaderOptions{})
}

// NewStreamReaderWithOptions returns a sequential ARJ stream reader with
// configurable parser limits.
func NewStreamReaderWithOptions(r io.Reader, opts StreamReaderOptions) (*StreamReader, error) {
	if r == nil {
		return nil, ErrFormat
	}
	if opts.MaxHeaderScanBytes < 0 {
		return nil, ErrInvalidStreamHeaderScanLimit
	}
	if err := validateParserLimits(opts.ParserLimits); err != nil {
		return nil, err
	}

	limits := normalizeParserLimits(opts.ParserLimits)
	br := bufio.NewReaderSize(r, streamHeaderScanChunkSize+arjMaxBasicHeaderSize+6)

	baseOffset, err := scanToHeaderSignature(br, opts.MaxHeaderScanBytes)
	if err != nil {
		return nil, err
	}
	mainBasic, mainExt, err := readHeaderBlockFromStreamAfterSignature(br, limits)
	if err != nil {
		return nil, err
	}
	if len(mainBasic) == 0 {
		return nil, ErrFormat
	}

	mainHeader, err := parseMainHeaderOwned(mainBasic, mainExt)
	if err != nil {
		return nil, err
	}

	sr := &StreamReader{
		br:            br,
		ArchiveHeader: mainHeader,
		ArchiveName:   mainHeader.Name,
		Comment:       mainHeader.Comment,
		method14Limit: normalizeMethod14DecodeLimits(Method14DecodeLimits{}),
		parserLimits:  limits,
		baseOffset:    baseOffset,
	}
	return sr, nil
}

// BaseOffset returns the offset where the main header signature was found.
func (r *StreamReader) BaseOffset() int64 {
	if r == nil {
		return 0
	}
	return r.baseOffset
}

// SetPassword configures the default password for opening encrypted entries.
func (r *StreamReader) SetPassword(password string) {
	if r == nil {
		return
	}
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	clearBytes(r.password)
	r.password = append(r.password[:0], password...)
}

// SetMethod14DecodeLimits overrides native method 1-4 decode limits.
// Any zero field keeps the corresponding package default.
func (r *StreamReader) SetMethod14DecodeLimits(limits Method14DecodeLimits) {
	if r == nil {
		return
	}
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	r.method14Limit = normalizeMethod14DecodeLimits(limits)
}

// RegisterDecompressor registers or overrides a custom decompressor for a
// specific method ID. If a decompressor for a given method is not found,
// StreamReader defaults to the package-level registration.
// Passing nil explicitly disables the method on this StreamReader instance.
func (r *StreamReader) RegisterDecompressor(method uint16, dcomp Decompressor) {
	if r == nil {
		return
	}
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	if r.decompressors == nil {
		r.decompressors = make(map[uint16]Decompressor)
	}
	r.decompressors[method] = dcomp
}

func (r *StreamReader) passwordBytes() []byte {
	r.stateMu.RLock()
	defer r.stateMu.RUnlock()
	if len(r.password) == 0 {
		return nil
	}
	return append([]byte(nil), r.password...)
}

func (r *StreamReader) method14DecodeLimits() Method14DecodeLimits {
	r.stateMu.RLock()
	defer r.stateMu.RUnlock()
	return normalizeMethod14DecodeLimits(r.method14Limit)
}

func (r *StreamReader) decompressor(method uint16) Decompressor {
	r.stateMu.RLock()
	defer r.stateMu.RUnlock()
	if r.decompressors != nil {
		if dcomp, ok := r.decompressors[method]; ok {
			return dcomp
		}
	}
	return decompressor(method)
}

// Next advances to the next file in the stream and returns its header and
// data reader.
//
// When there are no more files, it returns io.EOF.
func (r *StreamReader) Next() (*FileHeader, io.ReadCloser, error) {
	if r == nil || r.br == nil {
		return nil, nil, ErrFormat
	}
	if r.done {
		return nil, nil, io.EOF
	}
	if r.current != nil {
		if err := r.current.Close(); err != nil {
			return nil, nil, err
		}
	}

	basic, extHeaders, err := readHeaderBlockFromStream(r.br, r.parserLimits)
	if err != nil {
		return nil, nil, err
	}
	if len(basic) == 0 {
		r.done = true
		return nil, nil, io.EOF
	}
	if r.entryCount >= r.parserLimits.MaxEntries {
		return nil, nil, parserEntryLimitError(r.parserLimits.MaxEntries)
	}

	f, err := parseLocalFileHeaderOwned(basic, extHeaders, nil)
	if err != nil {
		return nil, nil, err
	}
	r.entryCount++

	if f.CompressedSize64 > uint64(math.MaxInt64) {
		return nil, nil, ErrFormat
	}
	entry := &streamEntryReadCloser{
		owner:  r,
		header: f.FileHeader,
		raw: &io.LimitedReader{
			R: r.br,
			N: int64(f.CompressedSize64),
		},
	}
	r.current = entry
	return &entry.header, entry, nil
}

func scanToHeaderSignature(r *bufio.Reader, maxScanBytes int64) (int64, error) {
	signature := []byte{arjHeaderID1, arjHeaderID2}
	var off int64
	for {
		if maxScanBytes > 0 && off > maxScanBytes {
			return 0, ErrStreamHeaderScanLimitExceeded
		}
		if _, err := r.Peek(len(signature)); err != nil {
			return 0, normalizeHeaderReadError(err)
		}

		n := r.Buffered()
		if n > streamHeaderScanChunkSize {
			n = streamHeaderScanChunkSize
		}
		chunk, err := r.Peek(n)
		if err != nil {
			return 0, normalizeHeaderReadError(err)
		}

		for i := 0; i < len(chunk)-1; {
			next := bytes.Index(chunk[i:], signature)
			if next < 0 {
				break
			}
			pos := i + next
			candidateOff := off + int64(pos)
			if maxScanBytes > 0 && candidateOff > maxScanBytes {
				return 0, ErrStreamHeaderScanLimitExceeded
			}
			if pos+3 < len(chunk) {
				basicSize := int(binary.LittleEndian.Uint16(chunk[pos+2 : pos+4]))
				if basicSize < arjMinFirstHeaderSize || basicSize > arjMaxBasicHeaderSize {
					i = pos + 1
					continue
				}
			}
			headerPrefix, err := r.Peek(pos + len(signature) + 2)
			if err != nil {
				return 0, normalizeHeaderReadError(err)
			}
			sizeBytes := headerPrefix[pos+len(signature) : pos+len(signature)+2]
			basicSize := int(binary.LittleEndian.Uint16(sizeBytes))
			if basicSize >= arjMinFirstHeaderSize && basicSize <= arjMaxBasicHeaderSize {
				const crcSize = 4
				candidateBytes, err := r.Peek(pos + len(signature) + 2 + basicSize + crcSize)
				if err != nil {
					return 0, normalizeHeaderReadError(err)
				}
				basicStart := pos + len(signature) + 2
				basic := candidateBytes[basicStart : basicStart+basicSize]
				wantCRC := binary.LittleEndian.Uint32(candidateBytes[basicStart+basicSize : basicStart+basicSize+crcSize])
				if crc32.ChecksumIEEE(basic) == wantCRC && probeMainHeaderBasicValid(basic) {
					if _, err := r.Discard(pos + len(signature)); err != nil {
						return 0, normalizeHeaderReadError(err)
					}
					return candidateOff, nil
				}
			}
			i = pos + 1
		}

		discard := len(chunk) - 1
		if maxScanBytes > 0 && off+int64(discard) > maxScanBytes {
			return 0, ErrStreamHeaderScanLimitExceeded
		}
		if _, err := r.Discard(discard); err != nil {
			return 0, normalizeHeaderReadError(err)
		}
		off += int64(discard)
	}
}

func readHeaderBlockFromStream(r io.Reader, limits ParserLimits) (basic []byte, extHeaders [][]byte, err error) {
	var pre [4]byte
	if _, err := io.ReadFull(r, pre[:]); err != nil {
		return nil, nil, normalizeHeaderReadError(err)
	}
	if pre[0] != arjHeaderID1 || pre[1] != arjHeaderID2 {
		return nil, nil, ErrFormat
	}
	return readHeaderBlockFromStreamAfterBasicSize(r, int(binary.LittleEndian.Uint16(pre[2:4])), limits)
}

func readHeaderBlockFromStreamAfterSignature(r io.Reader, limits ParserLimits) (basic []byte, extHeaders [][]byte, err error) {
	var sz [2]byte
	if _, err := io.ReadFull(r, sz[:]); err != nil {
		return nil, nil, normalizeHeaderReadError(err)
	}
	return readHeaderBlockFromStreamAfterBasicSize(r, int(binary.LittleEndian.Uint16(sz[:])), limits)
}

func readHeaderBlockFromStreamAfterBasicSize(r io.Reader, basicSize int, limits ParserLimits) (basic []byte, extHeaders [][]byte, err error) {
	limits = normalizeParserLimits(limits)
	if basicSize == 0 {
		return nil, nil, nil
	}
	if basicSize < arjMinFirstHeaderSize || basicSize > arjMaxBasicHeaderSize {
		return nil, nil, ErrFormat
	}

	basic = make([]byte, basicSize)
	if _, err := io.ReadFull(r, basic); err != nil {
		return nil, nil, normalizeHeaderReadError(err)
	}
	var crcBuf [4]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		return nil, nil, normalizeHeaderReadError(err)
	}
	if crc32.ChecksumIEEE(basic) != binary.LittleEndian.Uint32(crcBuf[:]) {
		return nil, nil, ErrFormat
	}

	extCount := 0
	var extBytes int64
	for {
		var sz [2]byte
		if _, err := io.ReadFull(r, sz[:]); err != nil {
			return nil, nil, normalizeHeaderReadError(err)
		}
		extSize := int(binary.LittleEndian.Uint16(sz[:]))
		if extSize == 0 {
			break
		}
		extCount++
		if extCount > limits.MaxExtendedHeaders {
			return nil, nil, parserExtendedHeaderCountLimitError(limits.MaxExtendedHeaders)
		}
		extBytes += int64(extSize)
		if extBytes < 0 || extBytes > limits.MaxExtendedHeaderBytes {
			return nil, nil, parserExtendedHeaderBytesLimitError(limits.MaxExtendedHeaderBytes)
		}

		ext := make([]byte, extSize)
		if _, err := io.ReadFull(r, ext); err != nil {
			return nil, nil, normalizeHeaderReadError(err)
		}
		if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
			return nil, nil, normalizeHeaderReadError(err)
		}
		if crc32.ChecksumIEEE(ext) != binary.LittleEndian.Uint32(crcBuf[:]) {
			return nil, nil, ErrFormat
		}
		extHeaders = append(extHeaders, ext)
	}

	return basic, extHeaders, nil
}

type streamEntryReadCloser struct {
	owner  *StreamReader
	header FileHeader
	raw    *io.LimitedReader
	limits Method14DecodeLimits

	openErr error
	rc      io.ReadCloser
	opened  bool
	closed  bool
}

func (r *streamEntryReadCloser) Read(p []byte) (int, error) {
	if r.closed {
		return 0, io.EOF
	}
	if r.openErr != nil {
		return 0, r.openErr
	}
	if !r.opened {
		if err := r.open(); err != nil {
			return 0, err
		}
	}
	if r.rc == nil {
		return 0, io.EOF
	}
	return r.rc.Read(p)
}

func (r *streamEntryReadCloser) open() error {
	if r.opened {
		return r.openErr
	}
	r.opened = true
	fail := func(err error) error {
		r.openErr = err
		return err
	}

	password := r.owner.passwordBytes()
	defer clearBytes(password)

	if err := unsupportedStreamOpenModeError(r.owner, r.header, password); err != nil {
		return fail(err)
	}
	limits := normalizeMethod14DecodeLimits(Method14DecodeLimits{})
	if r.owner != nil {
		limits = r.owner.method14DecodeLimits()
	}
	r.limits = limits
	if isNativeMethod14(r.header.Method) {
		if err := validateMethod14DecodeSizes(limits, r.header.CompressedSize64, r.header.UncompressedSize64); err != nil {
			return fail(err)
		}
	}

	if r.header.isDir() {
		r.rc = &dirReader{err: io.EOF}
		return nil
	}

	dcomp := r.owner.decompressor(r.header.Method)
	if dcomp == nil {
		return fail(ErrAlgorithm)
	}

	in := io.Reader(r.raw)
	var garbled *garbledReader
	if r.header.Flags&FlagGarbled != 0 {
		garbled = newGarbledReader(in, password, r.header.PasswordModifier)
		in = garbled
	}
	if isNativeMethod14(r.header.Method) {
		in = wrapMethod14DecompressorInput(in, int64(r.header.CompressedSize64), r.header.UncompressedSize64, limits)
	}
	rc := dcomp(in)
	if rc == nil {
		if garbled != nil {
			garbled.clearSensitiveData()
		}
		return fail(ErrAlgorithm)
	}
	if garbled != nil {
		rc = &garbledReaderCloser{
			ReadCloser: rc,
			garbled:    garbled,
		}
	}
	r.rc = &checksumReader{
		rc:      rc,
		wantCRC: r.header.CRC32,
		wantN:   r.header.UncompressedSize64,
	}
	r.openErr = nil
	return nil
}

func unsupportedStreamOpenModeError(owner *StreamReader, header FileHeader, password []byte) error {
	if err := unsupportedEncryptionError(header.Flags, header.EncryptionVersion(), password); err != nil {
		return err
	}
	if err := unsupportedSecuredFlagsError(header.Flags); err != nil {
		return err
	}
	if owner != nil {
		if err := unsupportedMainSecurityFlagsError(owner.ArchiveHeader.Flags, owner.ArchiveHeader.EncryptionVersion()); err != nil {
			return err
		}
	}
	return nil
}

func abortStreamReadCloser(rc io.ReadCloser) {
	if rc == nil {
		return
	}
	if aborter, ok := rc.(interface{ abort() error }); ok {
		_ = aborter.abort()
		return
	}
	_ = rc.Close()
}

func (r *streamEntryReadCloser) Close() error {
	if r.closed {
		return nil
	}

	var closeErr error
	if r.rc != nil {
		closeErr = r.rc.Close()
		r.rc = nil
	}
	var discardErr error
	if r.raw != nil && r.raw.N > 0 {
		_, discardErr = io.Copy(io.Discard, r.raw)
		r.raw.N = 0
	}
	r.closed = true
	if r.owner != nil && r.owner.current == r {
		r.owner.current = nil
	}
	return errors.Join(closeErr, discardErr)
}

func (r *streamEntryReadCloser) abort() error {
	if r.closed {
		return nil
	}

	var closeErr error
	if r.rc != nil {
		closeErr = r.rc.Close()
		r.rc = nil
	}
	if r.raw != nil {
		r.raw.N = 0
	}
	r.closed = true
	if r.owner != nil && r.owner.current == r {
		r.owner.current = nil
	}
	return closeErr
}
