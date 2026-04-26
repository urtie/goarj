package arj

import (
	"bufio"
	"fmt"
	"io"
	"sync"
)

const (
	methodCodeBit  = 16
	methodThresh   = 3
	methodDICSize  = 26624
	methodFDIC     = 32768
	methodFDICMask = methodFDIC - 1
	methodMaxM     = 256
	methodNC       = 255 + methodMaxM + 2 - methodThresh
	methodNP       = 16 + 1
	methodCBIT     = 9
	methodNT       = methodCodeBit + 3
	methodPBIT     = 5
	methodTBIT     = 5
	methodNPT      = methodNT
	methodCTable   = 4096
	methodPTable   = 256

	method14CompressorChunkSize       = 64 << 10
	method14DecompressorMaxBufferSize = 64 << 10
)

var (
	method123FastStreamDecoderPool = sync.Pool{
		New: func() any {
			return new(method123FastStreamDecoder)
		},
	}
	method4FastStreamDecoderPool = sync.Pool{
		New: func() any {
			return new(method4FastStreamDecoder)
		},
	}
)

var method14LowMask16 = [17]uint64{
	0x0, 0x1, 0x3, 0x7, 0xf, 0x1f, 0x3f, 0x7f,
	0xff, 0x1ff, 0x3ff, 0x7ff, 0xfff, 0x1fff, 0x3fff, 0x7fff,
	0xffff,
}

type method14Input struct {
	io.Reader
	compressedSize   int64
	uncompressedSize uint64
	limits           Method14DecodeLimits
}

type method14Compressor struct {
	method       uint16
	dst          io.Writer
	limit        uint64
	written      uint64
	fatalErr     error
	pending      []byte
	pendingOff   int
	bw           *arjBitWriter
	method123Enc method123BitEncoder
	closed       bool
}

type method14ErrorReadCloser struct {
	err error
}

type method14StreamingReadCloser struct {
	method uint16
	r      io.Reader
	err    error
	closed bool
}

type method14BitReader interface {
	fillBuf(n int) error
	getBits(n int) (uint16, error)
	peekBits(n int) (uint16, error)
}

type arjBitWriter struct {
	out      []byte
	dst      io.Writer
	bitBuf   uint32
	bitCount uint8
	err      error
	writeBuf [1024]byte
	writeN   int
}

type arjBitReader struct {
	data      []byte
	bitPos    uint64
	totalBits uint64
	bitBuf    uint16
	err       error
}

type arjBitStreamReader struct {
	r         io.Reader
	remaining int64
	bitPos    uint64
	totalBits uint64
	bitBuf    uint64
	bitCount  uint8
	err       error
	scratch   [8]byte
}

type method123Decoder struct {
	br        method14BitReader
	blockSize int

	ptLen   [methodNPT]uint8
	cLen    [methodNC]uint8
	left    [2*methodNC - 1]uint16
	right   [2*methodNC - 1]uint16
	cTable  [methodCTable]uint16
	ptTable [methodPTable]uint16
}

type method123BitStreamDecoder struct {
	br        *arjBitStreamReader
	blockSize int

	ptLen   [methodNPT]uint8
	cLen    [methodNC]uint8
	left    [2*methodNC - 1]uint16
	right   [2*methodNC - 1]uint16
	cTable  [methodCTable]uint16
	ptTable [methodPTable]uint16
}

type method123StreamDecoder struct {
	dec       method123Decoder
	dict      [methodDICSize]byte
	dictPos   int
	remaining uint64
	matchSrc  int
	matchLen  int
	err       error
}

type method123FastStreamDecoder struct {
	dec       method123BitStreamDecoder
	dict      [methodDICSize]byte
	dictPos   int
	remaining uint64
	matchSrc  int
	matchLen  int
	err       error
}

type method4StreamDecoder struct {
	br        method14BitReader
	dict      [methodFDIC]byte
	dictPos   int
	remaining uint64
	matchSrc  int
	matchLen  int
	err       error
}

type method4FastStreamDecoder struct {
	br        *arjBitStreamReader
	dict      [methodFDIC]byte
	dictPos   int
	remaining uint64
	matchSrc  int
	matchLen  int
	err       error
}

func isNativeMethod14(method uint16) bool {
	switch method {
	case Method1, Method2, Method3, Method4:
		return true
	default:
		return false
	}
}

func wrapMethod14DecompressorInput(in io.Reader, compressedSize int64, uncompressedSize uint64, limits Method14DecodeLimits) io.Reader {
	return &method14Input{
		Reader:           in,
		compressedSize:   compressedSize,
		uncompressedSize: uncompressedSize,
		limits:           normalizeMethod14DecodeLimits(limits),
	}
}

func compressorMethod14(method uint16) Compressor {
	return func(w io.Writer) (io.WriteCloser, error) {
		limit := DefaultMaxMethod14InputBufferSize
		bw := newARJBitWriter(w)
		return &method14Compressor{
			method:       method,
			dst:          w,
			limit:        limit,
			bw:           bw,
			method123Enc: method123BitEncoder{bw: bw},
		}, nil
	}
}

func decompressorMethod14(method uint16) Decompressor {
	return func(in io.Reader) io.ReadCloser {
		ctx, ok := in.(*method14Input)
		if !ok {
			return &method14ErrorReadCloser{
				err: fmt.Errorf("arj: method %d decompressor missing file context", method),
			}
		}

		limits := normalizeMethod14DecodeLimits(ctx.limits)
		if ctx.compressedSize < 0 {
			return &method14ErrorReadCloser{err: ErrFormat}
		}
		if err := validateMethod14DecodeBufferSizes(limits, uint64(ctx.compressedSize), ctx.uncompressedSize); err != nil {
			return &method14ErrorReadCloser{err: err}
		}
		if err := validateMethod14DecodeWorkingSet(uint64(ctx.compressedSize), ctx.uncompressedSize); err != nil {
			return &method14ErrorReadCloser{err: err}
		}

		// Decode uses bit-level reads, so keep compressed input buffered to avoid
		// tiny syscall-heavy reads from the underlying archive file.
		br := newARJBitStreamReader(bufio.NewReaderSize(ctx.Reader, method14DecompressorBufferSize(ctx.compressedSize)), ctx.compressedSize)
		switch method {
		case Method1, Method2, Method3:
			return &method14StreamingReadCloser{
				method: method,
				r:      newMethod123FastStreamDecoder(br, ctx.uncompressedSize),
			}
		case Method4:
			return &method14StreamingReadCloser{
				method: method,
				r:      newMethod4FastStreamDecoder(br, ctx.uncompressedSize),
			}
		default:
			return &method14ErrorReadCloser{err: ErrAlgorithm}
		}
	}
}

func method14DecompressorBufferSize(compressedSize int64) int {
	if compressedSize <= 0 {
		return 1
	}
	if compressedSize > method14DecompressorMaxBufferSize {
		return method14DecompressorMaxBufferSize
	}
	return int(compressedSize)
}

func (w *method14Compressor) Write(p []byte) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("arj: write to closed compressor")
	}
	if len(p) == 0 {
		return 0, nil
	}
	if w.fatalErr != nil {
		return 0, w.fatalErr
	}
	if w.bw == nil {
		w.bw = newARJBitWriter(w.dst)
		w.method123Enc = method123BitEncoder{bw: w.bw}
	}
	if w.written >= w.limit {
		return 0, w.limitErr(len(p))
	}

	remaining := w.limit - w.written
	chunk := p
	limited := false
	if uint64(len(chunk)) > remaining {
		chunk = chunk[:int(remaining)]
		limited = true
	}
	w.pending = append(w.pending, chunk...)
	w.written += uint64(len(chunk))
	if err := w.flushPendingFullChunks(); err != nil {
		w.fatalErr = err
		return len(chunk), err
	}
	if limited {
		return len(chunk), w.limitErr(len(p))
	}
	return len(chunk), nil
}

func (w *method14Compressor) setMethod14InputBufferLimit(limit uint64) {
	w.limit = normalizeWriteBufferLimits(WriteBufferLimits{
		MaxMethod14InputBufferSize: limit,
	}).MaxMethod14InputBufferSize
}

func (w *method14Compressor) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if w.fatalErr != nil {
		return w.fatalErr
	}
	if w.bw == nil {
		w.bw = newARJBitWriter(w.dst)
		w.method123Enc = method123BitEncoder{bw: w.bw}
	}
	if err := w.flushAllPending(); err != nil {
		w.fatalErr = err
		return err
	}
	_ = w.bw.finishWithShutdownPadding()
	if w.bw.err != nil {
		w.fatalErr = w.bw.err
		return w.fatalErr
	}
	return nil
}

func (w *method14Compressor) limitErr(attempted int) *BufferLimitError {
	return &BufferLimitError{
		Scope:     bufferScopeMethod14Input,
		Limit:     w.limit,
		Buffered:  w.written,
		Attempted: uint64(attempted),
	}
}

func (w *method14Compressor) flushPendingFullChunks() error {
	for len(w.pending)-w.pendingOff >= method14CompressorChunkSize {
		chunk := w.pending[w.pendingOff : w.pendingOff+method14CompressorChunkSize]
		if err := w.encodeChunk(chunk); err != nil {
			return err
		}
		w.pendingOff += method14CompressorChunkSize
	}
	w.compactPending()
	return nil
}

func (w *method14Compressor) flushAllPending() error {
	for w.pendingOff < len(w.pending) {
		chunk := w.pending[w.pendingOff:]
		if err := w.encodeChunk(chunk); err != nil {
			return err
		}
		w.pendingOff = len(w.pending)
	}
	w.compactPending()
	return nil
}

func (w *method14Compressor) compactPending() {
	if w.pendingOff == 0 {
		return
	}
	if w.pendingOff >= len(w.pending) {
		w.pending = w.pending[:0]
		w.pendingOff = 0
		return
	}
	copy(w.pending, w.pending[w.pendingOff:])
	w.pending = w.pending[:len(w.pending)-w.pendingOff]
	w.pendingOff = 0
}

func (w *method14Compressor) encodeChunk(chunk []byte) error {
	if len(chunk) == 0 {
		return nil
	}
	switch w.method {
	case Method1, Method2, Method3:
		method123TokenizeGreedyBlocks(chunk, w.method123Enc.writeBlock)
	case Method4:
		encodeMethod4NativeToBitWriter(w.bw, chunk)
	default:
		return ErrAlgorithm
	}
	return w.bw.err
}

func (r *method14ErrorReadCloser) Read([]byte) (int, error) {
	return 0, r.err
}

func (r *method14ErrorReadCloser) Close() error {
	return nil
}

func (r *method14StreamingReadCloser) Read(p []byte) (int, error) {
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	if r.err != nil {
		return 0, r.err
	}
	n, err := r.r.Read(p)
	if err != nil && err != io.EOF {
		err = fmt.Errorf("arj: method %d decompression failed: %w", r.method, err)
		r.err = err
	}
	return n, err
}

func (r *method14StreamingReadCloser) Close() error {
	if r == nil || r.closed {
		return nil
	}
	r.closed = true
	defer func() {
		r.r = nil
	}()
	if closer, ok := r.r.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func compressMethod14Payload(method uint16, plain []byte) ([]byte, error) {
	switch method {
	case Method1, Method2, Method3:
		return encodeMethod123Native(plain), nil
	case Method4:
		return encodeMethod4Native(plain), nil
	default:
		return nil, ErrAlgorithm
	}
}

func decompressMethod14Payload(method uint16, compressed []byte, origSize uint64) ([]byte, error) {
	return decompressMethod14PayloadWithLimits(method, compressed, origSize, Method14DecodeLimits{})
}

func decompressMethod14PayloadWithLimits(method uint16, compressed []byte, origSize uint64, limits Method14DecodeLimits) ([]byte, error) {
	limits = normalizeMethod14DecodeLimits(limits)
	if err := validateMethod14DecodeBufferSizes(limits, uint64(len(compressed)), origSize); err != nil {
		return nil, err
	}
	if err := validateMethod14DecodeWorkingSet(uint64(len(compressed)), origSize); err != nil {
		return nil, err
	}

	switch method {
	case Method1, Method2, Method3:
		plain, err := decodeMethod123(compressed, origSize, limits)
		if err != nil {
			return nil, fmt.Errorf("arj: method %d decompression failed: %w", method, err)
		}
		return plain, nil
	case Method4:
		plain, err := decodeMethod4(compressed, origSize, limits)
		if err != nil {
			return nil, fmt.Errorf("arj: method %d decompression failed: %w", method, err)
		}
		return plain, nil
	default:
		return nil, ErrAlgorithm
	}
}

func normalizeMethod14DecodeLimits(limits Method14DecodeLimits) Method14DecodeLimits {
	if limits.MaxCompressedSize == 0 {
		limits.MaxCompressedSize = DefaultMethod14MaxCompressedSize
	}
	if limits.MaxUncompressedSize == 0 {
		limits.MaxUncompressedSize = DefaultMethod14MaxUncompressedSize
	}
	return limits
}

func validateMethod14DecodeSizes(limits Method14DecodeLimits, compressedSize, uncompressedSize uint64) error {
	if compressedSize > limits.MaxCompressedSize {
		return ErrFormat
	}
	if uncompressedSize > limits.MaxUncompressedSize {
		return ErrFormat
	}
	return nil
}

func validateMethod14DecodeBufferSizes(limits Method14DecodeLimits, compressedSize, uncompressedSize uint64) error {
	if err := validateMethod14DecodeSizes(limits, compressedSize, uncompressedSize); err != nil {
		return err
	}
	return validateMethod14DecodeBufferSizesForArch(limits, compressedSize, uncompressedSize, uint64(int(^uint(0)>>1)))
}

func validateMethod14DecodeBufferSizesForArch(limits Method14DecodeLimits, compressedSize, uncompressedSize, archMaxInt uint64) error {
	if archMaxInt == 0 {
		return ErrFormat
	}
	if compressedSize > archMaxInt || uncompressedSize > archMaxInt {
		return ErrFormat
	}
	if compressedSize > archMaxInt-uncompressedSize {
		return ErrFormat
	}
	return nil
}

// method14MaxDecodeWorkingSetBytes is a non-configurable cap for method 1-4
// decode buffer usage (compressed + uncompressed). User-configured decode
// limits cannot exceed this hard ceiling.
const method14MaxDecodeWorkingSetBytes = DefaultMethod14MaxCompressedSize + DefaultMethod14MaxUncompressedSize

func validateMethod14DecodeWorkingSet(compressedSize, uncompressedSize uint64) error {
	if compressedSize > method14MaxDecodeWorkingSetBytes || uncompressedSize > method14MaxDecodeWorkingSetBytes {
		return ErrFormat
	}
	if compressedSize > method14MaxDecodeWorkingSetBytes-uncompressedSize {
		return ErrFormat
	}
	return nil
}

func encodeMethod4LiteralOnly(plain []byte) []byte {
	var bw arjBitWriter
	for _, b := range plain {
		bw.putBits(1, 0)
		bw.putBits(8, uint16(b))
	}
	return bw.finishWithShutdownPadding()
}

func decodeMethod123(compressed []byte, origSize uint64, limits Method14DecodeLimits) ([]byte, error) {
	if err := validateMethod14DecodeBufferSizes(limits, uint64(len(compressed)), origSize); err != nil {
		return nil, err
	}
	if origSize == 0 {
		return nil, nil
	}
	return io.ReadAll(newMethod123StreamDecoder(newARJBitReader(compressed), origSize))
}

func decodeMethod4(compressed []byte, origSize uint64, limits Method14DecodeLimits) ([]byte, error) {
	if err := validateMethod14DecodeBufferSizes(limits, uint64(len(compressed)), origSize); err != nil {
		return nil, err
	}
	if origSize == 0 {
		return nil, nil
	}
	return io.ReadAll(newMethod4StreamDecoder(newARJBitReader(compressed), origSize))
}

func decodeMethod4Ptr(br method14BitReader) (int, error) {
	plus := 0
	pwr := 1 << 9
	c := 0
	width := 0

	for width = 9; width < 13; width++ {
		v, err := br.getBits(1)
		if err != nil {
			return 0, err
		}
		c = int(v)
		if c == 0 {
			break
		}
		plus += pwr
		pwr <<= 1
	}
	if width != 0 {
		v, err := br.getBits(width)
		if err != nil {
			return 0, err
		}
		c = int(v)
	}
	c += plus
	return c, nil
}

func decodeMethod4Len(br method14BitReader) (int, error) {
	plus := 0
	pwr := 1
	c := 0
	width := 0

	for width = 0; width < 7; width++ {
		v, err := br.getBits(1)
		if err != nil {
			return 0, err
		}
		c = int(v)
		if c == 0 {
			break
		}
		plus += pwr
		pwr <<= 1
	}
	if width != 0 {
		v, err := br.getBits(width)
		if err != nil {
			return 0, err
		}
		c = int(v)
	}
	c += plus
	return c, nil
}

func decodeMethod4PtrStream(br *arjBitStreamReader) (int, error) {
	if br.err != nil {
		return 0, br.err
	}
	if int(br.bitCount) < 17 {
		if err := br.fillLookahead(17); err != nil {
			return 0, err
		}
	}
	if int(br.bitCount) >= 17 {
		bc := int(br.bitCount)
		prefix := uint8((br.bitBuf >> (bc - 4)) & 0xF)

		k := 0
		for k < 4 && ((prefix>>(3-k))&1) != 0 {
			k++
		}
		width := 9 + k
		consumedPrefix := k + 1
		if k == 4 {
			consumedPrefix = 4
		}
		totalConsumed := consumedPrefix + width
		extraShift := bc - totalConsumed
		extra := int((br.bitBuf >> extraShift) & method14LowMask16[width])
		plus := 0
		if k > 0 {
			plus = 512 * ((1 << k) - 1)
		}

		br.bitCount -= uint8(totalConsumed)
		br.bitPos += uint64(totalConsumed)
		if br.bitCount == 0 {
			br.bitBuf = 0
		}
		return plus + extra, nil
	}

	plus := 0
	pwr := 1 << 9
	c := 0
	width := 0

	for width = 9; width < 13; width++ {
		v, err := br.getBitFast()
		if err != nil {
			return 0, err
		}
		c = int(v)
		if c == 0 {
			break
		}
		plus += pwr
		pwr <<= 1
	}
	if width != 0 {
		v, err := br.getBitsFast(width)
		if err != nil {
			return 0, err
		}
		c = int(v)
	}
	c += plus
	return c, nil
}

func decodeMethod4LenStream(br *arjBitStreamReader) (int, error) {
	if br.err != nil {
		return 0, br.err
	}
	if int(br.bitCount) < 14 {
		if err := br.fillLookahead(14); err != nil {
			return 0, err
		}
	}
	if int(br.bitCount) >= 14 {
		bc := int(br.bitCount)
		prefix := uint8((br.bitBuf >> (bc - 7)) & 0x7F)

		width := 0
		for width < 7 && ((prefix>>(6-width))&1) != 0 {
			width++
		}
		consumedPrefix := width + 1
		if width == 7 {
			consumedPrefix = 7
		}
		if width == 0 {
			br.bitCount--
			br.bitPos++
			if br.bitCount == 0 {
				br.bitBuf = 0
			}
			return 0, nil
		}

		totalConsumed := consumedPrefix + width
		extraShift := bc - totalConsumed
		extra := int((br.bitBuf >> extraShift) & method14LowMask16[width])
		plus := (1 << width) - 1

		br.bitCount -= uint8(totalConsumed)
		br.bitPos += uint64(totalConsumed)
		if br.bitCount == 0 {
			br.bitBuf = 0
		}
		return plus + extra, nil
	}

	plus := 0
	pwr := 1
	c := 0
	width := 0

	for width = 0; width < 7; width++ {
		v, err := br.getBitFast()
		if err != nil {
			return 0, err
		}
		c = int(v)
		if c == 0 {
			break
		}
		plus += pwr
		pwr <<= 1
	}
	if width != 0 {
		v, err := br.getBitsFast(width)
		if err != nil {
			return 0, err
		}
		c = int(v)
	}
	c += plus
	return c, nil
}

func method14CanFastMatchCopy(src, dst, n, dictSize int) bool {
	if n <= 0 || src < 0 || dst < 0 {
		return false
	}
	if src+n > dictSize || dst+n > dictSize {
		return false
	}
	return src+n <= dst || dst+n <= src
}

func method4CopyMatchToDictAndOut(dict *[methodFDIC]byte, dstPos, srcPos int, out []byte) (int, int) {
	off := 0
	for off < len(out) {
		remaining := len(out) - off
		dstContig := methodFDIC - dstPos
		srcContig := methodFDIC - srcPos
		chunk := remaining
		if chunk > dstContig {
			chunk = dstContig
		}
		if chunk > srcContig {
			chunk = srcContig
		}

		if srcPos < dstPos && srcPos+chunk > dstPos {
			dist := dstPos - srcPos
			copied := 0
			for copied < chunk {
				step := chunk - copied
				if step > dist {
					step = dist
				}
				copy(
					dict[dstPos+copied:dstPos+copied+step],
					dict[srcPos+copied:srcPos+copied+step],
				)
				copied += step
			}
		} else {
			copy(dict[dstPos:dstPos+chunk], dict[srcPos:srcPos+chunk])
		}
		copy(out[off:off+chunk], dict[dstPos:dstPos+chunk])

		dstPos = (dstPos + chunk) & methodFDICMask
		srcPos = (srcPos + chunk) & methodFDICMask
		off += chunk
	}
	return dstPos, srcPos
}

func newMethod123StreamDecoder(br method14BitReader, origSize uint64) *method123StreamDecoder {
	d := &method123StreamDecoder{
		remaining: origSize,
	}
	d.dec.br = br
	return d
}

func newMethod123FastStreamDecoder(br *arjBitStreamReader, origSize uint64) *method123FastStreamDecoder {
	d := method123FastStreamDecoderPool.Get().(*method123FastStreamDecoder)
	d.dec.br = br
	d.remaining = origSize
	return d
}

func (d *method123FastStreamDecoder) Close() error {
	if d == nil {
		return nil
	}
	*d = method123FastStreamDecoder{}
	method123FastStreamDecoderPool.Put(d)
	return nil
}

func (d *method123StreamDecoder) Read(p []byte) (int, error) {
	if len(p) == 0 {
		if d.err != nil {
			return 0, d.err
		}
		return 0, nil
	}
	if d.err != nil {
		return 0, d.err
	}
	if d.remaining == 0 {
		return 0, io.EOF
	}

	n := 0
	for n < len(p) && d.remaining > 0 {
		if d.matchLen > 0 {
			run := d.matchLen
			if maxOut := len(p) - n; run > maxOut {
				run = maxOut
			}
			if maxRemain := int(d.remaining); run > maxRemain {
				run = maxRemain
			}
			if run > 0 && method14CanFastMatchCopy(d.matchSrc, d.dictPos, run, methodDICSize) {
				copy(d.dict[d.dictPos:d.dictPos+run], d.dict[d.matchSrc:d.matchSrc+run])
				copy(p[n:n+run], d.dict[d.matchSrc:d.matchSrc+run])
				d.remaining -= uint64(run)
				d.matchLen -= run
				n += run
				d.dictPos += run
				d.matchSrc += run
				if d.dictPos >= methodDICSize {
					d.dictPos -= methodDICSize
				}
				if d.matchSrc >= methodDICSize {
					d.matchSrc -= methodDICSize
				}
				continue
			}

			b := d.dict[d.matchSrc]
			d.dict[d.dictPos] = b
			p[n] = b
			n++
			d.remaining--
			d.dictPos++
			if d.dictPos >= methodDICSize {
				d.dictPos = 0
			}
			d.matchSrc++
			if d.matchSrc >= methodDICSize {
				d.matchSrc = 0
			}
			d.matchLen--
			continue
		}

		c, err := d.dec.decodeC()
		if err != nil {
			d.err = err
			break
		}
		if c <= 0xFF {
			b := byte(c)
			d.dict[d.dictPos] = b
			p[n] = b
			n++
			d.remaining--
			d.dictPos++
			if d.dictPos >= methodDICSize {
				d.dictPos = 0
			}
			continue
		}

		j := c - (0xFF + 1 - methodThresh)
		if j <= 0 || uint64(j) > d.remaining {
			d.err = ErrFormat
			break
		}
		ptr, err := d.dec.decodeP()
		if err != nil {
			d.err = err
			break
		}
		d.matchSrc = d.dictPos - ptr - 1
		for d.matchSrc < 0 {
			d.matchSrc += methodDICSize
		}
		d.matchLen = j
	}

	if n > 0 {
		return n, nil
	}
	if d.err != nil {
		return 0, d.err
	}
	return 0, io.EOF
}

func (d *method123FastStreamDecoder) Read(p []byte) (int, error) {
	if len(p) == 0 {
		if d.err != nil {
			return 0, d.err
		}
		return 0, nil
	}
	if d.err != nil {
		return 0, d.err
	}
	if d.remaining == 0 {
		return 0, io.EOF
	}

	n := 0
	for n < len(p) && d.remaining > 0 {
		if d.matchLen > 0 {
			run := d.matchLen
			if maxOut := len(p) - n; run > maxOut {
				run = maxOut
			}
			if maxRemain := int(d.remaining); run > maxRemain {
				run = maxRemain
			}
			if run > 0 && method14CanFastMatchCopy(d.matchSrc, d.dictPos, run, methodDICSize) {
				copy(d.dict[d.dictPos:d.dictPos+run], d.dict[d.matchSrc:d.matchSrc+run])
				copy(p[n:n+run], d.dict[d.matchSrc:d.matchSrc+run])
				d.remaining -= uint64(run)
				d.matchLen -= run
				n += run
				d.dictPos += run
				d.matchSrc += run
				if d.dictPos >= methodDICSize {
					d.dictPos -= methodDICSize
				}
				if d.matchSrc >= methodDICSize {
					d.matchSrc -= methodDICSize
				}
				continue
			}

			b := d.dict[d.matchSrc]
			d.dict[d.dictPos] = b
			p[n] = b
			n++
			d.remaining--
			d.dictPos++
			if d.dictPos >= methodDICSize {
				d.dictPos = 0
			}
			d.matchSrc++
			if d.matchSrc >= methodDICSize {
				d.matchSrc = 0
			}
			d.matchLen--
			continue
		}

		c, err := d.dec.decodeC()
		if err != nil {
			d.err = err
			break
		}
		if c <= 0xFF {
			b := byte(c)
			d.dict[d.dictPos] = b
			p[n] = b
			n++
			d.remaining--
			d.dictPos++
			if d.dictPos >= methodDICSize {
				d.dictPos = 0
			}
			continue
		}

		j := c - (0xFF + 1 - methodThresh)
		if j <= 0 || uint64(j) > d.remaining {
			d.err = ErrFormat
			break
		}
		ptr, err := d.dec.decodeP()
		if err != nil {
			d.err = err
			break
		}
		d.matchSrc = d.dictPos - ptr - 1
		for d.matchSrc < 0 {
			d.matchSrc += methodDICSize
		}
		d.matchLen = j
	}

	if n > 0 {
		return n, nil
	}
	if d.err != nil {
		return 0, d.err
	}
	return 0, io.EOF
}

func newMethod4StreamDecoder(br method14BitReader, origSize uint64) *method4StreamDecoder {
	return &method4StreamDecoder{
		br:        br,
		remaining: origSize,
	}
}

func newMethod4FastStreamDecoder(br *arjBitStreamReader, origSize uint64) *method4FastStreamDecoder {
	d := method4FastStreamDecoderPool.Get().(*method4FastStreamDecoder)
	d.br = br
	d.remaining = origSize
	return d
}

func (d *method4FastStreamDecoder) Close() error {
	if d == nil {
		return nil
	}
	*d = method4FastStreamDecoder{}
	method4FastStreamDecoderPool.Put(d)
	return nil
}

func (d *method4StreamDecoder) Read(p []byte) (int, error) {
	if len(p) == 0 {
		if d.err != nil {
			return 0, d.err
		}
		return 0, nil
	}
	if d.err != nil {
		return 0, d.err
	}
	if d.remaining == 0 {
		return 0, io.EOF
	}

	n := 0
	for n < len(p) && d.remaining > 0 {
		if d.matchLen > 0 {
			run := d.matchLen
			if maxOut := len(p) - n; run > maxOut {
				run = maxOut
			}
			if maxRemain := int(d.remaining); run > maxRemain {
				run = maxRemain
			}
			if run > 0 && method14CanFastMatchCopy(d.matchSrc, d.dictPos, run, methodFDIC) {
				copy(d.dict[d.dictPos:d.dictPos+run], d.dict[d.matchSrc:d.matchSrc+run])
				copy(p[n:n+run], d.dict[d.matchSrc:d.matchSrc+run])
				d.remaining -= uint64(run)
				d.matchLen -= run
				n += run
				d.dictPos = (d.dictPos + run) & methodFDICMask
				d.matchSrc = (d.matchSrc + run) & methodFDICMask
				continue
			}
			d.dictPos, d.matchSrc = method4CopyMatchToDictAndOut(&d.dict, d.dictPos, d.matchSrc, p[n:n+run])
			d.remaining -= uint64(run)
			d.matchLen -= run
			n += run
			continue
		}

		c, err := decodeMethod4Len(d.br)
		if err != nil {
			d.err = err
			break
		}
		if c == 0 {
			literal, err := d.br.getBits(8)
			if err != nil {
				d.err = err
				break
			}
			b := byte(literal)
			d.dict[d.dictPos] = b
			p[n] = b
			n++
			d.remaining--
			d.dictPos = (d.dictPos + 1) & methodFDICMask
			continue
		}

		j := c - 1 + methodThresh
		if j <= 0 || uint64(j) > d.remaining {
			d.err = ErrFormat
			break
		}
		ptr, err := decodeMethod4Ptr(d.br)
		if err != nil {
			d.err = err
			break
		}
		if ptr >= methodFDIC {
			d.err = ErrFormat
			break
		}
		d.matchSrc = (d.dictPos - ptr - 1) & methodFDICMask
		d.matchLen = j
	}

	if n > 0 {
		return n, nil
	}
	if d.err != nil {
		return 0, d.err
	}
	return 0, io.EOF
}

func (d *method4FastStreamDecoder) Read(p []byte) (int, error) {
	if len(p) == 0 {
		if d.err != nil {
			return 0, d.err
		}
		return 0, nil
	}
	if d.err != nil {
		return 0, d.err
	}
	if d.remaining == 0 {
		return 0, io.EOF
	}

	n := 0
	for n < len(p) && d.remaining > 0 {
		if d.matchLen > 0 {
			run := d.matchLen
			if maxOut := len(p) - n; run > maxOut {
				run = maxOut
			}
			if maxRemain := int(d.remaining); run > maxRemain {
				run = maxRemain
			}
			if run > 0 && method14CanFastMatchCopy(d.matchSrc, d.dictPos, run, methodFDIC) {
				copy(d.dict[d.dictPos:d.dictPos+run], d.dict[d.matchSrc:d.matchSrc+run])
				copy(p[n:n+run], d.dict[d.matchSrc:d.matchSrc+run])
				d.remaining -= uint64(run)
				d.matchLen -= run
				n += run
				d.dictPos = (d.dictPos + run) & methodFDICMask
				d.matchSrc = (d.matchSrc + run) & methodFDICMask
				continue
			}
			d.dictPos, d.matchSrc = method4CopyMatchToDictAndOut(&d.dict, d.dictPos, d.matchSrc, p[n:n+run])
			d.remaining -= uint64(run)
			d.matchLen -= run
			n += run
			continue
		}

		c, err := decodeMethod4LenStream(d.br)
		if err != nil {
			d.err = err
			break
		}
		if c == 0 {
			literal, err := d.br.getBitsFast(8)
			if err != nil {
				d.err = err
				break
			}
			b := byte(literal)
			d.dict[d.dictPos] = b
			p[n] = b
			n++
			d.remaining--
			d.dictPos = (d.dictPos + 1) & methodFDICMask
			continue
		}

		j := c - 1 + methodThresh
		if j <= 0 || uint64(j) > d.remaining {
			d.err = ErrFormat
			break
		}
		ptr, err := decodeMethod4PtrStream(d.br)
		if err != nil {
			d.err = err
			break
		}
		if ptr >= methodFDIC {
			d.err = ErrFormat
			break
		}
		d.matchSrc = (d.dictPos - ptr - 1) & methodFDICMask
		d.matchLen = j
	}

	if n > 0 {
		return n, nil
	}
	if d.err != nil {
		return 0, d.err
	}
	return 0, io.EOF
}

func (d *method123Decoder) decodeC() (int, error) {
	if d.blockSize == 0 {
		blockSize, err := d.br.getBits(methodCodeBit)
		if err != nil {
			return 0, err
		}
		d.blockSize = int(blockSize)
		if d.blockSize == 0 {
			return 0, ErrFormat
		}
		if err := d.readPtLen(methodNT, methodTBIT, 3); err != nil {
			return 0, err
		}
		if err := d.readCLen(); err != nil {
			return 0, err
		}
		if err := d.readPtLen(methodNP, methodPBIT, -1); err != nil {
			return 0, err
		}
	}
	if d.blockSize < 0 {
		return 0, ErrFormat
	}
	d.blockSize--

	peek, err := d.br.peekBits(methodCodeBit)
	if err != nil {
		return 0, err
	}
	j := int(d.cTable[peek>>4])
	if j >= methodNC {
		mask := uint16(1 << 3)
		for steps := 0; j >= methodNC; steps++ {
			if steps > len(d.left) || j < 0 || j >= len(d.left) {
				return 0, ErrFormat
			}
			if peek&mask != 0 {
				j = int(d.right[j])
			} else {
				j = int(d.left[j])
			}
			mask >>= 1
		}
	}
	if j < 0 || j >= len(d.cLen) {
		return 0, ErrFormat
	}
	if err := d.br.fillBuf(int(d.cLen[j])); err != nil {
		return 0, err
	}
	return j, nil
}

func (d *method123Decoder) decodeP() (int, error) {
	peek, err := d.br.peekBits(methodCodeBit)
	if err != nil {
		return 0, err
	}
	j := int(d.ptTable[peek>>8])
	if j >= methodNP {
		mask := uint16(1 << 7)
		for steps := 0; j >= methodNP; steps++ {
			if steps > len(d.left) || j < 0 || j >= len(d.left) {
				return 0, ErrFormat
			}
			if peek&mask != 0 {
				j = int(d.right[j])
			} else {
				j = int(d.left[j])
			}
			mask >>= 1
		}
	}
	if j < 0 || j >= len(d.ptLen) {
		return 0, ErrFormat
	}
	if err := d.br.fillBuf(int(d.ptLen[j])); err != nil {
		return 0, err
	}
	if j != 0 {
		j--
		if j > methodCodeBit {
			return 0, ErrFormat
		}
		extra, err := d.br.getBits(j)
		if err != nil {
			return 0, err
		}
		j = (1 << j) + int(extra)
	}
	return j, nil
}

func (d *method123Decoder) readPtLen(nn, nbit, iSpecial int) error {
	nBits, err := d.br.getBits(nbit)
	if err != nil {
		return err
	}
	n := int(nBits)
	if n == 0 {
		c, err := d.br.getBits(nbit)
		if err != nil {
			return err
		}
		if int(c) >= nn {
			return ErrFormat
		}
		for i := 0; i < nn; i++ {
			d.ptLen[i] = 0
		}
		for i := 0; i < methodPTable; i++ {
			d.ptTable[i] = c
		}
		return nil
	}

	i := 0
	if n >= methodNPT {
		n = methodNPT
	}
	for i < n {
		peek, err := d.br.peekBits(methodCodeBit)
		if err != nil {
			return err
		}
		c := int(peek >> 13)
		if c == 7 {
			mask := uint16(1 << 12)
			for mask&peek != 0 {
				mask >>= 1
				c++
			}
		}
		if c < 7 {
			if err := d.br.fillBuf(3); err != nil {
				return err
			}
		} else {
			if err := d.br.fillBuf(c - 3); err != nil {
				return err
			}
		}
		if i >= nn {
			return ErrFormat
		}
		d.ptLen[i] = uint8(c)
		i++
		if i == iSpecial {
			v, err := d.br.getBits(2)
			if err != nil {
				return err
			}
			c = int(v)
			for c > 0 {
				if i >= nn {
					return ErrFormat
				}
				d.ptLen[i] = 0
				i++
				c--
			}
		}
	}
	for i < nn {
		d.ptLen[i] = 0
		i++
	}
	return makeDecodeTable(nn, d.ptLen[:], 8, d.ptTable[:], methodPTable, d.left[:], d.right[:])
}

func (d *method123Decoder) readCLen() error {
	nBits, err := d.br.getBits(methodCBIT)
	if err != nil {
		return err
	}
	n := int(nBits)
	if n == 0 {
		c, err := d.br.getBits(methodCBIT)
		if err != nil {
			return err
		}
		if int(c) >= methodNC {
			return ErrFormat
		}
		for i := 0; i < methodNC; i++ {
			d.cLen[i] = 0
		}
		for i := 0; i < methodCTable; i++ {
			d.cTable[i] = c
		}
		return nil
	}

	i := 0
	for i < n {
		peek, err := d.br.peekBits(methodCodeBit)
		if err != nil {
			return err
		}
		c := int(d.ptTable[peek>>8])
		if c >= methodNT {
			mask := uint16(1 << 7)
			for steps := 0; c >= methodNT; steps++ {
				if steps > len(d.left) || c < 0 || c >= len(d.left) {
					return ErrFormat
				}
				if peek&mask != 0 {
					c = int(d.right[c])
				} else {
					c = int(d.left[c])
				}
				mask >>= 1
			}
		}
		if c < 0 || c >= len(d.ptLen) {
			return ErrFormat
		}
		if err := d.br.fillBuf(int(d.ptLen[c])); err != nil {
			return err
		}
		if c <= 2 {
			switch c {
			case 0:
				c = 1
			case 1:
				bits, err := d.br.getBits(4)
				if err != nil {
					return err
				}
				c = int(bits) + 3
			case 2:
				bits, err := d.br.getBits(methodCBIT)
				if err != nil {
					return err
				}
				c = int(bits) + 20
			}
			for c > 0 {
				if i >= methodNC {
					return ErrFormat
				}
				d.cLen[i] = 0
				i++
				c--
			}
		} else {
			if i >= methodNC {
				return ErrFormat
			}
			d.cLen[i] = uint8(c - 2)
			i++
		}
	}
	for i < methodNC {
		d.cLen[i] = 0
		i++
	}
	return makeDecodeTable(methodNC, d.cLen[:], 12, d.cTable[:], methodCTable, d.left[:], d.right[:])
}

func (d *method123BitStreamDecoder) decodeC() (int, error) {
	if d.blockSize == 0 {
		blockSize, err := d.br.getBits(methodCodeBit)
		if err != nil {
			return 0, err
		}
		d.blockSize = int(blockSize)
		if d.blockSize == 0 {
			return 0, ErrFormat
		}
		if err := d.readPtLen(methodNT, methodTBIT, 3); err != nil {
			return 0, err
		}
		if err := d.readCLen(); err != nil {
			return 0, err
		}
		if err := d.readPtLen(methodNP, methodPBIT, -1); err != nil {
			return 0, err
		}
	}
	if d.blockSize < 0 {
		return 0, ErrFormat
	}
	d.blockSize--

	peek, err := d.br.peekBits(methodCodeBit)
	if err != nil {
		return 0, err
	}
	j := int(d.cTable[peek>>4])
	if j >= methodNC {
		mask := uint16(1 << 3)
		for steps := 0; j >= methodNC; steps++ {
			if steps > len(d.left) || j < 0 || j >= len(d.left) {
				return 0, ErrFormat
			}
			if peek&mask != 0 {
				j = int(d.right[j])
			} else {
				j = int(d.left[j])
			}
			mask >>= 1
		}
	}
	if j < 0 || j >= len(d.cLen) {
		return 0, ErrFormat
	}
	if err := d.br.fillBuf(int(d.cLen[j])); err != nil {
		return 0, err
	}
	return j, nil
}

func (d *method123BitStreamDecoder) decodeP() (int, error) {
	peek, err := d.br.peekBits(methodCodeBit)
	if err != nil {
		return 0, err
	}
	j := int(d.ptTable[peek>>8])
	if j >= methodNP {
		mask := uint16(1 << 7)
		for steps := 0; j >= methodNP; steps++ {
			if steps > len(d.left) || j < 0 || j >= len(d.left) {
				return 0, ErrFormat
			}
			if peek&mask != 0 {
				j = int(d.right[j])
			} else {
				j = int(d.left[j])
			}
			mask >>= 1
		}
	}
	if j < 0 || j >= len(d.ptLen) {
		return 0, ErrFormat
	}
	if err := d.br.fillBuf(int(d.ptLen[j])); err != nil {
		return 0, err
	}
	if j != 0 {
		j--
		if j > methodCodeBit {
			return 0, ErrFormat
		}
		extra, err := d.br.getBits(j)
		if err != nil {
			return 0, err
		}
		j = (1 << j) + int(extra)
	}
	return j, nil
}

func (d *method123BitStreamDecoder) readPtLen(nn, nbit, iSpecial int) error {
	nBits, err := d.br.getBits(nbit)
	if err != nil {
		return err
	}
	n := int(nBits)
	if n == 0 {
		c, err := d.br.getBits(nbit)
		if err != nil {
			return err
		}
		if int(c) >= nn {
			return ErrFormat
		}
		for i := 0; i < nn; i++ {
			d.ptLen[i] = 0
		}
		for i := 0; i < methodPTable; i++ {
			d.ptTable[i] = c
		}
		return nil
	}

	i := 0
	if n >= methodNPT {
		n = methodNPT
	}
	for i < n {
		peek, err := d.br.peekBits(methodCodeBit)
		if err != nil {
			return err
		}
		c := int(peek >> 13)
		if c == 7 {
			mask := uint16(1 << 12)
			for mask&peek != 0 {
				mask >>= 1
				c++
			}
		}
		if c < 7 {
			if err := d.br.fillBuf(3); err != nil {
				return err
			}
		} else {
			if err := d.br.fillBuf(c - 3); err != nil {
				return err
			}
		}
		if i >= nn {
			return ErrFormat
		}
		d.ptLen[i] = uint8(c)
		i++
		if i == iSpecial {
			v, err := d.br.getBits(2)
			if err != nil {
				return err
			}
			c = int(v)
			for c > 0 {
				if i >= nn {
					return ErrFormat
				}
				d.ptLen[i] = 0
				i++
				c--
			}
		}
	}
	for i < nn {
		d.ptLen[i] = 0
		i++
	}
	return makeDecodeTable(nn, d.ptLen[:], 8, d.ptTable[:], methodPTable, d.left[:], d.right[:])
}

func (d *method123BitStreamDecoder) readCLen() error {
	nBits, err := d.br.getBits(methodCBIT)
	if err != nil {
		return err
	}
	n := int(nBits)
	if n == 0 {
		c, err := d.br.getBits(methodCBIT)
		if err != nil {
			return err
		}
		if int(c) >= methodNC {
			return ErrFormat
		}
		for i := 0; i < methodNC; i++ {
			d.cLen[i] = 0
		}
		for i := 0; i < methodCTable; i++ {
			d.cTable[i] = c
		}
		return nil
	}

	i := 0
	for i < n {
		peek, err := d.br.peekBits(methodCodeBit)
		if err != nil {
			return err
		}
		c := int(d.ptTable[peek>>8])
		if c >= methodNT {
			mask := uint16(1 << 7)
			for steps := 0; c >= methodNT; steps++ {
				if steps > len(d.left) || c < 0 || c >= len(d.left) {
					return ErrFormat
				}
				if peek&mask != 0 {
					c = int(d.right[c])
				} else {
					c = int(d.left[c])
				}
				mask >>= 1
			}
		}
		if c < 0 || c >= len(d.ptLen) {
			return ErrFormat
		}
		if err := d.br.fillBuf(int(d.ptLen[c])); err != nil {
			return err
		}
		if c <= 2 {
			switch c {
			case 0:
				c = 1
			case 1:
				bits, err := d.br.getBits(4)
				if err != nil {
					return err
				}
				c = int(bits) + 3
			case 2:
				bits, err := d.br.getBits(methodCBIT)
				if err != nil {
					return err
				}
				c = int(bits) + 20
			}
			for c > 0 {
				if i >= methodNC {
					return ErrFormat
				}
				d.cLen[i] = 0
				i++
				c--
			}
		} else {
			if i >= methodNC {
				return ErrFormat
			}
			d.cLen[i] = uint8(c - 2)
			i++
		}
	}
	for i < methodNC {
		d.cLen[i] = 0
		i++
	}
	return makeDecodeTable(methodNC, d.cLen[:], 12, d.cTable[:], methodCTable, d.left[:], d.right[:])
}

func makeDecodeTable(nchar int, bitLen []uint8, tableBits int, table []uint16, tableSize int, left, right []uint16) error {
	var count [17]uint32
	var weight [17]uint32
	var start [18]uint32

	if nchar <= 0 || nchar > len(bitLen) {
		return ErrFormat
	}
	if tableBits <= 0 || tableBits > 16 {
		return ErrFormat
	}
	if tableSize <= 0 || tableSize > len(table) {
		return ErrFormat
	}
	if len(left) == 0 || len(left) != len(right) {
		return ErrFormat
	}
	for i := 0; i < tableSize; i++ {
		table[i] = 0
	}

	for i := 0; i < nchar; i++ {
		bl := bitLen[i]
		if bl > 16 {
			return ErrFormat
		}
		count[bl]++
	}
	start[1] = 0
	for i := 1; i <= 16; i++ {
		start[i+1] = start[i] + (count[i] << (16 - i))
	}
	if start[17] != 1<<16 {
		return ErrFormat
	}

	jutBits := 16 - tableBits
	i := 1
	for ; i <= tableBits; i++ {
		start[i] >>= jutBits
		weight[i] = 1 << (tableBits - i)
	}
	for ; i <= 16; i++ {
		weight[i] = 1 << (16 - i)
	}
	tableStart := int(start[tableBits+1] >> jutBits)
	if tableStart != 0 {
		k := 1 << tableBits
		if tableStart > k {
			return ErrFormat
		}
		for tableStart != k {
			if tableStart < 0 || tableStart >= len(table) {
				return ErrFormat
			}
			table[tableStart] = 0
			tableStart++
		}
	}

	avail := nchar
	mask := uint32(1 << (15 - tableBits))
	for ch := 0; ch < nchar; ch++ {
		ln := int(bitLen[ch])
		if ln == 0 {
			continue
		}
		k := start[ln]
		nextCodeWide := k + weight[ln]
		if nextCodeWide > 1<<16 {
			return ErrFormat
		}
		nextCode := nextCodeWide
		if ln <= tableBits {
			if int(nextCode) > tableSize {
				return ErrFormat
			}
			for j := int(k); j < int(nextCode); j++ {
				table[j] = uint16(ch)
			}
		} else {
			tableIndex := int(k >> jutBits)
			if tableIndex < 0 || tableIndex >= tableSize {
				return ErrFormat
			}
			p := &table[tableIndex]
			iter := ln - tableBits
			for iter > 0 {
				if *p == 0 {
					if avail >= len(left) {
						return ErrFormat
					}
					left[avail] = 0
					right[avail] = 0
					*p = uint16(avail)
					avail++
				}
				if int(*p) >= len(left) {
					return ErrFormat
				}
				if k&mask != 0 {
					p = &right[*p]
				} else {
					p = &left[*p]
				}
				k <<= 1
				iter--
			}
			*p = uint16(ch)
		}
		start[ln] = nextCode
	}
	return nil
}

func newARJBitReader(data []byte) *arjBitReader {
	br := &arjBitReader{
		data:      data,
		totalBits: uint64(len(data)) << 3,
	}
	br.refreshBitBuf()
	return br
}

func newARJBitStreamReader(r io.Reader, compressedSize int64) *arjBitStreamReader {
	br := &arjBitStreamReader{
		r:         r,
		remaining: compressedSize,
	}
	if compressedSize >= 0 {
		br.totalBits = uint64(compressedSize) << 3
	} else {
		br.err = ErrFormat
	}
	return br
}

func newARJBitWriter(dst io.Writer) *arjBitWriter {
	return &arjBitWriter{dst: dst}
}

func (br *arjBitReader) refreshBitBuf() {
	bytePos := int(br.bitPos >> 3)
	bitOffset := uint(br.bitPos & 7)

	var window uint32
	if bytePos < len(br.data) {
		window |= uint32(br.data[bytePos]) << 16
	}
	if bytePos+1 < len(br.data) {
		window |= uint32(br.data[bytePos+1]) << 8
	}
	if bytePos+2 < len(br.data) {
		window |= uint32(br.data[bytePos+2])
	}

	window <<= bitOffset
	br.bitBuf = uint16(window >> 8)
}

func (br *arjBitReader) peekBits(n int) (uint16, error) {
	if n < 0 || n > methodCodeBit || br.bitPos > br.totalBits {
		br.err = ErrFormat
		return 0, br.err
	}
	if n == 0 {
		return 0, nil
	}
	if br.err != nil {
		return 0, br.err
	}
	return br.bitBuf >> (methodCodeBit - n), nil
}

func method14AdvanceBitPos(bitPos, totalBits uint64, n int) (uint64, bool) {
	if n < 0 || n > methodCodeBit {
		return 0, false
	}
	if n == 0 {
		return bitPos, true
	}
	if bitPos > totalBits {
		return 0, false
	}
	step := uint64(n)
	if step > totalBits-bitPos {
		return 0, false
	}
	return bitPos + step, true
}

func (br *arjBitReader) fillBuf(n int) error {
	nextPos, ok := method14AdvanceBitPos(br.bitPos, br.totalBits, n)
	if !ok {
		br.err = ErrFormat
		return br.err
	}
	if n == 0 {
		return nil
	}
	if br.err != nil {
		return br.err
	}
	br.bitPos = nextPos
	br.refreshBitBuf()
	return nil
}

func (br *arjBitReader) getBits(n int) (uint16, error) {
	nextPos, ok := method14AdvanceBitPos(br.bitPos, br.totalBits, n)
	if !ok {
		br.err = ErrFormat
		return 0, br.err
	}
	if n == 0 {
		return 0, nil
	}
	if br.err != nil {
		return 0, br.err
	}
	v := br.bitBuf >> (methodCodeBit - n)
	br.bitPos = nextPos
	br.refreshBitBuf()
	return v, nil
}

func method14LowBitMask32(n int) uint32 {
	if n <= 0 {
		return 0
	}
	if n >= 32 {
		return ^uint32(0)
	}
	return (uint32(1) << n) - 1
}

func method14LowBitMask64(n int) uint64 {
	if n <= 0 {
		return 0
	}
	if n >= 64 {
		return ^uint64(0)
	}
	return (uint64(1) << n) - 1
}

func (br *arjBitStreamReader) fillLookahead(n int) error {
	if n <= 0 {
		return nil
	}
	if br.err != nil {
		return br.err
	}
	for int(br.bitCount) < n && br.remaining > 0 {
		roomBytes := (64 - int(br.bitCount)) / 8
		if roomBytes <= 0 {
			br.err = ErrFormat
			return br.err
		}

		// Pull as much as fits in the bit window to amortize io.ReadFull call
		// overhead for frequent 1-bit/8-bit decode reads.
		readN := roomBytes
		if readN > len(br.scratch) {
			readN = len(br.scratch)
		}
		if remaining := int(br.remaining); readN > remaining {
			readN = remaining
		}
		if readN <= 0 {
			break
		}

		// Only unread low bits are meaningful; trim once per refill instead of
		// per-bit in fast decode paths.
		if br.bitCount > 0 {
			br.bitBuf &= method14LowBitMask64(int(br.bitCount))
		}

		if _, err := io.ReadFull(br.r, br.scratch[:readN]); err != nil {
			br.err = ErrFormat
			return br.err
		}
		br.remaining -= int64(readN)
		for i := 0; i < readN; i++ {
			br.bitBuf <<= 8
			br.bitBuf |= uint64(br.scratch[i])
			br.bitCount += 8
		}
	}
	return nil
}

func (br *arjBitStreamReader) peekBits(n int) (uint16, error) {
	if n < 0 || n > methodCodeBit || br.bitPos > br.totalBits {
		br.err = ErrFormat
		return 0, br.err
	}
	if n == 0 {
		return 0, nil
	}
	if err := br.fillLookahead(n); err != nil {
		return 0, err
	}

	if int(br.bitCount) >= n {
		shift := int(br.bitCount) - n
		return uint16((br.bitBuf >> shift) & method14LowMask16[n]), nil
	}
	if br.bitCount == 0 {
		return 0, nil
	}
	return uint16((br.bitBuf << (n - int(br.bitCount))) & method14LowMask16[n]), nil
}

func (br *arjBitStreamReader) fillBuf(n int) error {
	nextPos, ok := method14AdvanceBitPos(br.bitPos, br.totalBits, n)
	if !ok {
		br.err = ErrFormat
		return br.err
	}
	if n == 0 {
		return nil
	}
	if br.err != nil {
		return br.err
	}
	if err := br.fillLookahead(n); err != nil {
		return err
	}
	if int(br.bitCount) < n {
		br.err = ErrFormat
		return br.err
	}
	br.bitCount -= uint8(n)
	br.bitPos = nextPos
	if br.bitCount == 0 {
		br.bitBuf = 0
	} else {
		br.bitBuf &= method14LowBitMask64(int(br.bitCount))
	}
	return nil
}

func (br *arjBitStreamReader) getBits(n int) (uint16, error) {
	nextPos, ok := method14AdvanceBitPos(br.bitPos, br.totalBits, n)
	if !ok {
		br.err = ErrFormat
		return 0, br.err
	}
	if n == 0 {
		return 0, nil
	}
	if br.err != nil {
		return 0, br.err
	}
	if err := br.fillLookahead(n); err != nil {
		return 0, err
	}
	if int(br.bitCount) < n {
		br.err = ErrFormat
		return 0, br.err
	}
	shift := int(br.bitCount) - n
	v := uint16((br.bitBuf >> shift) & method14LowMask16[n])
	br.bitCount -= uint8(n)
	br.bitPos = nextPos
	if br.bitCount == 0 {
		br.bitBuf = 0
	} else {
		br.bitBuf &= method14LowBitMask64(int(br.bitCount))
	}
	return v, nil
}

func (br *arjBitStreamReader) getBitsFast(n int) (uint16, error) {
	if n <= 0 || n > methodCodeBit {
		br.err = ErrFormat
		return 0, br.err
	}
	if br.err != nil {
		return 0, br.err
	}
	if int(br.bitCount) < n {
		if err := br.fillLookahead(n); err != nil {
			return 0, err
		}
		if int(br.bitCount) < n {
			br.err = ErrFormat
			return 0, br.err
		}
	}
	shift := int(br.bitCount) - n
	v := uint16((br.bitBuf >> shift) & method14LowMask16[n])
	br.bitCount -= uint8(n)
	br.bitPos += uint64(n)
	if br.bitCount == 0 {
		br.bitBuf = 0
	}
	return v, nil
}

func (br *arjBitStreamReader) getBitFast() (uint16, error) {
	if br.err != nil {
		return 0, br.err
	}
	if br.bitCount == 0 {
		if err := br.fillLookahead(1); err != nil {
			return 0, err
		}
		if br.bitCount == 0 {
			br.err = ErrFormat
			return 0, br.err
		}
	}
	br.bitCount--
	br.bitPos++
	v := uint16((br.bitBuf >> br.bitCount) & 1)
	if br.bitCount == 0 {
		br.bitBuf = 0
	}
	return v, nil
}

func (bw *arjBitWriter) emitByte(b byte) {
	if bw.err != nil {
		return
	}
	if bw.dst == nil {
		bw.out = append(bw.out, b)
		return
	}
	bw.writeBuf[bw.writeN] = b
	bw.writeN++
	if bw.writeN == len(bw.writeBuf) {
		bw.flushOutput()
	}
}

func (bw *arjBitWriter) flushOutput() {
	if bw.err != nil || bw.dst == nil || bw.writeN == 0 {
		return
	}
	n, err := bw.dst.Write(bw.writeBuf[:bw.writeN])
	if err != nil {
		bw.err = err
		return
	}
	if n != bw.writeN {
		bw.err = io.ErrShortWrite
		return
	}
	bw.writeN = 0
}

func (bw *arjBitWriter) putBits(n int, x uint16) {
	if n <= 0 || bw.err != nil {
		return
	}

	bw.bitBuf <<= n
	if n >= methodCodeBit {
		bw.bitBuf |= uint32(x)
	} else {
		bw.bitBuf |= uint32(x & ((uint16(1) << n) - 1))
	}
	bw.bitCount += uint8(n)

	for bw.bitCount >= 8 {
		shift := bw.bitCount - 8
		bw.emitByte(byte(bw.bitBuf >> shift))
		if bw.err != nil {
			return
		}
		bw.bitCount -= 8
		if bw.bitCount == 0 {
			bw.bitBuf = 0
			continue
		}
		bw.bitBuf &= (uint32(1) << bw.bitCount) - 1
	}
}

func (bw *arjBitWriter) finishWithShutdownPadding() []byte {
	bw.putBits(7, 0)
	bw.flushOutput()
	return bw.out
}
