package arj

import (
	"bytes"
	"fmt"
	"io"
)

const (
	methodCodeBit = 16
	methodThresh  = 3
	methodDICSize = 26624
	methodFDIC    = 32768
	methodMaxM    = 256
	methodNC      = 255 + methodMaxM + 2 - methodThresh
	methodNP      = 16 + 1
	methodCBIT    = 9
	methodNT      = methodCodeBit + 3
	methodPBIT    = 5
	methodTBIT    = 5
	methodNPT     = methodNT
	methodCTable  = 4096
	methodPTable  = 256
)

type method14Input struct {
	io.Reader
	compressedSize   int64
	uncompressedSize uint64
	limits           Method14DecodeLimits
}

type method14Compressor struct {
	method uint16
	dst    io.Writer
	buf    *entryBuffer
	limit  uint64
	closed bool
}

type method14ErrorReadCloser struct {
	err error
}

type arjBitWriter struct {
	out      []byte
	bitBuf   uint32
	bitCount uint8
}

type arjBitReader struct {
	data      []byte
	bitPos    uint64
	totalBits uint64
	bitBuf    uint16
	err       error
}

type method123Decoder struct {
	br        *arjBitReader
	blockSize int

	ptLen   [methodNPT]uint8
	cLen    [methodNC]uint8
	left    [2*methodNC - 1]uint16
	right   [2*methodNC - 1]uint16
	cTable  [methodCTable]uint16
	ptTable [methodPTable]uint16
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
		return &method14Compressor{
			method: method,
			dst:    w,
			buf:    newEntryBuffer(limit, bufferScopeMethod14Input),
			limit:  limit,
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

		compressed, err := readMethod14CompressedPayload(ctx.Reader, ctx.compressedSize, ctx.limits)
		if err != nil {
			return &method14ErrorReadCloser{
				err: fmt.Errorf("arj: method %d failed to read compressed payload: %w", method, err),
			}
		}

		plain, err := decompressMethod14PayloadWithLimits(method, compressed, ctx.uncompressedSize, ctx.limits)
		if err != nil {
			return &method14ErrorReadCloser{err: err}
		}
		return io.NopCloser(bytes.NewReader(plain))
	}
}

func readMethod14CompressedPayload(r io.Reader, size int64, limits Method14DecodeLimits) ([]byte, error) {
	const maxInt = int(^uint(0) >> 1)
	limits = normalizeMethod14DecodeLimits(limits)
	if size < 0 {
		return nil, ErrFormat
	}
	if uint64(size) > limits.MaxCompressedSize {
		return nil, ErrFormat
	}
	if size > int64(maxInt) {
		return nil, ErrFormat
	}

	buf := make([]byte, int(size))
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func (w *method14Compressor) Write(p []byte) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("arj: write to closed compressor")
	}
	if w.buf == nil {
		w.buf = newEntryBuffer(w.limit, bufferScopeMethod14Input)
	}
	return w.buf.Write(p)
}

func (w *method14Compressor) setMethod14InputBufferLimit(limit uint64) {
	w.limit = normalizeWriteBufferLimits(WriteBufferLimits{
		MaxMethod14InputBufferSize: limit,
	}).MaxMethod14InputBufferSize
	if w.buf != nil {
		w.buf.limit = w.limit
		w.buf.memLimit = w.limit
		if w.buf.memLimit > maxInMemoryEntrySpoolSize {
			w.buf.memLimit = maxInMemoryEntrySpoolSize
		}
	}
}

func (w *method14Compressor) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if w.buf == nil {
		w.buf = newEntryBuffer(w.limit, bufferScopeMethod14Input)
	}
	defer func() { _ = w.buf.Close() }()

	plain, err := method14ReadCompressorInput(w.buf)
	if err != nil {
		return err
	}
	compressed, err := compressMethod14Payload(w.method, plain)
	if err != nil {
		return err
	}
	n, err := w.dst.Write(compressed)
	if err != nil {
		return err
	}
	if n != len(compressed) {
		return io.ErrShortWrite
	}
	return nil
}

func method14ReadCompressorInput(buf *entryBuffer) ([]byte, error) {
	if buf == nil || buf.Size() == 0 {
		return nil, nil
	}
	if buf.file == nil {
		return buf.mem.Bytes(), nil
	}
	const maxInt = int(^uint(0) >> 1)
	if buf.Size() > uint64(maxInt) {
		return nil, ErrBufferLimitExceeded
	}
	return buf.sliceAt(0, int(buf.Size()))
}

func (r *method14ErrorReadCloser) Read([]byte) (int, error) {
	return 0, r.err
}

func (r *method14ErrorReadCloser) Close() error {
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

	dec := &method123Decoder{
		br: newARJBitReader(compressed),
	}
	dict := make([]byte, methodDICSize)
	out := make([]byte, int(origSize))
	outPos := 0

	count := origSize
	r := 0
	for count > 0 {
		c, err := dec.decodeC()
		if err != nil {
			return nil, err
		}
		if c <= 0xFF {
			b := byte(c)
			dict[r] = b
			out[outPos] = b
			outPos++
			count--
			r++
			if r >= methodDICSize {
				r = 0
			}
			continue
		}

		j := c - (0xFF + 1 - methodThresh)
		if j <= 0 || uint64(j) > count {
			return nil, ErrFormat
		}
		count -= uint64(j)

		p, err := dec.decodeP()
		if err != nil {
			return nil, err
		}
		if p >= methodDICSize {
			return nil, ErrFormat
		}
		i := r - p - 1
		if i < 0 {
			i += methodDICSize
		}
		matchLen := j
		if method14CanFastMatchCopy(i, r, matchLen, methodDICSize) {
			src := dict[i : i+matchLen]
			copy(dict[r:r+matchLen], src)
			copy(out[outPos:outPos+matchLen], src)

			outPos += matchLen
			r += matchLen
			if r == methodDICSize {
				r = 0
			}
			continue
		}

		for ; j > 0; j-- {
			b := dict[i]
			dict[r] = b
			out[outPos] = b
			outPos++

			r++
			if r >= methodDICSize {
				r = 0
			}
			i++
			if i >= methodDICSize {
				i = 0
			}
		}
	}
	return out, nil
}

func decodeMethod4(compressed []byte, origSize uint64, limits Method14DecodeLimits) ([]byte, error) {
	if err := validateMethod14DecodeBufferSizes(limits, uint64(len(compressed)), origSize); err != nil {
		return nil, err
	}
	if origSize == 0 {
		return nil, nil
	}

	br := newARJBitReader(compressed)
	dict := make([]byte, methodFDIC)
	out := make([]byte, int(origSize))
	outPos := 0

	var produced uint64
	r := 0
	for produced < origSize {
		c, err := decodeMethod4Len(br)
		if err != nil {
			return nil, err
		}
		if c == 0 {
			literal, err := br.getBits(8)
			if err != nil {
				return nil, err
			}
			b := byte(literal)

			dict[r] = b
			out[outPos] = b
			outPos++
			produced++

			r++
			if r >= methodFDIC {
				r = 0
			}
			continue
		}

		j := c - 1 + methodThresh
		if j <= 0 || uint64(j) > origSize-produced {
			return nil, ErrFormat
		}
		produced += uint64(j)

		p, err := decodeMethod4Ptr(br)
		if err != nil {
			return nil, err
		}
		if p >= methodFDIC {
			return nil, ErrFormat
		}
		i := r - p - 1
		if i < 0 {
			i += methodFDIC
		}
		matchLen := j
		if method14CanFastMatchCopy(i, r, matchLen, methodFDIC) {
			src := dict[i : i+matchLen]
			copy(dict[r:r+matchLen], src)
			copy(out[outPos:outPos+matchLen], src)

			outPos += matchLen
			r += matchLen
			if r == methodFDIC {
				r = 0
			}
			continue
		}

		for ; j > 0; j-- {
			b := dict[i]
			dict[r] = b
			out[outPos] = b
			outPos++

			r++
			if r >= methodFDIC {
				r = 0
			}
			i++
			if i >= methodFDIC {
				i = 0
			}
		}
	}
	return out, nil
}

func decodeMethod4Ptr(br *arjBitReader) (int, error) {
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

func decodeMethod4Len(br *arjBitReader) (int, error) {
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

func method14CanFastMatchCopy(src, dst, n, dictSize int) bool {
	if n <= 0 || src < 0 || dst < 0 {
		return false
	}
	if src+n > dictSize || dst+n > dictSize {
		return false
	}
	return src+n <= dst || dst+n <= src
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

	j := int(d.cTable[d.br.bitBuf>>4])
	if j >= methodNC {
		mask := uint16(1 << 3)
		for steps := 0; j >= methodNC; steps++ {
			if steps > len(d.left) || j < 0 || j >= len(d.left) {
				return 0, ErrFormat
			}
			if d.br.bitBuf&mask != 0 {
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
	j := int(d.ptTable[d.br.bitBuf>>8])
	if j >= methodNP {
		mask := uint16(1 << 7)
		for steps := 0; j >= methodNP; steps++ {
			if steps > len(d.left) || j < 0 || j >= len(d.left) {
				return 0, ErrFormat
			}
			if d.br.bitBuf&mask != 0 {
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
		d.resetDecodeTree()
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
		c := int(d.br.bitBuf >> 13)
		if c == 7 {
			mask := uint16(1 << 12)
			for mask&d.br.bitBuf != 0 {
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
	d.resetDecodeTree()
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
		d.resetDecodeTree()
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
		c := int(d.ptTable[d.br.bitBuf>>8])
		if c >= methodNT {
			mask := uint16(1 << 7)
			for steps := 0; c >= methodNT; steps++ {
				if steps > len(d.left) || c < 0 || c >= len(d.left) {
					return ErrFormat
				}
				if d.br.bitBuf&mask != 0 {
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
	d.resetDecodeTree()
	return makeDecodeTable(methodNC, d.cLen[:], 12, d.cTable[:], methodCTable, d.left[:], d.right[:])
}

func makeDecodeTable(nchar int, bitLen []uint8, tableBits int, table []uint16, tableSize int, left, right []uint16) error {
	var count [17]uint16
	var weight [17]uint16
	var start [18]uint16

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
	if start[17] != 0 {
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
	mask := uint16(1 << (15 - tableBits))
	for ch := 0; ch < nchar; ch++ {
		ln := int(bitLen[ch])
		if ln == 0 {
			continue
		}
		k := start[ln]
		nextCode := k + weight[ln]
		if nextCode < k {
			return ErrFormat
		}
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

func (d *method123Decoder) resetDecodeTree() {
	for i := range d.left {
		d.left[i] = 0
		d.right[i] = 0
	}
}

func newARJBitReader(data []byte) *arjBitReader {
	br := &arjBitReader{
		data:      data,
		totalBits: uint64(len(data)) << 3,
	}
	br.refreshBitBuf()
	return br
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

func (bw *arjBitWriter) putBits(n int, x uint16) {
	if n <= 0 {
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
		bw.out = append(bw.out, byte(bw.bitBuf>>shift))
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
	return bw.out
}
