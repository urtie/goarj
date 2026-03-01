package arj

import "sync"

const (
	method123BlockMaxTokens = 0xFFFF
	method123HashBits       = 15
	method123HashSize       = 1 << method123HashBits
	method123HashMask       = method123HashSize - 1
	method123HashChainLimit = 256
	method123NoPos          = int32(-1)
	method123MaxNarrowPos   = int32(^uint32(0) >> 1)
	method123MaxPooledPrev  = 1 << 20
)

type method123Token struct {
	literal byte
	length  uint16
	dist    uint16
}

type method123HuffmanTree struct {
	lengths []uint8
	codes   []uint16
	root    int
}

type method123BitEncoder struct {
	bw arjBitWriter
}

func encodeMethod123Native(plain []byte) []byte {
	if len(plain) == 0 {
		return nil
	}

	enc := method123BitEncoder{}
	method123TokenizeGreedyBlocks(plain, enc.writeBlock)

	return enc.bw.finishWithShutdownPadding()
}

var (
	method123Head32Pool = sync.Pool{
		New: func() any {
			return make([]int32, method123HashSize)
		},
	}
	method123HeadWidePool = sync.Pool{
		New: func() any {
			return make([]int, method123HashSize)
		},
	}
	method123Prev32Pool     sync.Pool
	method123PrevWidePool   sync.Pool
	method123BlockTokenPool = sync.Pool{
		New: func() any {
			return make([]method123Token, 0, method123BlockMaxTokens)
		},
	}
)

func method123AcquireHead32() []int32 {
	head := method123Head32Pool.Get().([]int32)
	for i := range head {
		head[i] = method123NoPos
	}
	return head
}

func method123ReleaseHead32(head []int32) {
	if cap(head) < method123HashSize {
		return
	}
	method123Head32Pool.Put(head[:method123HashSize])
}

func method123AcquireHeadWide() []int {
	head := method123HeadWidePool.Get().([]int)
	for i := range head {
		head[i] = -1
	}
	return head
}

func method123ReleaseHeadWide(head []int) {
	if cap(head) < method123HashSize {
		return
	}
	method123HeadWidePool.Put(head[:method123HashSize])
}

func method123AcquirePrev32(n int) []int32 {
	if n <= method123MaxPooledPrev {
		if v := method123Prev32Pool.Get(); v != nil {
			prev := v.([]int32)
			if cap(prev) >= n {
				prev = prev[:n]
				for i := range prev {
					prev[i] = method123NoPos
				}
				return prev
			}
		}
	}

	prev := make([]int32, n)
	for i := range prev {
		prev[i] = method123NoPos
	}
	return prev
}

func method123ReleasePrev32(prev []int32) {
	if cap(prev) == 0 || cap(prev) > method123MaxPooledPrev {
		return
	}
	method123Prev32Pool.Put(prev[:cap(prev)])
}

func method123AcquirePrevWide(n int) []int {
	if n <= method123MaxPooledPrev {
		if v := method123PrevWidePool.Get(); v != nil {
			prev := v.([]int)
			if cap(prev) >= n {
				prev = prev[:n]
				for i := range prev {
					prev[i] = -1
				}
				return prev
			}
		}
	}

	prev := make([]int, n)
	for i := range prev {
		prev[i] = -1
	}
	return prev
}

func method123ReleasePrevWide(prev []int) {
	if cap(prev) == 0 || cap(prev) > method123MaxPooledPrev {
		return
	}
	method123PrevWidePool.Put(prev[:cap(prev)])
}

func method123AcquireBlockTokens() []method123Token {
	return method123BlockTokenPool.Get().([]method123Token)[:0]
}

func method123ReleaseBlockTokens(tokens []method123Token) {
	if cap(tokens) < method123BlockMaxTokens {
		return
	}
	method123BlockTokenPool.Put(tokens[:0])
}

func (e *method123BitEncoder) writeBlock(tokens []method123Token) {
	if len(tokens) == 0 {
		return
	}

	var cFreq [methodNC]uint32
	var pFreq [methodNP]uint32
	for _, tok := range tokens {
		cFreq[tok.cSymbol()]++
		if tok.isMatch() {
			pFreq[method123PClass(int(tok.dist)-1)]++
		}
	}

	cTree := method123BuildHuffman(cFreq[:])

	e.bw.putBits(methodCodeBit, uint16(len(tokens)))
	if cTree.root >= methodNC {
		tFreq := method123CountTFreq(cTree.lengths)
		tTree := method123BuildHuffman(tFreq[:])
		if tTree.root >= methodNT {
			e.writePtLen(tTree.lengths, methodNT, methodTBIT, 3)
		} else {
			e.bw.putBits(methodTBIT, 0)
			e.bw.putBits(methodTBIT, uint16(tTree.root))
		}
		e.writeCLen(cTree.lengths, tTree.lengths, tTree.codes)
	} else {
		e.bw.putBits(methodTBIT, 0)
		e.bw.putBits(methodTBIT, 0)
		e.bw.putBits(methodCBIT, 0)
		e.bw.putBits(methodCBIT, uint16(cTree.root))
	}

	pTree := method123BuildHuffman(pFreq[:])
	if pTree.root >= methodNP {
		e.writePtLen(pTree.lengths, methodNP, methodPBIT, -1)
	} else {
		e.bw.putBits(methodPBIT, 0)
		e.bw.putBits(methodPBIT, uint16(pTree.root))
	}

	for _, tok := range tokens {
		c := tok.cSymbol()
		e.bw.putBits(int(cTree.lengths[c]), cTree.codes[c])
		if !tok.isMatch() {
			continue
		}

		p := int(tok.dist) - 1
		qc := method123PClass(p)
		e.bw.putBits(int(pTree.lengths[qc]), pTree.codes[qc])
		if qc > 1 {
			e.bw.putBits(qc-1, uint16(p))
		}
	}
}

func (e *method123BitEncoder) writePtLen(ptLen []uint8, n, nbit, iSpecial int) {
	for n > 0 && ptLen[n-1] == 0 {
		n--
	}
	e.bw.putBits(nbit, uint16(n))

	i := 0
	for i < n {
		k := int(ptLen[i])
		i++
		if k <= 6 {
			e.bw.putBits(3, uint16(k))
		} else {
			e.bw.putBits(k-3, 0xFFFE)
		}
		if i == iSpecial {
			for i < 6 && ptLen[i] == 0 {
				i++
			}
			e.bw.putBits(2, uint16(i-3))
		}
	}
}

func (e *method123BitEncoder) writeCLen(cLen, ptLen []uint8, ptCode []uint16) {
	n := methodNC
	for n > 0 && cLen[n-1] == 0 {
		n--
	}
	e.bw.putBits(methodCBIT, uint16(n))

	i := 0
	for i < n {
		k := int(cLen[i])
		i++
		if k == 0 {
			count := 1
			for i < n && cLen[i] == 0 {
				i++
				count++
			}
			if count <= 2 {
				for j := 0; j < count; j++ {
					e.bw.putBits(int(ptLen[0]), ptCode[0])
				}
			} else if count <= 18 {
				e.bw.putBits(int(ptLen[1]), ptCode[1])
				e.bw.putBits(4, uint16(count-3))
			} else if count == 19 {
				e.bw.putBits(int(ptLen[0]), ptCode[0])
				e.bw.putBits(int(ptLen[1]), ptCode[1])
				e.bw.putBits(4, 15)
			} else {
				e.bw.putBits(int(ptLen[2]), ptCode[2])
				e.bw.putBits(methodCBIT, uint16(count-20))
			}
			continue
		}
		e.bw.putBits(int(ptLen[k+2]), ptCode[k+2])
	}
}

func method123BuildHuffman(freq []uint32) method123HuffmanTree {
	n := len(freq)
	lengths := make([]uint8, n)
	codes := make([]uint16, n)

	heap := make([]int, n+1)
	nodeFreq := make([]uint32, 2*n)
	left := make([]int, 2*n)
	right := make([]int, 2*n)

	heapsize := 0
	heap[1] = 0
	for i := 0; i < n; i++ {
		nodeFreq[i] = freq[i]
		if freq[i] != 0 {
			heapsize++
			heap[heapsize] = i
		}
	}

	if heapsize < 2 {
		root := heap[1]
		codes[root] = 0
		return method123HuffmanTree{
			lengths: lengths,
			codes:   codes,
			root:    root,
		}
	}

	downheap := func(i int) {
		k := heap[i]
		for {
			j := 2 * i
			if j > heapsize {
				break
			}
			if j < heapsize && nodeFreq[heap[j]] > nodeFreq[heap[j+1]] {
				j++
			}
			if nodeFreq[k] <= nodeFreq[heap[j]] {
				break
			}
			heap[i] = heap[j]
			i = j
		}
		heap[i] = k
	}

	for i := heapsize / 2; i >= 1; i-- {
		downheap(i)
	}

	sortOrder := make([]int, 0, n)
	avail := n
	for heapsize > 1 {
		i := heap[1]
		if i < n {
			sortOrder = append(sortOrder, i)
		}
		heap[1] = heap[heapsize]
		heapsize--
		downheap(1)

		j := heap[1]
		if j < n {
			sortOrder = append(sortOrder, j)
		}

		k := avail
		avail++
		nodeFreq[k] = nodeFreq[i] + nodeFreq[j]
		heap[1] = k
		downheap(1)
		left[k] = i
		right[k] = j
	}
	root := heap[1]

	var lenCnt [17]int
	var countLen func(node, depth int)
	countLen = func(node, depth int) {
		if node < n {
			if depth > 16 {
				depth = 16
			}
			lenCnt[depth]++
			return
		}
		depth++
		countLen(left[node], depth)
		countLen(right[node], depth)
	}
	countLen(root, 0)

	var cum uint16
	for i := 16; i > 0; i-- {
		cum += uint16(lenCnt[i] << (16 - i))
	}
	for cum != 0 {
		lenCnt[16]--
		for i := 15; i > 0; i-- {
			if lenCnt[i] == 0 {
				continue
			}
			lenCnt[i]--
			lenCnt[i+1] += 2
			break
		}
		cum--
	}

	idx := 0
	for bits := 16; bits > 0; bits-- {
		for j := 0; j < lenCnt[bits]; j++ {
			lengths[sortOrder[idx]] = uint8(bits)
			idx++
		}
	}

	var count [17]int
	var start [18]int
	for i := 0; i < n; i++ {
		count[lengths[i]]++
	}
	start[1] = 0
	for i := 1; i <= 16; i++ {
		start[i+1] = (start[i] + count[i]) << 1
	}
	for i := 0; i < n; i++ {
		if lengths[i] == 0 {
			continue
		}
		codes[i] = uint16(start[lengths[i]])
		start[lengths[i]]++
	}

	return method123HuffmanTree{
		lengths: lengths,
		codes:   codes,
		root:    root,
	}
}

func method123CountTFreq(cLen []uint8) [methodNT]uint32 {
	var tFreq [methodNT]uint32
	n := methodNC
	for n > 0 && cLen[n-1] == 0 {
		n--
	}

	i := 0
	for i < n {
		k := int(cLen[i])
		i++
		if k == 0 {
			count := 1
			for i < n && cLen[i] == 0 {
				i++
				count++
			}
			if count <= 2 {
				tFreq[0] += uint32(count)
			} else if count <= 18 {
				tFreq[1]++
			} else if count == 19 {
				tFreq[0]++
				tFreq[1]++
			} else {
				tFreq[2]++
			}
			continue
		}
		tFreq[k+2]++
	}
	return tFreq
}

func method123PClass(p int) int {
	qc := 0
	for p > 0 {
		p >>= 1
		qc++
	}
	return qc
}

func (t method123Token) isMatch() bool {
	return t.length != 0
}

func (t method123Token) cSymbol() int {
	if !t.isMatch() {
		return int(t.literal)
	}
	return int(t.length) + (0xFF + 1 - methodThresh)
}

func method123TokenizeGreedy(plain []byte) []method123Token {
	out := make([]method123Token, 0, minInt(len(plain), method123BlockMaxTokens))
	method123TokenizeGreedyBlocks(plain, func(block []method123Token) {
		out = append(out, block...)
	})
	return out
}

func method123TokenizeGreedyBlocks(plain []byte, emit func([]method123Token)) {
	n := len(plain)
	if n == 0 {
		return
	}
	if n > int(method123MaxNarrowPos) {
		method123TokenizeGreedyBlocksWide(plain, emit)
		return
	}

	head := method123AcquireHead32()
	defer method123ReleaseHead32(head)
	prev := method123AcquirePrev32(n)
	defer method123ReleasePrev32(prev)

	tokens := method123AcquireBlockTokens()
	defer method123ReleaseBlockTokens(tokens)
	for i := 0; i < n; {
		bestLen := 0
		bestDist := 0

		if i+methodThresh <= n {
			h := method123Hash3(plain[i], plain[i+1], plain[i+2])
			for cand, iter := head[h], 0; cand != method123NoPos && i-int(cand) <= methodDICSize && iter < method123HashChainLimit; cand, iter = prev[cand], iter+1 {
				candPos := int(cand)
				if plain[candPos] != plain[i] || plain[candPos+1] != plain[i+1] || plain[candPos+2] != plain[i+2] {
					continue
				}

				maxLen := methodMaxM
				if maxLen > n-i {
					maxLen = n - i
				}
				ml := methodThresh
				for ml < maxLen && plain[candPos+ml] == plain[i+ml] {
					ml++
				}
				if ml > bestLen {
					bestLen = ml
					bestDist = i - candPos
					if ml == methodMaxM {
						break
					}
				}
			}
		}

		consume := 1
		if bestLen >= methodThresh {
			tokens = append(tokens, method123Token{
				length: uint16(bestLen),
				dist:   uint16(bestDist),
			})
			consume = bestLen
		} else {
			tokens = append(tokens, method123Token{literal: plain[i]})
		}
		if len(tokens) == method123BlockMaxTokens {
			emit(tokens)
			tokens = tokens[:0]
		}

		for pos := i; pos < i+consume; pos++ {
			if pos+methodThresh <= n {
				h := method123Hash3(plain[pos], plain[pos+1], plain[pos+2])
				prev[pos] = head[h]
				head[h] = int32(pos)
			}
		}
		i += consume
	}
	if len(tokens) > 0 {
		emit(tokens)
	}
}

func method123TokenizeGreedyWide(plain []byte) []method123Token {
	out := make([]method123Token, 0, minInt(len(plain), method123BlockMaxTokens))
	method123TokenizeGreedyBlocksWide(plain, func(block []method123Token) {
		out = append(out, block...)
	})
	return out
}

func method123TokenizeGreedyBlocksWide(plain []byte, emit func([]method123Token)) {
	n := len(plain)
	head := method123AcquireHeadWide()
	defer method123ReleaseHeadWide(head)
	prev := method123AcquirePrevWide(n)
	defer method123ReleasePrevWide(prev)

	tokens := method123AcquireBlockTokens()
	defer method123ReleaseBlockTokens(tokens)
	for i := 0; i < n; {
		bestLen := 0
		bestDist := 0

		if i+methodThresh <= n {
			h := method123Hash3(plain[i], plain[i+1], plain[i+2])
			for cand, iter := head[h], 0; cand >= 0 && i-cand <= methodDICSize && iter < method123HashChainLimit; cand, iter = prev[cand], iter+1 {
				if plain[cand] != plain[i] || plain[cand+1] != plain[i+1] || plain[cand+2] != plain[i+2] {
					continue
				}

				maxLen := methodMaxM
				if maxLen > n-i {
					maxLen = n - i
				}
				ml := methodThresh
				for ml < maxLen && plain[cand+ml] == plain[i+ml] {
					ml++
				}
				if ml > bestLen {
					bestLen = ml
					bestDist = i - cand
					if ml == methodMaxM {
						break
					}
				}
			}
		}

		consume := 1
		if bestLen >= methodThresh {
			tokens = append(tokens, method123Token{
				length: uint16(bestLen),
				dist:   uint16(bestDist),
			})
			consume = bestLen
		} else {
			tokens = append(tokens, method123Token{literal: plain[i]})
		}
		if len(tokens) == method123BlockMaxTokens {
			emit(tokens)
			tokens = tokens[:0]
		}

		for pos := i; pos < i+consume; pos++ {
			if pos+methodThresh <= n {
				h := method123Hash3(plain[pos], plain[pos+1], plain[pos+2])
				prev[pos] = head[h]
				head[h] = pos
			}
		}
		i += consume
	}
	if len(tokens) > 0 {
		emit(tokens)
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func method123Hash3(a, b, c byte) int {
	v := uint32(a)<<16 | uint32(b)<<8 | uint32(c)
	v ^= v >> 11
	v *= 0x9E3779B1
	return int((v >> (32 - method123HashBits)) & method123HashMask)
}
