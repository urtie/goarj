package arj

import "sync"

const (
	method4MinMatch = methodThresh
	method4MaxMatch = methodMaxM

	// decodeMethod4Ptr can represent pointer values in [0, 15871], so
	// encode-side match distance is capped at 15872 bytes.
	method4MaxPtr  = 15871
	method4MaxDist = method4MaxPtr + 1

	method4HashBits      = 15
	method4HashSize      = 1 << method4HashBits
	method4HashMask      = method4HashSize - 1
	method4MaxChain      = 96
	method4NoPos         = int32(-1)
	method4MaxNarrowPos  = int32(^uint32(0) >> 1)
	method4MaxPooledPrev = 1 << 20
)

var (
	method4Head32Pool = sync.Pool{
		New: func() any {
			return make([]int32, method4HashSize)
		},
	}
	method4HeadWidePool = sync.Pool{
		New: func() any {
			return make([]int, method4HashSize)
		},
	}
	method4Prev32Pool   sync.Pool
	method4PrevWidePool sync.Pool
)

func method4AcquireHead32() []int32 {
	head := method4Head32Pool.Get().([]int32)
	for i := range head {
		head[i] = method4NoPos
	}
	return head
}

func method4ReleaseHead32(head []int32) {
	if cap(head) < method4HashSize {
		return
	}
	method4Head32Pool.Put(head[:method4HashSize])
}

func method4AcquireHeadWide() []int {
	head := method4HeadWidePool.Get().([]int)
	for i := range head {
		head[i] = -1
	}
	return head
}

func method4ReleaseHeadWide(head []int) {
	if cap(head) < method4HashSize {
		return
	}
	method4HeadWidePool.Put(head[:method4HashSize])
}

func method4AcquirePrev32(n int) []int32 {
	if n <= method4MaxPooledPrev {
		if v := method4Prev32Pool.Get(); v != nil {
			prev := v.([]int32)
			if cap(prev) >= n {
				prev = prev[:n]
				for i := range prev {
					prev[i] = method4NoPos
				}
				return prev
			}
		}
	}
	prev := make([]int32, n)
	for i := range prev {
		prev[i] = method4NoPos
	}
	return prev
}

func method4ReleasePrev32(prev []int32) {
	if cap(prev) == 0 || cap(prev) > method4MaxPooledPrev {
		return
	}
	method4Prev32Pool.Put(prev[:cap(prev)])
}

func method4AcquirePrevWide(n int) []int {
	if n <= method4MaxPooledPrev {
		if v := method4PrevWidePool.Get(); v != nil {
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

func method4ReleasePrevWide(prev []int) {
	if cap(prev) == 0 || cap(prev) > method4MaxPooledPrev {
		return
	}
	method4PrevWidePool.Put(prev[:cap(prev)])
}

func encodeMethod4Native(plain []byte) []byte {
	var bw arjBitWriter
	encodeMethod4NativeToBitWriter(&bw, plain)
	return bw.finishWithShutdownPadding()
}

func encodeMethod4NativeToBitWriter(bw *arjBitWriter, plain []byte) {
	if len(plain) == 0 {
		return
	}
	if len(plain) > int(method4MaxNarrowPos) {
		encodeMethod4NativeWideToBitWriter(bw, plain)
		return
	}

	head := method4AcquireHead32()
	defer method4ReleaseHead32(head)
	prev := method4AcquirePrev32(len(plain))
	defer method4ReleasePrev32(prev)

	for i := 0; i < len(plain); {
		bestLen, bestDist := method4FindBestMatch(plain, head, prev, i)
		if bestLen >= method4MinMatch {
			enc4Pass1(bw, bestLen-(methodThresh-1))
			enc4Pass2(bw, bestDist-1)

			for j := 0; j < bestLen; j++ {
				method4InsertHash(plain, head, prev, i+j)
			}
			i += bestLen
			continue
		}

		bw.putBits(1, 0)
		bw.putBits(8, uint16(plain[i]))
		method4InsertHash(plain, head, prev, i)
		i++
	}
}

func encodeMethod4NativeWide(plain []byte) []byte {
	var bw arjBitWriter
	encodeMethod4NativeWideToBitWriter(&bw, plain)
	return bw.finishWithShutdownPadding()
}

func encodeMethod4NativeWideToBitWriter(bw *arjBitWriter, plain []byte) {
	head := method4AcquireHeadWide()
	defer method4ReleaseHeadWide(head)
	prev := method4AcquirePrevWide(len(plain))
	defer method4ReleasePrevWide(prev)

	for i := 0; i < len(plain); {
		bestLen, bestDist := method4FindBestMatchWide(plain, head, prev, i)
		if bestLen >= method4MinMatch {
			enc4Pass1(bw, bestLen-(methodThresh-1))
			enc4Pass2(bw, bestDist-1)

			for j := 0; j < bestLen; j++ {
				method4InsertHashWide(plain, head, prev, i+j)
			}
			i += bestLen
			continue
		}

		bw.putBits(1, 0)
		bw.putBits(8, uint16(plain[i]))
		method4InsertHashWide(plain, head, prev, i)
		i++
	}
}

func method4FindBestMatch(plain []byte, head, prev []int32, pos int) (bestLen int, bestDist int) {
	if pos+method4MinMatch > len(plain) {
		return 0, 0
	}

	h := method4Hash3(plain[pos], plain[pos+1], plain[pos+2])
	for cand, depth := head[h], 0; cand != method4NoPos && depth < method4MaxChain; cand, depth = prev[cand], depth+1 {
		candPos := int(cand)
		dist := pos - candPos
		if dist > method4MaxDist {
			break
		}
		if dist <= 0 {
			continue
		}

		matchLen := method4MatchLen(plain, candPos, pos, method4MaxMatch)
		if matchLen < method4MinMatch {
			continue
		}
		if matchLen > bestLen || (matchLen == bestLen && dist < bestDist) {
			bestLen = matchLen
			bestDist = dist
			if bestLen == method4MaxMatch {
				break
			}
		}
	}
	return bestLen, bestDist
}

func method4FindBestMatchWide(plain []byte, head, prev []int, pos int) (bestLen int, bestDist int) {
	if pos+method4MinMatch > len(plain) {
		return 0, 0
	}

	h := method4Hash3(plain[pos], plain[pos+1], plain[pos+2])
	for cand, depth := head[h], 0; cand >= 0 && depth < method4MaxChain; cand, depth = prev[cand], depth+1 {
		dist := pos - cand
		if dist > method4MaxDist {
			break
		}
		if dist <= 0 {
			continue
		}

		matchLen := method4MatchLen(plain, cand, pos, method4MaxMatch)
		if matchLen < method4MinMatch {
			continue
		}
		if matchLen > bestLen || (matchLen == bestLen && dist < bestDist) {
			bestLen = matchLen
			bestDist = dist
			if bestLen == method4MaxMatch {
				break
			}
		}
	}
	return bestLen, bestDist
}

func method4InsertHash(plain []byte, head, prev []int32, pos int) {
	if pos+method4MinMatch > len(plain) {
		return
	}
	h := method4Hash3(plain[pos], plain[pos+1], plain[pos+2])
	prev[pos] = head[h]
	head[h] = int32(pos)
}

func method4InsertHashWide(plain []byte, head, prev []int, pos int) {
	if pos+method4MinMatch > len(plain) {
		return
	}
	h := method4Hash3(plain[pos], plain[pos+1], plain[pos+2])
	prev[pos] = head[h]
	head[h] = pos
}

func method4Hash3(a, b, c byte) int {
	h := (uint32(a) << 10) ^ (uint32(b) << 5) ^ uint32(c)
	return int(h & method4HashMask)
}

func method4MatchLen(plain []byte, left, right, maxLen int) int {
	limit := len(plain) - right
	if limit > maxLen {
		limit = maxLen
	}
	n := 0
	for n < limit && plain[left+n] == plain[right+n] {
		n++
	}
	return n
}

// enc4Pass1 uses the same variable-width length coding as ARJ's enc4_pass1.
func enc4Pass1(bw *arjBitWriter, n int) {
	step := 1
	width := 0
	for n >= step {
		n -= step
		width++
		step <<= 1
	}
	if width != 0 {
		bw.putBits(width, ^uint16(0))
	}
	if width < 7 {
		width++
	}
	bw.putBits(width, uint16(n))
}

// enc4Pass2 uses the same variable-width pointer coding as ARJ's enc4_pass2.
func enc4Pass2(bw *arjBitWriter, n int) {
	step := 1 << 9
	width := 9
	for n >= step {
		n -= step
		width++
		step <<= 1
	}
	if width != 9 {
		bw.putBits(width-9, ^uint16(0))
	}
	if width < 13 {
		width++
	}
	bw.putBits(width, uint16(n))
}
