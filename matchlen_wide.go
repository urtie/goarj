package arj

import (
	"encoding/binary"
	"math/bits"
)

func wideMatchLen(plain []byte, left, right, maxLen int) int {
	if maxLen <= 0 || left < 0 || right < 0 || left >= len(plain) || right >= len(plain) {
		return 0
	}

	limit := len(plain) - right
	if limit > maxLen {
		limit = maxLen
	}
	if left+limit > len(plain) {
		limit = len(plain) - left
	}
	if limit <= 0 {
		return 0
	}

	n := 0
	for n+8 <= limit {
		lv := binary.LittleEndian.Uint64(plain[left+n:])
		rv := binary.LittleEndian.Uint64(plain[right+n:])
		if lv != rv {
			return n + bits.TrailingZeros64(lv^rv)/8
		}
		n += 8
	}

	for n < limit && plain[left+n] == plain[right+n] {
		n++
	}
	return n
}
