package arj

import "testing"

func TestWideMatchLenMatchesNaive(t *testing.T) {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte((i*37 + 11) & 0xff)
	}
	// Force long equal runs and mismatches across word boundaries.
	copy(data[512:1024], data[128:640])
	copy(data[2048:3072], data[1024:2048])
	data[700] ^= 0x80
	data[2500] ^= 0x01

	naive := func(left, right, maxLen int) int {
		limit := len(data) - right
		if limit > maxLen {
			limit = maxLen
		}
		if left+limit > len(data) {
			limit = len(data) - left
		}
		if limit < 0 {
			return 0
		}
		n := 0
		for n < limit && data[left+n] == data[right+n] {
			n++
		}
		return n
	}

	for left := 0; left < len(data); left += 97 {
		for right := left + 1; right < len(data); right += 113 {
			for _, maxLen := range []int{1, 2, 3, 7, 8, 9, 31, 64, 127, 255} {
				got := wideMatchLen(data, left, right, maxLen)
				want := naive(left, right, maxLen)
				if got != want {
					t.Fatalf("wideMatchLen(%d,%d,%d) = %d, want %d", left, right, maxLen, got, want)
				}
			}
		}
	}
}
