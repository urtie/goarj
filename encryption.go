package arj

import (
	"io"
)

func unsupportedEncryptionError(flags, version uint8, password []byte) error {
	if flags&FlagGarbled == 0 {
		return nil
	}
	switch version {
	case EncryptOld, EncryptStd:
		if len(password) == 0 {
			return ErrPasswordRequired
		}
		return nil
	default:
		return &UnsupportedModeError{
			Kind:              UnsupportedModeKindEncrypted,
			Flags:             flags,
			EncryptionVersion: version,
		}
	}
}

func newGarbledReader(r io.Reader, password []byte, modifier uint8) *garbledReader {
	return &garbledReader{
		r:        r,
		password: append([]byte(nil), password...),
		modifier: modifier,
	}
}

type garbledReader struct {
	r        io.Reader
	password []byte
	modifier uint8
	idx      int
}

func (r *garbledReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n > 0 {
		r.idx = applyGarbledXORInPlace(p[:n], r.password, r.modifier, r.idx)
	}
	if err != nil {
		r.clearSensitiveData()
	}
	return n, err
}

func (r *garbledReader) clearSensitiveData() {
	clearBytes(r.password)
	r.password = nil
	r.modifier = 0
	r.idx = 0
}

func applyGarbledXORInPlace(data, password []byte, modifier uint8, startIdx int) int {
	if len(password) == 0 {
		return startIdx
	}
	idx := startIdx
	for i := range data {
		data[i] ^= modifier + password[idx]
		idx++
		if idx == len(password) {
			idx = 0
		}
	}
	return idx
}
