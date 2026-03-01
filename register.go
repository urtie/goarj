package arj

import (
	"errors"
	"io"
	"sync"
)

// A Compressor returns a new compressing writer writing to w.
type Compressor func(w io.Writer) (io.WriteCloser, error)

// A Decompressor returns a new decompressing reader reading from r.
type Decompressor func(r io.Reader) io.ReadCloser

var (
	compressors   sync.Map // map[uint16]Compressor
	decompressors sync.Map // map[uint16]Decompressor
)

var (
	// ErrNilCompressor indicates a nil package-level Compressor registration.
	ErrNilCompressor = errors.New("arj: nil compressor")
	// ErrNilDecompressor indicates a nil package-level Decompressor registration.
	ErrNilDecompressor = errors.New("arj: nil decompressor")
)

func init() {
	// Built-in defaults align with classic ARJ methods 0..4.
	compressors.Store(Store, Compressor(func(w io.Writer) (io.WriteCloser, error) {
		return &nopCloser{w}, nil
	}))
	decompressors.Store(Store, Decompressor(io.NopCloser))

	for _, method := range []uint16{Method1, Method2, Method3, Method4} {
		compressors.Store(method, compressorMethod14(method))
		decompressors.Store(method, decompressorMethod14(method))
	}
}

// RegisterDecompressor installs a package-level decompressor for method.
// It panics for nil or duplicate registrations.
func RegisterDecompressor(method uint16, dcomp Decompressor) {
	if dcomp == nil {
		panic(ErrNilDecompressor)
	}
	if _, dup := decompressors.LoadOrStore(method, dcomp); dup {
		panic("decompressor already registered")
	}
}

// RegisterCompressor installs a package-level compressor for method.
// It panics for nil or duplicate registrations.
func RegisterCompressor(method uint16, comp Compressor) {
	if comp == nil {
		panic(ErrNilCompressor)
	}
	if _, dup := compressors.LoadOrStore(method, comp); dup {
		panic("compressor already registered")
	}
}

func compressor(method uint16) Compressor {
	ci, ok := compressors.Load(method)
	if !ok {
		return nil
	}
	return ci.(Compressor)
}

func decompressor(method uint16) Decompressor {
	di, ok := decompressors.Load(method)
	if !ok {
		return nil
	}
	return di.(Decompressor)
}

type nopCloser struct {
	io.Writer
}

func (w *nopCloser) Close() error {
	return nil
}
