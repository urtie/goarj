package arj

import (
	"bytes"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var (
	benchmarkMethods = []struct {
		name   string
		method uint16
	}{
		{name: "store", method: Store},
		{name: "method1", method: Method1},
		{name: "method2", method: Method2},
		{name: "method3", method: Method3},
		{name: "method4", method: Method4},
	}
	benchmarkSizes = []struct {
		name string
		size int
	}{
		{name: "small_4KiB", size: 4 << 10},
		{name: "medium_64KiB", size: 64 << 10},
		{name: "large_1MiB", size: 1 << 20},
		{name: "xlarge_8MiB", size: 8 << 20},
	}
	benchmarkModifiedTime = time.Unix(1700000000, 0).UTC()
)

func BenchmarkReaderDecodeThroughput(b *testing.B) {
	for _, sz := range benchmarkSizes {
		sz := sz
		payload := benchmarkPayload(sz.size)

		for _, method := range benchmarkMethods {
			method := method
			b.Run(sz.name+"/"+method.name, func(b *testing.B) {
				f := benchmarkReaderFixture(b, method.method, payload)
				wantN := int64(len(payload))

				b.ReportAllocs()
				b.SetBytes(wantN)
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					rc, err := f.Open()
					if err != nil {
						b.Fatalf("Open method %d: %v", method.method, err)
					}
					n, err := io.Copy(io.Discard, rc)
					if err != nil {
						_ = rc.Close()
						b.Fatalf("decode method %d: %v", method.method, err)
					}
					if err := rc.Close(); err != nil {
						b.Fatalf("Close method %d: %v", method.method, err)
					}
					if n != wantN {
						b.Fatalf("decoded bytes method %d = %d, want %d", method.method, n, wantN)
					}
				}
			})
		}
	}
}

func BenchmarkWriterEncodeThroughput(b *testing.B) {
	for _, sz := range benchmarkSizes {
		sz := sz
		payload := benchmarkPayload(sz.size)

		for _, method := range benchmarkMethods {
			method := method
			b.Run(sz.name+"/"+method.name, func(b *testing.B) {
				var archive bytes.Buffer
				header := FileHeader{
					Name:     "payload.bin",
					Method:   method.method,
					Modified: benchmarkModifiedTime,
				}

				b.ReportAllocs()
				b.SetBytes(int64(len(payload)))
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					archive.Reset()

					w := NewWriter(&archive)
					fw, err := w.CreateHeader(&header)
					if err != nil {
						b.Fatalf("CreateHeader method %d: %v", method.method, err)
					}
					if _, err := fw.Write(payload); err != nil {
						b.Fatalf("Write method %d: %v", method.method, err)
					}
					if err := w.Close(); err != nil {
						b.Fatalf("Close method %d: %v", method.method, err)
					}
				}
			})
		}
	}
}

func BenchmarkWriterEncodeStoreSpillBuffer(b *testing.B) {
	payload := benchmarkPayload(int(maxInMemoryEntrySpoolSize) + (1 << 20))
	var archive bytes.Buffer
	header := FileHeader{
		Name:     "spill.bin",
		Method:   Store,
		Modified: benchmarkModifiedTime,
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		archive.Reset()

		w := NewWriter(&archive)
		w.SetBufferLimits(WriteBufferLimits{
			MaxEntryBufferSize:         uint64(len(payload)) + (1 << 20),
			MaxMethod14InputBufferSize: DefaultMaxMethod14InputBufferSize,
		})
		fw, err := w.CreateHeader(&header)
		if err != nil {
			b.Fatalf("CreateHeader: %v", err)
		}
		if _, err := fw.Write(payload); err != nil {
			b.Fatalf("Write: %v", err)
		}
		if err := w.Close(); err != nil {
			b.Fatalf("Close: %v", err)
		}
	}
}

func benchmarkReaderFixture(b *testing.B, method uint16, payload []byte) *File {
	b.Helper()

	archive := benchmarkArchiveBytes(b, method, payload)
	r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		b.Fatalf("NewReader method %d: %v", method, err)
	}
	if got, want := len(r.File), 1; got != want {
		b.Fatalf("file count method %d = %d, want %d", method, got, want)
	}
	if got := r.File[0].Method; got != method {
		b.Fatalf("header method = %d, want %d", got, method)
	}
	return r.File[0]
}

func benchmarkArchiveBytes(b *testing.B, method uint16, payload []byte) []byte {
	b.Helper()

	var archive bytes.Buffer
	w := NewWriter(&archive)
	fw, err := w.CreateHeader(&FileHeader{
		Name:     "payload.bin",
		Method:   method,
		Modified: benchmarkModifiedTime,
	})
	if err != nil {
		b.Fatalf("CreateHeader method %d: %v", method, err)
	}
	if _, err := fw.Write(payload); err != nil {
		b.Fatalf("Write method %d: %v", method, err)
	}
	if err := w.Close(); err != nil {
		b.Fatalf("Close method %d: %v", method, err)
	}
	return append([]byte(nil), archive.Bytes()...)
}

func benchmarkPayload(size int) []byte {
	payload := make([]byte, size)
	const pattern = "goarj-native-benchmark-pattern-0123456789"

	var x uint32 = 1
	for i := 0; i < len(payload); i++ {
		if i%64 < len(pattern) {
			payload[i] = pattern[i%len(pattern)]
			continue
		}
		x = x*1664525 + 1013904223
		payload[i] = byte(x >> 24)
	}
	return payload
}

func BenchmarkFindMainHeaderOffsetPrefixedScan(b *testing.B) {
	real := benchmarkArchiveBytes(b, Store, benchmarkPayload(4<<10))
	decoy := benchmarkDecoyMainHeaderArchive(b, "decoy-empty.arj", true)

	cases := []struct {
		name   string
		prefix []byte
	}{
		{name: "sparse_256KiB", prefix: bytes.Repeat([]byte{0x7f}, 256<<10)},
		{name: "dense_256KiB", prefix: benchmarkSignatureDenseNoise(256 << 10)},
		{name: "dense_2MiB", prefix: benchmarkSignatureDenseNoise(2 << 20)},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			container := make([]byte, 0, len(tc.prefix)+len(decoy)+len(real))
			container = append(container, tc.prefix...)
			container = append(container, decoy...)
			container = append(container, real...)
			size := int64(len(container))
			want := int64(len(tc.prefix) + len(decoy))

			b.ReportAllocs()
			b.SetBytes(size)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				got, err := findMainHeaderOffset(bytes.NewReader(container), size)
				if err != nil {
					b.Fatalf("findMainHeaderOffset: %v", err)
				}
				if got != want {
					b.Fatalf("findMainHeaderOffset = %d, want %d", got, want)
				}
			}
		})
	}
}

func BenchmarkReaderInitPrefixedScan(b *testing.B) {
	real := benchmarkArchiveBytes(b, Store, benchmarkPayload(4<<10))
	decoy := benchmarkDecoyMainHeaderArchive(b, "decoy-empty.arj", true)

	cases := []struct {
		name   string
		prefix []byte
	}{
		{name: "sparse_256KiB", prefix: bytes.Repeat([]byte{0x7f}, 256<<10)},
		{name: "dense_256KiB", prefix: benchmarkSignatureDenseNoise(256 << 10)},
		{name: "dense_2MiB", prefix: benchmarkSignatureDenseNoise(2 << 20)},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			container := make([]byte, 0, len(tc.prefix)+len(decoy)+len(real))
			container = append(container, tc.prefix...)
			container = append(container, decoy...)
			container = append(container, real...)
			size := int64(len(container))
			wantBaseOffset := int64(len(tc.prefix) + len(decoy))

			b.ReportAllocs()
			b.SetBytes(size)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				r, err := NewReader(bytes.NewReader(container), size)
				if err != nil {
					b.Fatalf("NewReader: %v", err)
				}
				if got := r.BaseOffset(); got != wantBaseOffset {
					b.Fatalf("BaseOffset = %d, want %d", got, wantBaseOffset)
				}
				if got, want := len(r.File), 1; got != want {
					b.Fatalf("file count = %d, want %d", got, want)
				}
			}
		})
	}
}

func BenchmarkOpenMultiReaderContinuation(b *testing.B) {
	const (
		baseName   = "bench-split"
		targetName = "split.bin"
	)

	payload := benchmarkPayload(1 << 20)
	splitAt := len(payload) / 2
	firstPart := append([]byte(nil), payload[:splitAt]...)
	secondPart := append([]byte(nil), payload[splitAt:]...)

	tmp := b.TempDir()
	base := filepath.Join(tmp, baseName)

	benchmarkWriteVolumeArchive(b, base+".arj", []benchmarkVolumeEntry{
		{name: targetName, flags: FlagVolume, payload: firstPart},
	})
	benchmarkWriteVolumeArchive(b, base+".a01", []benchmarkVolumeEntry{
		{name: targetName, flags: FlagExtFile, payload: secondPart},
	})

	wantSize := int64(len(payload))
	b.ReportAllocs()
	b.SetBytes(wantSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		multi, err := OpenMultiReader(base + ".arj")
		if err != nil {
			b.Fatalf("OpenMultiReader: %v", err)
		}

		if got, want := len(multi.File), 1; got != want {
			_ = multi.Close()
			b.Fatalf("file count = %d, want %d", got, want)
		}
		if gotType := multi.File[0].Mode().Type(); gotType != 0 {
			_ = multi.Close()
			b.Fatalf("file mode type = %v, want %v", gotType, fs.FileMode(0))
		}

		rc, err := multi.File[0].Open()
		if err != nil {
			_ = multi.Close()
			b.Fatalf("Open split file: %v", err)
		}
		n, err := io.Copy(io.Discard, rc)
		closeErr := rc.Close()
		multiCloseErr := multi.Close()
		if err != nil {
			b.Fatalf("io.Copy split file: %v", err)
		}
		if closeErr != nil {
			b.Fatalf("Close split file: %v", closeErr)
		}
		if multiCloseErr != nil {
			b.Fatalf("Close multi reader: %v", multiCloseErr)
		}
		if n != wantSize {
			b.Fatalf("copied bytes = %d, want %d", n, wantSize)
		}
	}
}

func BenchmarkConcatenatedReaderAtVolumeIndex(b *testing.B) {
	const (
		volumeCount = 4096
		volumeSize  = 128
	)

	readers := make([]io.ReaderAt, volumeCount)
	sizes := make([]int64, volumeCount)
	for i := 0; i < volumeCount; i++ {
		readers[i] = bytes.NewReader(make([]byte, volumeSize))
		sizes[i] = volumeSize
	}

	cr, err := newConcatenatedReaderAt(readers, sizes)
	if err != nil {
		b.Fatalf("newConcatenatedReaderAt: %v", err)
	}

	offsets := make([]int64, 2048)
	for i := range offsets {
		offsets[i] = int64((i * 7919) % (volumeCount * volumeSize))
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		off := offsets[i%len(offsets)]
		got := cr.volumeIndex(off)
		want := int(off / volumeSize)
		if got != want {
			b.Fatalf("volumeIndex(%d) = %d, want %d", off, got, want)
		}
	}
}

func BenchmarkMultiVolumeWriterMaxCompressedChunk(b *testing.B) {
	payload := benchmarkPayload(256 << 10)
	cases := []struct {
		name    string
		maxComp int64
	}{
		{name: "tight_64KiB", maxComp: 64 << 10},
		{name: "medium_128KiB", maxComp: 128 << 10},
		{name: "loose_220KiB", maxComp: 220 << 10},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			w := &MultiVolumeWriter{}

			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				n, comp, err := w.maxCompressedChunk(Method1, payload, tc.maxComp)
				if err != nil {
					b.Fatalf("maxCompressedChunk: %v", err)
				}
				if n < 0 || n > len(payload) {
					b.Fatalf("chunk size = %d, want [0,%d]", n, len(payload))
				}
				if n > 0 && int64(len(comp)) > tc.maxComp {
					b.Fatalf("compressed len = %d, want <= %d", len(comp), tc.maxComp)
				}
			}
		})
	}
}

func benchmarkDecoyMainHeaderArchive(b *testing.B, name string, withTerminator bool) []byte {
	b.Helper()

	basic := make([]byte, arjMinFirstHeaderSize)
	basic[0] = arjMinFirstHeaderSize
	basic[1] = arjVersionCurrent
	basic[2] = arjVersionNeeded
	basic[3] = currentHostOS()
	basic[6] = arjFileTypeMain

	full := make([]byte, 0, len(basic)+len(name)+2)
	full = append(full, basic...)
	full = append(full, name...)
	full = append(full, 0)
	full = append(full, 0)

	var buf bytes.Buffer
	if err := writeHeaderBlock(&buf, full); err != nil {
		b.Fatalf("writeHeaderBlock(decoy): %v", err)
	}
	if withTerminator {
		if _, err := buf.Write([]byte{arjHeaderID1, arjHeaderID2, 0, 0}); err != nil {
			b.Fatalf("write decoy terminator: %v", err)
		}
	} else {
		if _, err := buf.Write([]byte("NOPE")); err != nil {
			b.Fatalf("write malformed decoy trailer: %v", err)
		}
	}
	return buf.Bytes()
}

func benchmarkSignatureDenseNoise(size int) []byte {
	if size < 0 {
		size = 0
	}
	out := make([]byte, size)
	for i := 0; i+3 < len(out); i += 4 {
		out[i] = arjHeaderID1
		out[i+1] = arjHeaderID2
		out[i+2] = 0xff
		out[i+3] = 0xff
	}
	if len(out)%4 == 1 {
		out[len(out)-1] = arjHeaderID1
	}
	return out
}

type benchmarkVolumeEntry struct {
	name    string
	flags   uint8
	payload []byte
}

func benchmarkWriteVolumeArchive(b *testing.B, path string, entries []benchmarkVolumeEntry) {
	b.Helper()

	var buf bytes.Buffer
	w := NewWriter(&buf)
	for _, entry := range entries {
		h := &FileHeader{
			Name:   entry.name,
			Method: Store,
			Flags:  entry.flags,
		}
		fw, err := w.CreateHeader(h)
		if err != nil {
			b.Fatalf("CreateHeader(%s): %v", entry.name, err)
		}
		if _, err := fw.Write(entry.payload); err != nil {
			b.Fatalf("Write(%s): %v", entry.name, err)
		}
	}
	if err := w.Close(); err != nil {
		b.Fatalf("Close writer for %s: %v", path, err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		b.Fatalf("WriteFile(%s): %v", path, err)
	}
}
