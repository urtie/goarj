package arj

import (
	"bytes"
	"fmt"
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

func BenchmarkWriterEncodeThroughputSeekable(b *testing.B) {
	for _, sz := range benchmarkSizes {
		sz := sz
		payload := benchmarkPayload(sz.size)

		for _, method := range benchmarkMethods {
			method := method
			b.Run(sz.name+"/"+method.name, func(b *testing.B) {
				var archive benchmarkWriteSeeker
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

type benchmarkWriteSeeker struct {
	buf []byte
	off int64
}

func (w *benchmarkWriteSeeker) Reset() {
	w.buf = w.buf[:0]
	w.off = 0
}

func (w *benchmarkWriteSeeker) Write(p []byte) (int, error) {
	if w.off < 0 {
		return 0, fmt.Errorf("negative offset")
	}
	end := w.off + int64(len(p))
	if end < w.off {
		return 0, fmt.Errorf("offset overflow")
	}
	if end > int64(len(w.buf)) {
		if end > int64(cap(w.buf)) {
			next := make([]byte, end, growBenchmarkWriteSeekerCap(cap(w.buf), int(end)))
			copy(next, w.buf)
			w.buf = next
		} else {
			w.buf = w.buf[:end]
		}
	}
	copy(w.buf[w.off:end], p)
	w.off = end
	return len(p), nil
}

func (w *benchmarkWriteSeeker) Seek(offset int64, whence int) (int64, error) {
	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = w.off + offset
	case io.SeekEnd:
		next = int64(len(w.buf)) + offset
	default:
		return 0, fmt.Errorf("invalid whence %d", whence)
	}
	if next < 0 {
		return 0, fmt.Errorf("negative offset")
	}
	w.off = next
	return next, nil
}

func growBenchmarkWriteSeekerCap(oldCap, want int) int {
	next := oldCap
	if next == 0 {
		next = 1024
	}
	for next < want {
		next *= 2
	}
	return next
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

type benchmarkCorpusFile struct {
	name    string
	method  uint16
	payload []byte
}

func benchmarkArchiveFromFiles(b *testing.B, files []benchmarkCorpusFile) []byte {
	b.Helper()

	var archive bytes.Buffer
	w := NewWriter(&archive)
	for _, file := range files {
		fw, err := w.CreateHeader(&FileHeader{
			Name:     file.name,
			Method:   file.method,
			Modified: benchmarkModifiedTime,
		})
		if err != nil {
			b.Fatalf("CreateHeader(%s): %v", file.name, err)
		}
		if _, err := fw.Write(file.payload); err != nil {
			b.Fatalf("Write(%s): %v", file.name, err)
		}
	}
	if err := w.Close(); err != nil {
		b.Fatalf("Close writer: %v", err)
	}
	return append([]byte(nil), archive.Bytes()...)
}

func benchmarkHighEntropyPayload(size int) []byte {
	out := make([]byte, size)
	var x uint64 = 0x9e3779b97f4a7c15
	for i := range out {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		out[i] = byte(x)
	}
	return out
}

func benchmarkReaderDecodeCorpus(b *testing.B, files []benchmarkCorpusFile) {
	b.Helper()
	archive := benchmarkArchiveFromFiles(b, files)
	var total int64
	for _, file := range files {
		total += int64(len(file.payload))
	}

	b.ReportAllocs()
	b.SetBytes(total)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r, err := NewReader(bytes.NewReader(archive), int64(len(archive)))
		if err != nil {
			b.Fatalf("NewReader: %v", err)
		}
		var copied int64
		for _, f := range r.File {
			rc, err := f.Open()
			if err != nil {
				b.Fatalf("Open(%s): %v", f.Name, err)
			}
			n, err := io.Copy(io.Discard, rc)
			closeErr := rc.Close()
			if err != nil {
				b.Fatalf("Read(%s): %v", f.Name, err)
			}
			if closeErr != nil {
				b.Fatalf("Close(%s): %v", f.Name, closeErr)
			}
			copied += n
		}
		if copied != total {
			b.Fatalf("decoded bytes = %d, want %d", copied, total)
		}
	}
}

func BenchmarkReaderDecodeCorpus(b *testing.B) {
	const oneMiB = 1 << 20
	cases := []struct {
		name  string
		files []benchmarkCorpusFile
	}{
		{
			name: "entropy_1MiB",
			files: []benchmarkCorpusFile{
				{name: "entropy.bin", method: Method1, payload: benchmarkHighEntropyPayload(oneMiB)},
			},
		},
		{
			name: "repetitive_1MiB",
			files: []benchmarkCorpusFile{
				{name: "repeat.bin", method: Method1, payload: bytes.Repeat([]byte("goarj-repeat-pattern-"), oneMiB/len("goarj-repeat-pattern-"))},
			},
		},
		{
			name: "many_small_1000x1KiB",
			files: func() []benchmarkCorpusFile {
				files := make([]benchmarkCorpusFile, 1000)
				for i := range files {
					files[i] = benchmarkCorpusFile{
						name:    fmt.Sprintf("tiny/%04d.bin", i),
						method:  Method1,
						payload: benchmarkPayload(1 << 10),
					}
				}
				return files
			}(),
		},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			benchmarkReaderDecodeCorpus(b, tc.files)
		})
	}
}

func BenchmarkWriterEncodeCorpus(b *testing.B) {
	const oneMiB = 1 << 20
	cases := []struct {
		name  string
		files []benchmarkCorpusFile
	}{
		{
			name: "entropy_1MiB",
			files: []benchmarkCorpusFile{
				{name: "entropy.bin", method: Method1, payload: benchmarkHighEntropyPayload(oneMiB)},
			},
		},
		{
			name: "repetitive_1MiB",
			files: []benchmarkCorpusFile{
				{name: "repeat.bin", method: Method1, payload: bytes.Repeat([]byte("goarj-repeat-pattern-"), oneMiB/len("goarj-repeat-pattern-"))},
			},
		},
		{
			name: "many_small_1000x1KiB",
			files: func() []benchmarkCorpusFile {
				files := make([]benchmarkCorpusFile, 1000)
				for i := range files {
					files[i] = benchmarkCorpusFile{
						name:    fmt.Sprintf("tiny/%04d.bin", i),
						method:  Method1,
						payload: benchmarkPayload(1 << 10),
					}
				}
				return files
			}(),
		},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			var total int64
			for _, file := range tc.files {
				total += int64(len(file.payload))
			}

			b.ReportAllocs()
			b.SetBytes(total)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_ = benchmarkArchiveFromFiles(b, tc.files)
			}
		})
	}
}

func BenchmarkOpenMultiReaderContinuationManyVolumes(b *testing.B) {
	payload := benchmarkPayload(2 << 20)
	cases := []int{8, 32, 128}

	for _, parts := range cases {
		parts := parts
		b.Run(fmt.Sprintf("%d_parts", parts), func(b *testing.B) {
			tmp := b.TempDir()
			base := filepath.Join(tmp, "many")
			partSize := len(payload) / parts
			if partSize == 0 {
				partSize = 1
			}

			for i := 0; i < parts; i++ {
				ext := ".arj"
				if i > 0 {
					ext = fmt.Sprintf(".a%02d", i)
				}
				start := i * partSize
				end := start + partSize
				if i == parts-1 || end > len(payload) {
					end = len(payload)
				}
				flags := uint8(0)
				switch {
				case parts == 1:
				case i == 0:
					flags = FlagVolume
				case i == parts-1:
					flags = FlagExtFile
				default:
					flags = FlagVolume | FlagExtFile
				}
				entries := []benchmarkVolumeEntry{
					{name: "split.bin", flags: flags, payload: payload[start:end]},
				}
				if i%16 == 0 {
					entries = append(entries, benchmarkVolumeEntry{
						name:    fmt.Sprintf("meta/%03d.txt", i),
						payload: []byte("sidecar"),
					})
				}
				benchmarkWriteVolumeArchive(b, base+ext, entries)
			}

			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				mr, err := OpenMultiReader(base + ".arj")
				if err != nil {
					b.Fatalf("OpenMultiReader: %v", err)
				}
				rc, err := mr.File[0].Open()
				if err != nil {
					_ = mr.Close()
					b.Fatalf("Open split file: %v", err)
				}
				n, err := io.Copy(io.Discard, rc)
				closeErr := rc.Close()
				multiCloseErr := mr.Close()
				if err != nil {
					b.Fatalf("Read split file: %v", err)
				}
				if closeErr != nil {
					b.Fatalf("Close split file: %v", closeErr)
				}
				if multiCloseErr != nil {
					b.Fatalf("Close multi reader: %v", multiCloseErr)
				}
				if n != int64(len(payload)) {
					b.Fatalf("decoded bytes = %d, want %d", n, len(payload))
				}
			}
		})
	}
}

func BenchmarkMultiVolumeWriterWriteEntrySplit(b *testing.B) {
	payload := benchmarkPayload(768 << 10)
	cases := []struct {
		name       string
		volumeSize int64
		method     uint16
	}{
		{name: "store_64KiB", volumeSize: 64 << 10, method: Store},
		{name: "store_256KiB", volumeSize: 256 << 10, method: Store},
		{name: "method1_64KiB", volumeSize: 64 << 10, method: Method1},
		{name: "method1_256KiB", volumeSize: 256 << 10, method: Method1},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			tmp := b.TempDir()
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				path := filepath.Join(tmp, fmt.Sprintf("split-%d.arj", i))
				mw, err := NewMultiVolumeWriter(path, MultiVolumeWriterOptions{VolumeSize: tc.volumeSize})
				if err != nil {
					b.Fatalf("NewMultiVolumeWriter: %v", err)
				}
				fw, err := mw.CreateHeader(&FileHeader{Name: "payload.bin", Method: tc.method})
				if err != nil {
					b.Fatalf("CreateHeader: %v", err)
				}
				if _, err := fw.Write(payload); err != nil {
					b.Fatalf("Write: %v", err)
				}
				if err := mw.Close(); err != nil {
					b.Fatalf("Close: %v", err)
				}
				paths, err := VolumePaths(path)
				if err != nil {
					b.Fatalf("VolumePaths: %v", err)
				}
				for _, p := range paths {
					_ = os.Remove(p)
				}
			}
		})
	}
}

func BenchmarkFindMainHeaderOffsetPrefixedScanLarge(b *testing.B) {
	real := benchmarkArchiveBytes(b, Store, benchmarkPayload(4<<10))
	decoy := benchmarkDecoyMainHeaderArchive(b, "decoy-empty.arj", true)
	cases := []struct {
		name string
		size int
	}{
		{name: "dense_16MiB", size: 16 << 20},
		{name: "dense_64MiB", size: 64 << 20},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			prefix := benchmarkSignatureDenseNoise(tc.size)
			container := make([]byte, 0, len(prefix)+len(decoy)+len(real))
			container = append(container, prefix...)
			container = append(container, decoy...)
			container = append(container, real...)
			size := int64(len(container))
			want := int64(len(prefix) + len(decoy))

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

func BenchmarkNewStreamReaderPrefixedScanLarge(b *testing.B) {
	real := benchmarkArchiveBytes(b, Store, benchmarkPayload(4<<10))
	cases := []struct {
		name string
		size int
	}{
		{name: "dense_16MiB", size: 16 << 20},
		{name: "dense_64MiB", size: 64 << 20},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			prefix := benchmarkSignatureDenseNoise(tc.size)
			container := make([]byte, 0, len(prefix)+len(real))
			container = append(container, prefix...)
			container = append(container, real...)
			wantOffset := int64(len(prefix))

			b.ReportAllocs()
			b.SetBytes(int64(len(container)))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				sr, err := NewStreamReader(bytes.NewReader(container))
				if err != nil {
					b.Fatalf("NewStreamReader: %v", err)
				}
				if got := sr.BaseOffset(); got != wantOffset {
					b.Fatalf("BaseOffset = %d, want %d", got, wantOffset)
				}
			}
		})
	}
}
