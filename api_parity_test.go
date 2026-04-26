package arj

import (
	"bytes"
	"errors"
	"hash/crc32"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"testing/fstest"
	"time"
)

const xorMethod uint16 = 7

func TestReaderOpen(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	fw, err := w.Create("open.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := io.WriteString(fw, "open-data"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	f, err := r.Open("open.txt")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if want := "open-data"; string(got) != want {
		t.Fatalf("payload = %q, want %q", got, want)
	}

	st, err := f.Stat()
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if got, want := st.Name(), "open.txt"; got != want {
		t.Fatalf("name = %q, want %q", got, want)
	}
	if got, want := st.Size(), int64(len("open-data")); got != want {
		t.Fatalf("size = %d, want %d", got, want)
	}

	_, err = r.Open("missing.txt")
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Open missing error = %v, want %v", err, fs.ErrNotExist)
	}
}

func TestReaderOpenNilReceiver(t *testing.T) {
	var r *Reader

	_, err := r.Open("open.txt")
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("Open error = %v, want %v", err, ErrFormat)
	}

	_, err = r.Open("../x")
	if !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("Open invalid path error = %v, want %v", err, fs.ErrInvalid)
	}
}

func TestReaderOpenInvalidPath(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	fw, err := w.Create("valid.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := io.WriteString(fw, "ok"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	_, err = r.Open("../x")
	if !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("Open invalid path error = %v, want %v", err, fs.ErrInvalid)
	}
	var pe *fs.PathError
	if !errors.As(err, &pe) {
		t.Fatalf("error type = %T, want *fs.PathError", err)
	}
	if got, want := pe.Op, "open"; got != want {
		t.Fatalf("PathError.Op = %q, want %q", got, want)
	}
}

func TestReaderDuplicateFileNamesLastWinsAcrossFSAndExtract(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	first, err := w.Create("dup.txt")
	if err != nil {
		t.Fatalf("Create first: %v", err)
	}
	if _, err := io.WriteString(first, "first"); err != nil {
		t.Fatalf("Write first: %v", err)
	}
	second, err := w.Create("dup.txt")
	if err != nil {
		t.Fatalf("Create second: %v", err)
	}
	if _, err := io.WriteString(second, "second"); err != nil {
		t.Fatalf("Write second: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	f, err := r.Open("dup.txt")
	if err != nil {
		t.Fatalf("Open(dup.txt): %v", err)
	}
	data, err := io.ReadAll(f)
	_ = f.Close()
	if err != nil {
		t.Fatalf("ReadAll(dup.txt): %v", err)
	}
	if got, want := string(data), "second"; got != want {
		t.Fatalf("Open(dup.txt) payload = %q, want %q", got, want)
	}

	entries, err := fs.ReadDir(r, ".")
	if err != nil {
		t.Fatalf("ReadDir(.): %v", err)
	}
	if got, want := dirEntryNames(entries), []string{"dup.txt"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ReadDir(.) names = %v, want %v", got, want)
	}

	var walked []string
	if err := fs.WalkDir(r, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		walked = append(walked, name)
		return nil
	}); err != nil {
		t.Fatalf("WalkDir: %v", err)
	}
	if got, want := walked, []string{".", "dup.txt"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("WalkDir paths = %v, want %v", got, want)
	}

	out := filepath.Join(t.TempDir(), "out")
	if err := r.ExtractAll(out); err != nil {
		t.Fatalf("ExtractAll: %v", err)
	}
	extracted, err := os.ReadFile(filepath.Join(out, "dup.txt"))
	if err != nil {
		t.Fatalf("ReadFile extracted dup.txt: %v", err)
	}
	if got, want := string(extracted), "second"; got != want {
		t.Fatalf("extracted dup.txt payload = %q, want %q", got, want)
	}
}

func TestReaderFSIndexReflectsMutatedFileNames(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	fw, err := w.Create("a.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := io.WriteString(fw, "payload"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	f, err := r.Open("a.txt")
	if err != nil {
		t.Fatalf("Open(a.txt): %v", err)
	}
	_ = f.Close()
	firstIndex := r.fsIndex
	if firstIndex == nil {
		t.Fatalf("fs index was not cached after Open")
	}

	f, err = r.Open("a.txt")
	if err != nil {
		t.Fatalf("Open(a.txt) cached: %v", err)
	}
	_ = f.Close()
	if r.fsIndex != firstIndex {
		t.Fatalf("fs index was rebuilt without a file-header mutation")
	}

	r.File[0].Name = "b.txt"

	if _, err := r.Open("a.txt"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Open(a.txt) after rename error = %v, want %v", err, fs.ErrNotExist)
	}
	if r.fsIndex == firstIndex {
		t.Fatalf("fs index was not rebuilt after file-header mutation")
	}

	renamed, err := r.Open("b.txt")
	if err != nil {
		t.Fatalf("Open(b.txt): %v", err)
	}
	data, err := io.ReadAll(renamed)
	_ = renamed.Close()
	if err != nil {
		t.Fatalf("ReadAll(b.txt): %v", err)
	}
	if got, want := string(data), "payload"; got != want {
		t.Fatalf("Open(b.txt) payload = %q, want %q", got, want)
	}

	entries, err := fs.ReadDir(r, ".")
	if err != nil {
		t.Fatalf("ReadDir(.): %v", err)
	}
	if got, want := dirEntryNames(entries), []string{"b.txt"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ReadDir(.) names after rename = %v, want %v", got, want)
	}
}

func TestReaderPathCollisionFileWinsAcrossFSAPIs(t *testing.T) {
	tests := []struct {
		name  string
		order []string
	}{
		{name: "file first", order: []string{"file", "leaf", "dir"}},
		{name: "leaf first", order: []string{"leaf", "dir", "file"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf)

			for _, step := range tc.order {
				switch step {
				case "file":
					root, err := w.Create("shadow")
					if err != nil {
						t.Fatalf("Create(shadow): %v", err)
					}
					if _, err := io.WriteString(root, "file-wins"); err != nil {
						t.Fatalf("Write(shadow): %v", err)
					}
				case "leaf":
					leaf, err := w.Create("shadow/leaf.txt")
					if err != nil {
						t.Fatalf("Create(shadow/leaf.txt): %v", err)
					}
					if _, err := io.WriteString(leaf, "leaf-data"); err != nil {
						t.Fatalf("Write(shadow/leaf.txt): %v", err)
					}
				case "dir":
					if _, err := w.CreateHeader(&FileHeader{Name: "shadow/"}); err != nil {
						t.Fatalf("CreateHeader(shadow/): %v", err)
					}
				default:
					t.Fatalf("unknown step %q", step)
				}
			}

			if err := w.Close(); err != nil {
				t.Fatalf("Close writer: %v", err)
			}

			r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			if err != nil {
				t.Fatalf("NewReader: %v", err)
			}

			f, err := r.Open("shadow")
			if err != nil {
				t.Fatalf("Open(shadow): %v", err)
			}
			defer f.Close()

			info, err := f.Stat()
			if err != nil {
				t.Fatalf("Stat(shadow): %v", err)
			}
			if info.IsDir() {
				t.Fatalf("Open(shadow) returned directory, want file")
			}
			got, err := io.ReadAll(f)
			if err != nil {
				t.Fatalf("ReadAll(shadow): %v", err)
			}
			if want := "file-wins"; string(got) != want {
				t.Fatalf("shadow payload = %q, want %q", got, want)
			}

			_, err = r.Open("shadow/leaf.txt")
			if !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("Open(shadow/leaf.txt) error = %v, want %v", err, fs.ErrNotExist)
			}

			rootEntries, err := fs.ReadDir(r, ".")
			if err != nil {
				t.Fatalf("ReadDir(.): %v", err)
			}
			if got, want := dirEntryNames(rootEntries), []string{"shadow"}; !reflect.DeepEqual(got, want) {
				t.Fatalf("ReadDir(.) names = %v, want %v", got, want)
			}
			if rootEntries[0].IsDir() {
				t.Fatalf("ReadDir(.) entry shadow is directory, want file")
			}

			var walked []string
			if err := fs.WalkDir(r, ".", func(name string, d fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				walked = append(walked, name)
				return nil
			}); err != nil {
				t.Fatalf("WalkDir: %v", err)
			}
			if got, want := walked, []string{".", "shadow"}; !reflect.DeepEqual(got, want) {
				t.Fatalf("walked paths = %v, want %v", got, want)
			}
		})
	}
}

func TestReaderSkipsInvalidArchivePathsInFSView(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	ok, err := w.Create("safe.txt")
	if err != nil {
		t.Fatalf("Create(safe.txt): %v", err)
	}
	if _, err := io.WriteString(ok, "safe-data"); err != nil {
		t.Fatalf("Write(safe.txt): %v", err)
	}

	invalidNames := []string{
		"../escape/leaf.txt",
		"./dot/leaf.txt",
		"nested/../escape.txt",
		"bad//leaf.txt",
	}
	for _, name := range invalidNames {
		fw, err := w.Create(name)
		if err != nil {
			t.Fatalf("Create(%q): %v", name, err)
		}
		if _, err := io.WriteString(fw, "invalid-data"); err != nil {
			t.Fatalf("Write(%q): %v", name, err)
		}
	}
	if _, err := w.CreateHeader(&FileHeader{Name: "../hidden/"}); err != nil {
		t.Fatalf("CreateHeader(../hidden/): %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	entries, err := fs.ReadDir(r, ".")
	if err != nil {
		t.Fatalf("ReadDir(.): %v", err)
	}
	if got, want := dirEntryNames(entries), []string{"safe.txt"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ReadDir(.) names = %v, want %v", got, want)
	}

	f, err := r.Open("safe.txt")
	if err != nil {
		t.Fatalf("Open(safe.txt): %v", err)
	}
	data, err := io.ReadAll(f)
	_ = f.Close()
	if err != nil {
		t.Fatalf("ReadAll(safe.txt): %v", err)
	}
	if got, want := string(data), "safe-data"; got != want {
		t.Fatalf("safe.txt payload = %q, want %q", got, want)
	}

	var walked []string
	if err := fs.WalkDir(r, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		walked = append(walked, name)
		return nil
	}); err != nil {
		t.Fatalf("WalkDir: %v", err)
	}
	if got, want := walked, []string{".", "safe.txt"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("walked paths = %v, want %v", got, want)
	}
}

func TestReaderMainHeaderProbeBudgetDefaultAllowsLargeTrailingData(t *testing.T) {
	archive := buildSingleStoreArchive(t, "payload.txt", []byte("payload"))
	const trailingBytes int64 = DefaultMainHeaderProbeBudgetMax + (1 << 20)

	rdr := &virtualTailReaderAt{
		head:    archive,
		tailLen: trailingBytes,
	}
	size := int64(len(archive)) + trailingBytes

	r, err := NewReaderWithOptions(rdr, size, ReaderOptions{})
	if err != nil {
		t.Fatalf("NewReaderWithOptions default budget: %v", err)
	}
	f, err := r.Open("payload.txt")
	if err != nil {
		t.Fatalf("Open(payload.txt): %v", err)
	}
	data, err := io.ReadAll(f)
	_ = f.Close()
	if err != nil {
		t.Fatalf("ReadAll(payload.txt): %v", err)
	}
	if got, want := string(data), "payload"; got != want {
		t.Fatalf("payload = %q, want %q", got, want)
	}
}

func TestReaderMainHeaderProbeBudgetLowConfiguredFailsWithLargeTrailingData(t *testing.T) {
	archive := buildSingleStoreArchive(t, "payload.txt", []byte("payload"))
	const trailingBytes int64 = DefaultMainHeaderProbeBudgetMax + (1 << 20)

	rdr := &virtualTailReaderAt{
		head:    archive,
		tailLen: trailingBytes,
	}
	size := int64(len(archive)) + trailingBytes

	_, err := NewReaderWithOptions(rdr, size, ReaderOptions{
		MainHeaderProbeBudget: 16,
	})
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("NewReaderWithOptions low budget error = %v, want %v", err, ErrFormat)
	}
}

func TestRegisterDecompressorRejectsNil(t *testing.T) {
	const method = uint16(0xFF01)
	decompressors.Delete(method)
	t.Cleanup(func() { decompressors.Delete(method) })

	func() {
		defer func() {
			recovered := recover()
			if recovered == nil {
				t.Fatalf("RegisterDecompressor(nil) did not panic")
			}
			err, ok := recovered.(error)
			if !ok || !errors.Is(err, ErrNilDecompressor) {
				t.Fatalf("RegisterDecompressor(nil) panic = %v, want %v", recovered, ErrNilDecompressor)
			}
		}()
		RegisterDecompressor(method, nil)
	}()

	if _, ok := decompressors.Load(method); ok {
		t.Fatalf("nil decompressor registration unexpectedly stored method %d", method)
	}
}

func TestRegisterCompressorRejectsNil(t *testing.T) {
	const method = uint16(0xFF02)
	compressors.Delete(method)
	t.Cleanup(func() { compressors.Delete(method) })

	func() {
		defer func() {
			recovered := recover()
			if recovered == nil {
				t.Fatalf("RegisterCompressor(nil) did not panic")
			}
			err, ok := recovered.(error)
			if !ok || !errors.Is(err, ErrNilCompressor) {
				t.Fatalf("RegisterCompressor(nil) panic = %v, want %v", recovered, ErrNilCompressor)
			}
		}()
		RegisterCompressor(method, nil)
	}()

	if _, ok := compressors.Load(method); ok {
		t.Fatalf("nil compressor registration unexpectedly stored method %d", method)
	}
}

func TestReaderConcurrentMutableStateAccess(t *testing.T) {
	payload := []byte("concurrent-open-payload")

	var buf bytes.Buffer
	w := NewWriter(&buf)
	fw, err := w.Create("concurrent.txt")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := fw.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if got, want := len(r.File), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}

	// Ensure the per-reader map exists before concurrent reads/writes.
	r.RegisterDecompressor(xorMethod, xorDecompressor)

	const (
		loops   = 2000
		readers = 4
	)

	errCh := make(chan error, readers+2)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < loops; i++ {
			if i%2 == 0 {
				r.RegisterDecompressor(xorMethod, xorDecompressor)
			} else {
				r.RegisterDecompressor(xorMethod, nil)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < loops; i++ {
			r.SetPassword("pw")
		}
	}()

	for worker := 0; worker < readers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < loops; i++ {
				rc, err := r.File[0].Open()
				if err != nil {
					errCh <- err
					return
				}
				got, err := io.ReadAll(rc)
				closeErr := rc.Close()
				if err != nil {
					errCh <- err
					return
				}
				if closeErr != nil {
					errCh <- closeErr
					return
				}
				if !bytes.Equal(got, payload) {
					errCh <- errors.New("payload mismatch during concurrent open")
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent reader operation failed: %v", err)
		}
	}
}

func TestReaderOpenConcurrentIndexBuild(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	for _, name := range []string{
		"root.txt",
		"dir/sub/leaf.txt",
		"dir/sub/branch.txt",
		"implied/deeper/node.txt",
	} {
		fw, err := w.Create(name)
		if err != nil {
			t.Fatalf("Create(%s): %v", name, err)
		}
		if _, err := io.WriteString(fw, "payload-"+name); err != nil {
			t.Fatalf("Write(%s): %v", name, err)
		}
	}
	if _, err := w.CreateHeader(&FileHeader{Name: "explicit/"}); err != nil {
		t.Fatalf("CreateHeader(explicit/): %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	targets := []string{".", "dir", "dir/sub", "implied", "implied/deeper", "explicit", "root.txt", "dir/sub/leaf.txt"}
	const (
		workers = 8
		loops   = 300
	)

	errCh := make(chan error, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup

	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			<-start
			for i := 0; i < loops; i++ {
				name := targets[(i+offset)%len(targets)]
				f, err := r.Open(name)
				if err != nil {
					errCh <- err
					return
				}

				info, err := f.Stat()
				if err != nil {
					_ = f.Close()
					errCh <- err
					return
				}
				if info.IsDir() {
					rd, ok := f.(fs.ReadDirFile)
					if !ok {
						_ = f.Close()
						errCh <- errors.New("directory open did not return fs.ReadDirFile")
						return
					}
					if _, err := rd.ReadDir(-1); err != nil {
						_ = f.Close()
						errCh <- err
						return
					}
				} else {
					if _, err := io.ReadAll(f); err != nil {
						_ = f.Close()
						errCh <- err
						return
					}
				}
				if err := f.Close(); err != nil {
					errCh <- err
					return
				}

				_, err = r.Open("missing.txt")
				if !errors.Is(err, fs.ErrNotExist) {
					errCh <- errors.New("missing file lookup returned unexpected error")
					return
				}
			}
		}(worker)
	}

	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent Open failed: %v", err)
		}
	}
}

func TestFileOpenRaw(t *testing.T) {
	payload := []byte("open-raw-payload")

	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.RegisterCompressor(xorMethod, xorCompressor)

	fw, err := w.CreateHeader(&FileHeader{Name: "raw.bin", Method: xorMethod})
	if err != nil {
		t.Fatalf("CreateHeader: %v", err)
	}
	if _, err := fw.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	r.RegisterDecompressor(xorMethod, xorDecompressor)

	raw, err := r.File[0].OpenRaw()
	if err != nil {
		t.Fatalf("OpenRaw: %v", err)
	}
	gotRaw, err := io.ReadAll(raw)
	if err != nil {
		t.Fatalf("ReadAll raw: %v", err)
	}
	if want := xorBytes(payload); !bytes.Equal(gotRaw, want) {
		t.Fatalf("raw payload = %v, want %v", gotRaw, want)
	}
}

func TestWriterCreateRaw(t *testing.T) {
	payload := []byte("create-raw-payload")
	rawPayload := xorBytes(payload)
	mod := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)

	var buf bytes.Buffer
	w := NewWriter(&buf)

	fh := &FileHeader{
		Name:               "raw-create.bin",
		Method:             xorMethod,
		Modified:           mod,
		CRC32:              crc32.ChecksumIEEE(payload),
		CompressedSize64:   uint64(len(rawPayload)),
		UncompressedSize64: uint64(len(payload)),
	}
	fw, err := w.CreateRaw(fh)
	if err != nil {
		t.Fatalf("CreateRaw: %v", err)
	}
	if _, err := fw.Write(rawPayload); err != nil {
		t.Fatalf("Write raw payload: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	r.RegisterDecompressor(xorMethod, xorDecompressor)

	if got, want := r.File[0].Method, xorMethod; got != want {
		t.Fatalf("method = %d, want %d", got, want)
	}
	if got, want := r.File[0].CompressedSize64, uint64(len(rawPayload)); got != want {
		t.Fatalf("compressed size = %d, want %d", got, want)
	}
	if got, want := r.File[0].UncompressedSize64, uint64(len(payload)); got != want {
		t.Fatalf("uncompressed size = %d, want %d", got, want)
	}

	rc, err := r.File[0].Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload = %q, want %q", got, payload)
	}
}

func TestWriterCreateRawAutoClosesBeforeNextEntry(t *testing.T) {
	payload := []byte("raw-store-payload")
	tailPayload := []byte("tail")

	var buf bytes.Buffer
	w := NewWriter(&buf)
	fw, err := w.CreateRaw(&FileHeader{
		Name:               "raw-store.bin",
		Method:             Store,
		CRC32:              crc32.ChecksumIEEE(payload),
		CompressedSize64:   uint64(len(payload)),
		UncompressedSize64: uint64(len(payload)),
	})
	if err != nil {
		t.Fatalf("CreateRaw: %v", err)
	}
	if _, err := fw.Write(payload); err != nil {
		t.Fatalf("Write raw payload: %v", err)
	}

	tail, err := w.CreateHeader(&FileHeader{Name: "tail.bin", Method: Store})
	if err != nil {
		t.Fatalf("CreateHeader tail: %v", err)
	}
	if _, err := tail.Write(tailPayload); err != nil {
		t.Fatalf("Write tail: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if got, want := len(r.File), 2; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	if got := mustReadFileEntry(t, r.File[0]); !bytes.Equal(got, payload) {
		t.Fatalf("raw payload = %q, want %q", got, payload)
	}
	if got := mustReadFileEntry(t, r.File[1]); !bytes.Equal(got, tailPayload) {
		t.Fatalf("tail payload = %q, want %q", got, tailPayload)
	}
}

func TestWriterCreateRawRejectsCompressedSizeMismatch(t *testing.T) {
	payload := []byte("too-long")

	var buf bytes.Buffer
	w := NewWriter(&buf)
	fw, err := w.CreateRaw(&FileHeader{
		Name:               "bad-raw.bin",
		Method:             Store,
		CRC32:              crc32.ChecksumIEEE(payload),
		CompressedSize64:   uint64(len(payload) - 1),
		UncompressedSize64: uint64(len(payload)),
	})
	if err != nil {
		t.Fatalf("CreateRaw: %v", err)
	}
	n, err := fw.Write(payload)
	if got, want := n, len(payload)-1; got != want {
		t.Fatalf("Write bytes = %d, want %d", got, want)
	}
	if !errors.Is(err, errRawPayloadSizeMismatch) {
		t.Fatalf("Write error = %v, want %v", err, errRawPayloadSizeMismatch)
	}
	if err := fw.(io.Closer).Close(); !errors.Is(err, errRawPayloadSizeMismatch) {
		t.Fatalf("Close raw writer error = %v, want %v", err, errRawPayloadSizeMismatch)
	}
	if err := w.Close(); !errors.Is(err, errRawPayloadSizeMismatch) {
		t.Fatalf("Close writer error = %v, want %v", err, errRawPayloadSizeMismatch)
	}
}

func TestWriterCreateRawRejectsStoreCRCMismatch(t *testing.T) {
	payload := []byte("crc-mismatch")

	var buf bytes.Buffer
	w := NewWriter(&buf)
	fw, err := w.CreateRaw(&FileHeader{
		Name:               "bad-crc.bin",
		Method:             Store,
		CRC32:              crc32.ChecksumIEEE([]byte("different")),
		CompressedSize64:   uint64(len(payload)),
		UncompressedSize64: uint64(len(payload)),
	})
	if err != nil {
		t.Fatalf("CreateRaw: %v", err)
	}
	if _, err := fw.Write(payload); err != nil {
		t.Fatalf("Write raw payload: %v", err)
	}
	if err := fw.(io.Closer).Close(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("Close raw writer error = %v, want %v", err, ErrChecksum)
	}
	if err := w.Close(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("Close writer error = %v, want %v", err, ErrChecksum)
	}
}

func TestWriterCopy(t *testing.T) {
	payload := []byte("copy-payload")
	rawPayload := xorBytes(payload)

	var src bytes.Buffer
	sw := NewWriter(&src)
	srcHdr := &FileHeader{
		Name:               "copy.bin",
		Method:             xorMethod,
		CRC32:              crc32.ChecksumIEEE(payload),
		CompressedSize64:   uint64(len(rawPayload)),
		UncompressedSize64: uint64(len(payload)),
	}
	swRaw, err := sw.CreateRaw(srcHdr)
	if err != nil {
		t.Fatalf("CreateRaw source: %v", err)
	}
	if _, err := swRaw.Write(rawPayload); err != nil {
		t.Fatalf("Write source raw payload: %v", err)
	}
	if err := sw.Close(); err != nil {
		t.Fatalf("Close source writer: %v", err)
	}

	srcReader, err := NewReader(bytes.NewReader(src.Bytes()), int64(src.Len()))
	if err != nil {
		t.Fatalf("NewReader source: %v", err)
	}

	var dst bytes.Buffer
	dw := NewWriter(&dst)
	if err := dw.Copy(srcReader.File[0]); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if err := dw.Close(); err != nil {
		t.Fatalf("Close destination writer: %v", err)
	}

	dstReader, err := NewReader(bytes.NewReader(dst.Bytes()), int64(dst.Len()))
	if err != nil {
		t.Fatalf("NewReader destination: %v", err)
	}
	dstReader.RegisterDecompressor(xorMethod, xorDecompressor)

	if got, want := dstReader.File[0].Method, xorMethod; got != want {
		t.Fatalf("method = %d, want %d", got, want)
	}
	if got, want := dstReader.File[0].CompressedSize64, uint64(len(rawPayload)); got != want {
		t.Fatalf("compressed size = %d, want %d", got, want)
	}
	if got, want := dstReader.File[0].CRC32, srcHdr.CRC32; got != want {
		t.Fatalf("crc = %d, want %d", got, want)
	}

	rc, err := dstReader.File[0].Open()
	if err != nil {
		t.Fatalf("Open copied file: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll copied file: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload = %q, want %q", got, payload)
	}
}

func TestWriterCopyRejectsMultiSegmentEntry(t *testing.T) {
	tmp := t.TempDir()
	base := filepath.Join(tmp, "copy-split")
	part1 := bytes.Repeat([]byte("left-"), 32)
	part2 := bytes.Repeat([]byte("right-"), 32)

	writeVolumeArchive(t, base+".arj", []volumeEntry{
		{name: "split.bin", flags: FlagVolume, payload: part1},
	})
	writeVolumeArchive(t, base+".a01", []volumeEntry{
		{name: "split.bin", flags: FlagExtFile, payload: part2},
	})

	src, err := OpenMultiReader(base + ".arj")
	if err != nil {
		t.Fatalf("OpenMultiReader: %v", err)
	}
	defer src.Close()

	if got, want := len(src.File), 1; got != want {
		t.Fatalf("source file count = %d, want %d", got, want)
	}
	if got, want := len(src.File[0].segments), 2; got != want {
		t.Fatalf("segment count = %d, want %d", got, want)
	}

	var dst bytes.Buffer
	w := NewWriter(&dst)
	if err := w.Copy(src.File[0]); !errors.Is(err, errRawCopyMultisegment) {
		t.Fatalf("Copy multi-segment error = %v, want %v", err, errRawCopyMultisegment)
	}
	if got := dst.Len(); got != 0 {
		t.Fatalf("destination length after rejected Copy = %d, want 0", got)
	}
	if err := w.SetArchiveName("after-copy-reject.arj"); err != nil {
		t.Fatalf("SetArchiveName after rejected Copy: %v", err)
	}
}

func TestWriterCopyFailureIsStickyAndCloseFails(t *testing.T) {
	payload := bytes.Repeat([]byte("copy-failure-payload"), 16)

	var src bytes.Buffer
	sw := NewWriter(&src)
	srcHdr := &FileHeader{
		Name:               "copy-fail.bin",
		Method:             Store,
		CRC32:              crc32.ChecksumIEEE(payload),
		CompressedSize64:   uint64(len(payload)),
		UncompressedSize64: uint64(len(payload)),
	}
	swRaw, err := sw.CreateRaw(srcHdr)
	if err != nil {
		t.Fatalf("CreateRaw source: %v", err)
	}
	if _, err := swRaw.Write(payload); err != nil {
		t.Fatalf("Write source payload: %v", err)
	}
	if err := sw.Close(); err != nil {
		t.Fatalf("Close source writer: %v", err)
	}

	srcReader, err := NewReader(bytes.NewReader(src.Bytes()), int64(src.Len()))
	if err != nil {
		t.Fatalf("NewReader source: %v", err)
	}
	srcFile := srcReader.File[0]
	wantErr := errors.New("test: source read failure")
	failAt := srcFile.dataOffset + int64(len(payload))/2
	if failAt <= srcFile.dataOffset {
		failAt = srcFile.dataOffset + 1
	}
	srcFile.arj.r = &failAfterOffsetReaderAt{
		data:   src.Bytes(),
		failAt: failAt,
		err:    wantErr,
	}

	var dst bytes.Buffer
	dw := NewWriter(&dst)
	if err := dw.Copy(srcFile); !errors.Is(err, wantErr) || !errors.Is(err, errRawCopySizeMismatch) {
		t.Fatalf("Copy failure error = %v, want wrapped %v and %v", err, wantErr, errRawCopySizeMismatch)
	}
	if _, err := dw.Create("tail.txt"); !errors.Is(err, wantErr) {
		t.Fatalf("Create after failed Copy error = %v, want wrapped %v", err, wantErr)
	}
	if err := dw.Close(); !errors.Is(err, wantErr) {
		t.Fatalf("Close after failed Copy error = %v, want wrapped %v", err, wantErr)
	}
}

func TestWriterAddFS(t *testing.T) {
	mod := time.Date(2024, 8, 9, 10, 11, 12, 0, time.UTC)
	fsys := fstest.MapFS{
		"root.txt": {
			Data:    []byte("root-file"),
			Mode:    0o644,
			ModTime: mod,
		},
		"sub": {
			Mode:    fs.ModeDir | 0o755,
			ModTime: mod,
		},
		"sub/leaf.txt": {
			Data:    []byte("leaf-file"),
			Mode:    0o600,
			ModTime: mod,
		},
	}

	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.AddFS(fsys); err != nil {
		t.Fatalf("AddFS: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	methodByName := make(map[string]uint16, len(r.File))
	for _, f := range r.File {
		methodByName[f.Name] = f.Method
	}
	if got, want := methodByName["root.txt"], uint16(Method1); got != want {
		t.Fatalf("root.txt method = %d, want %d", got, want)
	}
	if got, want := methodByName["sub/leaf.txt"], uint16(Method1); got != want {
		t.Fatalf("sub/leaf.txt method = %d, want %d", got, want)
	}
	if got, want := methodByName["sub/"], uint16(Store); got != want {
		t.Fatalf("sub/ method = %d, want %d", got, want)
	}

	root, err := r.Open("root.txt")
	if err != nil {
		t.Fatalf("Open root.txt: %v", err)
	}
	rootData, err := io.ReadAll(root)
	if err != nil {
		t.Fatalf("ReadAll root.txt: %v", err)
	}
	_ = root.Close()
	if got, want := string(rootData), "root-file"; got != want {
		t.Fatalf("root.txt payload = %q, want %q", got, want)
	}

	leaf, err := r.Open("sub/leaf.txt")
	if err != nil {
		t.Fatalf("Open sub/leaf.txt: %v", err)
	}
	leafData, err := io.ReadAll(leaf)
	if err != nil {
		t.Fatalf("ReadAll sub/leaf.txt: %v", err)
	}
	_ = leaf.Close()
	if got, want := string(leafData), "leaf-file"; got != want {
		t.Fatalf("sub/leaf.txt payload = %q, want %q", got, want)
	}

	dir, err := r.Open("sub")
	if err != nil {
		t.Fatalf("Open sub: %v", err)
	}
	defer dir.Close()
	st, err := dir.Stat()
	if err != nil {
		t.Fatalf("Stat sub: %v", err)
	}
	if !st.IsDir() {
		t.Fatalf("sub is not a directory")
	}
}

func TestWriterAddFSSurfacesEntryFinalizeFailure(t *testing.T) {
	fsys := fstest.MapFS{
		"bad.txt": {Data: []byte("bad-payload"), Mode: 0o644},
	}

	ws := &patchFailureWriteSeeker{failWriteAfterPatchSeek: true}
	w := NewWriter(ws)

	if err := w.AddFS(fsys); !errors.Is(err, errInjectedPatchWrite) {
		t.Fatalf("AddFS error = %v, want %v", err, errInjectedPatchWrite)
	}
	if _, err := w.Create("tail.txt"); !errors.Is(err, errInjectedPatchWrite) {
		t.Fatalf("Create after failed AddFS error = %v, want wrapped %v", err, errInjectedPatchWrite)
	}
	if err := w.Close(); !errors.Is(err, errInjectedPatchWrite) {
		t.Fatalf("Close after failed AddFS error = %v, want wrapped %v", err, errInjectedPatchWrite)
	}
}

func TestWriterAddFSFailureIsStickyAndCloseFails(t *testing.T) {
	openErr := errors.New("test: addfs open failure")
	readErr := errors.New("test: addfs read failure")
	closeErr := errors.New("test: addfs close failure")

	tests := []struct {
		name    string
		fsys    fs.FS
		wantErr error
	}{
		{
			name: "open",
			fsys: openFailMapFS{
				MapFS: fstest.MapFS{
					"bad.txt": {Data: []byte("bad-open"), Mode: 0o644},
				},
				failName: "bad.txt",
				failErr:  openErr,
			},
			wantErr: openErr,
		},
		{
			name: "read",
			fsys: readFailMapFS{
				MapFS: fstest.MapFS{
					"bad.txt": {Data: []byte("bad-read"), Mode: 0o644},
				},
				failName: "bad.txt",
				failErr:  readErr,
			},
			wantErr: readErr,
		},
		{
			name: "close",
			fsys: closeFailMapFS{
				MapFS: fstest.MapFS{
					"bad.txt": {Data: []byte("bad-close"), Mode: 0o644},
				},
				failName: "bad.txt",
				failErr:  closeErr,
			},
			wantErr: closeErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf)
			if err := w.AddFS(tc.fsys); !errors.Is(err, tc.wantErr) {
				t.Fatalf("AddFS error = %v, want wrapped %v", err, tc.wantErr)
			}
			if _, err := w.Create("tail.txt"); !errors.Is(err, tc.wantErr) {
				t.Fatalf("Create after failed AddFS error = %v, want wrapped %v", err, tc.wantErr)
			}
			if err := w.Close(); !errors.Is(err, tc.wantErr) {
				t.Fatalf("Close after failed AddFS error = %v, want wrapped %v", err, tc.wantErr)
			}
		})
	}
}

func TestReaderTraversalWithImpliedDirs(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	root, err := w.Create("root.txt")
	if err != nil {
		t.Fatalf("Create root.txt: %v", err)
	}
	if _, err := io.WriteString(root, "root-data"); err != nil {
		t.Fatalf("Write root.txt: %v", err)
	}
	leaf, err := w.Create("implied/leaf.txt")
	if err != nil {
		t.Fatalf("Create implied/leaf.txt: %v", err)
	}
	if _, err := io.WriteString(leaf, "leaf-data"); err != nil {
		t.Fatalf("Write implied/leaf.txt: %v", err)
	}
	deep, err := w.Create("implied/deeper/node.txt")
	if err != nil {
		t.Fatalf("Create implied/deeper/node.txt: %v", err)
	}
	if _, err := io.WriteString(deep, "node-data"); err != nil {
		t.Fatalf("Write implied/deeper/node.txt: %v", err)
	}
	_, err = w.CreateHeader(&FileHeader{Name: "explicit/"})
	if err != nil {
		t.Fatalf("CreateHeader explicit/: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	dot, err := r.Open(".")
	if err != nil {
		t.Fatalf("Open .: %v", err)
	}
	defer dot.Close()
	dotInfo, err := dot.Stat()
	if err != nil {
		t.Fatalf("Stat .: %v", err)
	}
	if !dotInfo.IsDir() {
		t.Fatalf("root stat IsDir = false, want true")
	}
	if got, want := dotInfo.Name(), "."; got != want {
		t.Fatalf("root stat name = %q, want %q", got, want)
	}

	dotDir, ok := dot.(fs.ReadDirFile)
	if !ok {
		t.Fatalf("Open(.) returned %T, want fs.ReadDirFile", dot)
	}
	rootEntries, err := dotDir.ReadDir(-1)
	if err != nil {
		t.Fatalf("ReadDir(.) : %v", err)
	}
	if got, want := dirEntryNames(rootEntries), []string{"explicit", "implied", "root.txt"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("root entries = %v, want %v", got, want)
	}

	implied, err := r.Open("implied")
	if err != nil {
		t.Fatalf("Open implied: %v", err)
	}
	defer implied.Close()

	impliedDir, ok := implied.(fs.ReadDirFile)
	if !ok {
		t.Fatalf("Open(implied) returned %T, want fs.ReadDirFile", implied)
	}
	first, err := impliedDir.ReadDir(1)
	if err != nil {
		t.Fatalf("ReadDir(1) first: %v", err)
	}
	if got, want := dirEntryNames(first), []string{"deeper"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ReadDir(1) first names = %v, want %v", got, want)
	}
	second, err := impliedDir.ReadDir(1)
	if err != nil {
		t.Fatalf("ReadDir(1) second: %v", err)
	}
	if got, want := dirEntryNames(second), []string{"leaf.txt"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ReadDir(1) second names = %v, want %v", got, want)
	}
	third, err := impliedDir.ReadDir(1)
	if !errors.Is(err, io.EOF) || len(third) != 0 {
		t.Fatalf("ReadDir(1) end = (%v, %v), want (empty, EOF)", third, err)
	}

	impliedEntries, err := fs.ReadDir(r, "implied")
	if err != nil {
		t.Fatalf("fs.ReadDir(implied): %v", err)
	}
	if got, want := dirEntryNames(impliedEntries), []string{"deeper", "leaf.txt"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("fs.ReadDir(implied) = %v, want %v", got, want)
	}

	var walked []string
	if err := fs.WalkDir(r, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		walked = append(walked, name)
		return nil
	}); err != nil {
		t.Fatalf("WalkDir: %v", err)
	}
	if got, want := walked, []string{".", "explicit", "implied", "implied/deeper", "implied/deeper/node.txt", "implied/leaf.txt", "root.txt"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("walked paths = %v, want %v", got, want)
	}
}

func TestReaderOpenDirectoryReadErrorStable(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.CreateHeader(&FileHeader{Name: "sub/"}); err != nil {
		t.Fatalf("CreateHeader(sub/): %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	f, err := r.Open("sub")
	if err != nil {
		t.Fatalf("Open sub: %v", err)
	}
	defer f.Close()

	probe := make([]byte, 1)
	for i := 0; i < 2; i++ {
		n, err := f.Read(probe)
		if n != 0 {
			t.Fatalf("Read #%d bytes = %d, want 0", i+1, n)
		}
		var pe *fs.PathError
		if !errors.As(err, &pe) {
			t.Fatalf("Read #%d error type = %T, want *fs.PathError", i+1, err)
		}
		if pe.Op != "read" || pe.Path != "sub" {
			t.Fatalf("Read #%d path error = (%q, %q), want (read, sub)", i+1, pe.Op, pe.Path)
		}
		if pe.Err != errReadOnDirectory {
			t.Fatalf("Read #%d wrapped error = %v, want %v", i+1, pe.Err, errReadOnDirectory)
		}
		if !errors.Is(err, errReadOnDirectory) {
			t.Fatalf("Read #%d error does not match sentinel %v", i+1, errReadOnDirectory)
		}
	}
}

func TestReaderDirEntryMetadataMethods(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.CreateHeader(&FileHeader{Name: "sub/"}); err != nil {
		t.Fatalf("CreateHeader(sub/): %v", err)
	}
	fw, err := w.CreateHeader(&FileHeader{Name: "sub/leaf.txt", Method: Store})
	if err != nil {
		t.Fatalf("CreateHeader(leaf): %v", err)
	}
	if _, err := io.WriteString(fw, "leaf-data"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	entries, err := fs.ReadDir(r, "sub")
	if err != nil {
		t.Fatalf("ReadDir(sub): %v", err)
	}
	if got, want := len(entries), 1; got != want {
		t.Fatalf("entry count = %d, want %d", got, want)
	}
	e := entries[0]
	if got, want := e.Type(), fs.FileMode(0); got != want {
		t.Fatalf("Type() = %v, want %v", got, want)
	}
	info, err := e.Info()
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if got, want := info.Name(), "leaf.txt"; got != want {
		t.Fatalf("Info().Name() = %q, want %q", got, want)
	}
	if got, want := info.Size(), int64(len("leaf-data")); got != want {
		t.Fatalf("Info().Size() = %d, want %d", got, want)
	}
	if sys := info.Sys(); sys == nil {
		t.Fatalf("Info().Sys() = nil, want non-nil")
	}
}

func TestWriterCreateRawPreservesZeroMetadataFields(t *testing.T) {
	payload := []byte("raw-zero-metadata")

	h := &FileHeader{
		Name:                 "zero-meta.bin",
		Method:               Store,
		CRC32:                crc32.ChecksumIEEE(payload),
		CompressedSize64:     uint64(len(payload)),
		UncompressedSize64:   uint64(len(payload)),
		FirstHeaderSize:      arjMinFirstHeaderSize + 2,
		LocalExtendedHeaders: [][]byte{{0x9a}},
	}
	h.firstHeaderExtra = []byte{0xde, 0xad}
	h.fileMode = 0
	h.modifiedDOS = 0

	var buf bytes.Buffer
	w := NewWriter(&buf)
	fw, err := w.CreateRaw(h)
	if err != nil {
		t.Fatalf("CreateRaw: %v", err)
	}
	if _, err := fw.Write(payload); err != nil {
		t.Fatalf("Write raw payload: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got := &r.File[0].FileHeader

	if got.ArchiverVersion != 0 {
		t.Fatalf("ArchiverVersion = %d, want 0", got.ArchiverVersion)
	}
	if got.MinVersion != 0 {
		t.Fatalf("MinVersion = %d, want 0", got.MinVersion)
	}
	if got.HostOS != 0 {
		t.Fatalf("HostOS = %d, want 0", got.HostOS)
	}
	if got.fileMode != 0 {
		t.Fatalf("fileMode = %d, want 0", got.fileMode)
	}
	if got.modifiedDOS != 0 {
		t.Fatalf("modifiedDOS = %d, want 0", got.modifiedDOS)
	}
	if !bytes.Equal(got.firstHeaderExtra, h.firstHeaderExtra) {
		t.Fatalf("firstHeaderExtra = %v, want %v", got.firstHeaderExtra, h.firstHeaderExtra)
	}
	if !reflect.DeepEqual(got.LocalExtendedHeaders, h.LocalExtendedHeaders) {
		t.Fatalf("local extended headers = %v, want %v", got.LocalExtendedHeaders, h.LocalExtendedHeaders)
	}
}

func TestWriteLocalFileHeaderSizeBounds(t *testing.T) {
	base := FileHeader{
		Name:            "bound.bin",
		Method:          Store,
		FirstHeaderSize: arjMinFirstHeaderSize,
		ArchiverVersion: arjVersionCurrent,
		MinVersion:      arjVersionNeeded,
		HostOS:          currentHostOS(),
		fileType:        arjFileTypeBinary,
		fileMode:        uint16(fileModeToUnixMode(0o644)),
	}
	base.SetModTime(time.Unix(0, 0).UTC())

	cases := []struct {
		name    string
		comp    uint64
		uncomp  uint64
		wantErr error
	}{
		{
			name:   "max values accepted",
			comp:   maxARJFileSize,
			uncomp: maxARJFileSize,
		},
		{
			name:    "compressed overflow rejected",
			comp:    maxARJFileSize + 1,
			uncomp:  1,
			wantErr: errFileTooLarge,
		},
		{
			name:    "uncompressed overflow rejected",
			comp:    1,
			uncomp:  maxARJFileSize + 1,
			wantErr: errFileTooLarge,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := base
			h.CompressedSize64 = tc.comp
			h.UncompressedSize64 = tc.uncomp

			var buf bytes.Buffer
			err := writeLocalFileHeader(&buf, &h)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("writeLocalFileHeader error = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestWriteLocalFileHeaderNormalizesZeroFirstHeaderSize(t *testing.T) {
	h := FileHeader{
		Name:            "normalized.bin",
		Method:          Store,
		FirstHeaderSize: 0,
		ArchiverVersion: arjVersionCurrent,
		MinVersion:      arjVersionNeeded,
		HostOS:          currentHostOS(),
		fileType:        arjFileTypeBinary,
		fileMode:        uint16(fileModeToUnixMode(0o644)),
	}
	h.SetModTime(time.Unix(0, 0).UTC())

	var buf bytes.Buffer
	if err := writeLocalFileHeader(&buf, &h); err != nil {
		t.Fatalf("writeLocalFileHeader error = %v, want nil", err)
	}
	if got, want := h.FirstHeaderSize, uint8(arjMinFirstHeaderSize); got != want {
		t.Fatalf("FirstHeaderSize = %d, want %d", got, want)
	}
}

func TestWriterCopyPreservesRawMetadataFields(t *testing.T) {
	payload := []byte("copy-zero-metadata")

	srcHdr := &FileHeader{
		Name:                 "copy-zero.bin",
		Method:               Store,
		CRC32:                crc32.ChecksumIEEE(payload),
		CompressedSize64:     uint64(len(payload)),
		UncompressedSize64:   uint64(len(payload)),
		FirstHeaderSize:      arjMinFirstHeaderSize + 3,
		FilespecPos:          7,
		LocalExtendedHeaders: [][]byte{{0x11, 0x22, 0x33}},
	}
	srcHdr.firstHeaderExtra = []byte{0xa1, 0xb2, 0xc3}
	srcHdr.fileMode = 0
	srcHdr.modifiedDOS = 0

	var src bytes.Buffer
	sw := NewWriter(&src)
	srcRaw, err := sw.CreateRaw(srcHdr)
	if err != nil {
		t.Fatalf("CreateRaw source: %v", err)
	}
	if _, err := srcRaw.Write(payload); err != nil {
		t.Fatalf("Write source payload: %v", err)
	}
	if err := sw.Close(); err != nil {
		t.Fatalf("Close source writer: %v", err)
	}

	sr, err := NewReader(bytes.NewReader(src.Bytes()), int64(src.Len()))
	if err != nil {
		t.Fatalf("NewReader source: %v", err)
	}

	var dst bytes.Buffer
	dw := NewWriter(&dst)
	if err := dw.Copy(sr.File[0]); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if err := dw.Close(); err != nil {
		t.Fatalf("Close destination writer: %v", err)
	}

	dr, err := NewReader(bytes.NewReader(dst.Bytes()), int64(dst.Len()))
	if err != nil {
		t.Fatalf("NewReader destination: %v", err)
	}

	got := &dr.File[0].FileHeader
	want := &sr.File[0].FileHeader
	assertLocalFixedFieldsEqual(t, got, want)
	assertCopySemanticsEqual(t, got, want)
	if got.fileMode != want.fileMode {
		t.Fatalf("fileMode = %d, want %d", got.fileMode, want.fileMode)
	}
	if got.modifiedDOS != want.modifiedDOS {
		t.Fatalf("modifiedDOS = %d, want %d", got.modifiedDOS, want.modifiedDOS)
	}
	if !reflect.DeepEqual(got.LocalExtendedHeaders, want.LocalExtendedHeaders) {
		t.Fatalf("local extended headers = %v, want %v", got.LocalExtendedHeaders, want.LocalExtendedHeaders)
	}
}

func TestFileInfoHeaderRejectsNegativeSize(t *testing.T) {
	_, err := FileInfoHeader(stubFileInfo{
		name:    "negative.bin",
		size:    -1,
		mode:    0o644,
		modTime: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
	})
	if err == nil {
		t.Fatalf("FileInfoHeader error = nil, want non-nil")
	}
}

func TestHeaderFileInfoSizeOverflowClamp(t *testing.T) {
	overflow := &FileHeader{
		Name:               "overflow.bin",
		UncompressedSize64: uint64(math.MaxInt64) + 1,
	}
	if got, want := overflow.FileInfo().Size(), int64(math.MaxInt64); got != want {
		t.Fatalf("overflow size = %d, want %d", got, want)
	}

	max := &FileHeader{
		Name:               "max.bin",
		UncompressedSize64: uint64(math.MaxInt64),
	}
	if got, want := max.FileInfo().Size(), int64(math.MaxInt64); got != want {
		t.Fatalf("max size = %d, want %d", got, want)
	}
}

type stubFileInfo struct {
	name    string
	size    int64
	mode    fs.FileMode
	modTime time.Time
}

func (fi stubFileInfo) Name() string       { return fi.name }
func (fi stubFileInfo) Size() int64        { return fi.size }
func (fi stubFileInfo) Mode() fs.FileMode  { return fi.mode }
func (fi stubFileInfo) ModTime() time.Time { return fi.modTime }
func (fi stubFileInfo) IsDir() bool        { return fi.mode.IsDir() }
func (fi stubFileInfo) Sys() any           { return nil }

func dirEntryNames(entries []fs.DirEntry) []string {
	names := make([]string, len(entries))
	for i := range entries {
		names[i] = entries[i].Name()
	}
	return names
}

func xorCompressor(out io.Writer) (io.WriteCloser, error) {
	return &xorWriteCloser{w: out}, nil
}

type xorWriteCloser struct {
	w io.Writer
}

func (w *xorWriteCloser) Write(p []byte) (int, error) {
	buf := xorBytes(p)
	return w.w.Write(buf)
}

func (w *xorWriteCloser) Close() error {
	return nil
}

func xorDecompressor(in io.Reader) io.ReadCloser {
	return &xorReadCloser{r: in}
}

type xorReadCloser struct {
	r io.Reader
}

func (r *xorReadCloser) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	for i := 0; i < n; i++ {
		p[i] ^= 0xff
	}
	return n, err
}

func (r *xorReadCloser) Close() error {
	return nil
}

func xorBytes(p []byte) []byte {
	out := make([]byte, len(p))
	for i := range p {
		out[i] = p[i] ^ 0xff
	}
	return out
}

type failAfterOffsetReaderAt struct {
	data   []byte
	failAt int64
	err    error
}

func (r *failAfterOffsetReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	if off >= r.failAt {
		return 0, r.err
	}

	maxN := len(p)
	if remain := len(r.data) - int(off); remain < maxN {
		maxN = remain
	}
	if boundary := int(r.failAt - off); boundary < maxN {
		maxN = boundary
	}
	if maxN < 0 {
		maxN = 0
	}

	n := copy(p[:maxN], r.data[off:int(off)+maxN])
	if n < len(p) {
		if int64(off)+int64(n) >= r.failAt {
			return n, r.err
		}
		return n, io.EOF
	}
	return n, nil
}

type virtualTailReaderAt struct {
	head     []byte
	tailLen  int64
	tailByte byte
}

func (r *virtualTailReaderAt) ReadAt(p []byte, off int64) (int, error) {
	total := int64(len(r.head)) + r.tailLen
	if off < 0 || off >= total {
		return 0, io.EOF
	}

	n := 0
	if off < int64(len(r.head)) {
		n = copy(p, r.head[off:])
	}

	remain := len(p) - n
	if remain > 0 {
		tailOff := off + int64(n)
		if tailOff < total {
			fill := remain
			if available := int(total - tailOff); available < fill {
				fill = available
			}
			buf := p[n : n+fill]
			if r.tailByte == 0 {
				clear(buf)
			} else {
				for i := range buf {
					buf[i] = r.tailByte
				}
			}
			n += fill
		}
	}

	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

type closeFailMapFS struct {
	fstest.MapFS
	failName string
	failErr  error
}

func (f closeFailMapFS) Open(name string) (fs.File, error) {
	base, err := f.MapFS.Open(name)
	if err != nil {
		return nil, err
	}
	if name != f.failName {
		return base, nil
	}
	return &closeFailFile{
		File: base,
		err:  f.failErr,
	}, nil
}

type closeFailFile struct {
	fs.File
	err error
}

func (f *closeFailFile) Close() error {
	return errors.Join(f.File.Close(), f.err)
}
