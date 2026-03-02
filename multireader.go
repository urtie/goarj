package arj

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	// DefaultMaxVolumeCount caps multi-volume enumeration/opening to avoid
	// exhausting file descriptors on very large or adversarial sets.
	DefaultMaxVolumeCount = 256
)

var (
	// ErrTooManyVolumes indicates the detected volume set exceeds a configured
	// maximum count guard.
	ErrTooManyVolumes = errors.New("arj: too many volumes in set")

	// ErrInvalidMaxVolumeCount indicates MultiVolumeOptions.MaxVolumes is invalid.
	ErrInvalidMaxVolumeCount = errors.New("arj: max volume count must be zero (default) or greater than zero")

	errInvalidMaxVolumeCount = ErrInvalidMaxVolumeCount
)

// MultiVolumeOptions configures multi-volume enumeration/open behavior.
//
// MaxVolumes:
//   - 0 means use DefaultMaxVolumeCount.
//   - >0 sets an explicit maximum.
//
// ReaderOptions are passed through to per-volume reader parsing when opening
// via OpenMultiReaderWithOptions.
type MultiVolumeOptions struct {
	MaxVolumes    int
	ReaderOptions ReaderOptions
}

// A MultiReadCloser is a Reader backed by multiple ARJ volume files.
type MultiReadCloser struct {
	files []*os.File
	Reader
}

// OpenMultiReader opens a split ARJ volume set and returns a reader that
// exposes logical file entries across continued segments.
//
// The input path may point to either the first volume (".arj") or any
// continuation volume (".aNN", ".aNNN").
func OpenMultiReader(name string) (*MultiReadCloser, error) {
	return OpenMultiReaderWithOptions(name, MultiVolumeOptions{})
}

// OpenMultiReaderWithOptions opens a split ARJ volume set with configurable
// limits.
func OpenMultiReaderWithOptions(name string, opts MultiVolumeOptions) (*MultiReadCloser, error) {
	if err := validateReaderOptions(opts.ReaderOptions); err != nil {
		return nil, err
	}
	maxVolumes, err := resolveMaxVolumeCount(opts.MaxVolumes)
	if err != nil {
		return nil, err
	}

	paths, err := volumePaths(name, maxVolumes)
	if err != nil {
		return nil, err
	}
	if len(paths) > maxVolumes {
		return nil, tooManyVolumesError(maxVolumes)
	}

	rc := &MultiReadCloser{}
	var readers []*Reader
	var sizes []int64
	var volumeReaders []io.ReaderAt
	closeOnError := func() {
		_ = rc.Close()
	}

	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			closeOnError()
			return nil, err
		}

		fi, err := f.Stat()
		if err != nil {
			_ = f.Close()
			closeOnError()
			return nil, err
		}

		r, err := NewReaderWithOptions(f, fi.Size(), opts.ReaderOptions)
		if err != nil {
			_ = f.Close()
			closeOnError()
			return nil, err
		}

		rc.files = append(rc.files, f)
		readers = append(readers, r)
		sizes = append(sizes, fi.Size())
		volumeReaders = append(volumeReaders, f)
	}
	if err := validateVolumeMainHeaderCoherence(readers); err != nil {
		closeOnError()
		return nil, err
	}
	parserLimits := normalizeParserLimits(opts.ReaderOptions.ParserLimits)

	cr, err := newConcatenatedReaderAt(volumeReaders, sizes)
	if err != nil {
		closeOnError()
		return nil, err
	}

	if err := mergeVolumeReaders(&rc.Reader, readers, cr.starts, cr, parserLimits.MaxEntries); err != nil {
		closeOnError()
		return nil, err
	}
	return rc, nil
}

// Close closes all open files in the volume set.
func (rc *MultiReadCloser) Close() error {
	if rc == nil {
		return nil
	}
	rc.stateMu.Lock()
	clearBytes(rc.password)
	rc.password = nil
	rc.stateMu.Unlock()

	var err error
	for i := len(rc.files) - 1; i >= 0; i-- {
		if closeErr := rc.files[i].Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}
	rc.files = nil
	return err
}

// VolumePaths returns contiguous ARJ volume paths for the set that contains
// name, subject to DefaultMaxVolumeCount.
//
// If name does not look like an ARJ volume name, it returns []string{name}.
func VolumePaths(name string) ([]string, error) {
	return VolumePathsWithOptions(name, MultiVolumeOptions{})
}

// VolumePathsWithOptions returns contiguous ARJ volume paths for the set that
// contains name with configurable limits.
func VolumePathsWithOptions(name string, opts MultiVolumeOptions) ([]string, error) {
	maxVolumes, err := resolveMaxVolumeCount(opts.MaxVolumes)
	if err != nil {
		return nil, err
	}
	return volumePaths(name, maxVolumes)
}

func volumePaths(name string, maxVolumes int) ([]string, error) {
	ext := filepath.Ext(name)
	switch {
	case strings.EqualFold(ext, ".arj"):
		stem := strings.TrimSuffix(name, ext)
		return collectVolumePaths(stem, name, maxVolumes, 0)
	case isVolumePartExt(ext):
		stem := strings.TrimSuffix(name, ext)
		part, width, _ := parseVolumePartExt(ext)
		if _, err := resolveContinuationInputPath(name, stem, part, width); err != nil {
			return nil, err
		}
		first, err := resolveFirstVolumePath(stem)
		if err != nil {
			return nil, err
		}
		return collectVolumePaths(stem, first, maxVolumes, width)
	default:
		return []string{name}, nil
	}
}

func resolveContinuationInputPath(name, stem string, part, width int) (string, error) {
	if ok, err := pathExists(name); err != nil {
		return "", err
	} else if ok {
		return name, nil
	}

	path, _, found, err := resolvePartVolumePath(stem, part, width)
	if err != nil {
		return "", err
	}
	if !found {
		return "", &os.PathError{Op: "open", Path: name, Err: os.ErrNotExist}
	}
	return path, nil
}

func collectVolumePaths(stem, first string, maxVolumes, preferredWidth int) ([]string, error) {
	paths := []string{first}
	for part := 1; ; part++ {
		path, width, found, err := resolvePartVolumePath(stem, part, preferredWidth)
		if err != nil {
			return nil, err
		}
		if !found {
			break
		}
		if len(paths) >= maxVolumes {
			return nil, tooManyVolumesError(maxVolumes)
		}
		paths = append(paths, path)
		if preferredWidth == 0 {
			preferredWidth = width
		}
	}
	return paths, nil
}

func resolveMaxVolumeCount(maxVolumes int) (int, error) {
	if maxVolumes == 0 {
		return DefaultMaxVolumeCount, nil
	}
	if maxVolumes < 0 {
		return 0, errInvalidMaxVolumeCount
	}
	return maxVolumes, nil
}

func tooManyVolumesError(maxVolumes int) error {
	return fmt.Errorf("%w (max=%d)", ErrTooManyVolumes, maxVolumes)
}

func resolveFirstVolumePath(stem string) (string, error) {
	candidates := []string{
		stem + ".arj",
		stem + ".ARJ",
	}
	for _, path := range candidates {
		ok, err := pathExists(path)
		if err != nil {
			return "", err
		}
		if ok {
			return path, nil
		}
	}
	dir := filepath.Dir(stem)
	if dir == "" {
		dir = "."
	}
	wantName := filepath.Base(stem) + ".arj"
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	for _, entry := range entries {
		if strings.EqualFold(entry.Name(), wantName) {
			return filepath.Join(dir, entry.Name()), nil
		}
	}
	return "", &os.PathError{Op: "open", Path: candidates[0], Err: os.ErrNotExist}
}

func resolvePartVolumePath(stem string, part int, preferredWidth int) (string, int, bool, error) {
	if part < 1 {
		return "", 0, false, nil
	}

	for _, width := range continuationWidths(preferredWidth) {
		index := fmt.Sprintf("%0*d", width, part)
		candidates := []string{
			stem + ".a" + index,
			stem + ".A" + index,
		}
		for _, path := range candidates {
			ok, err := pathExists(path)
			if err != nil {
				return "", 0, false, err
			}
			if ok {
				return path, width, true, nil
			}
		}
	}
	return "", 0, false, nil
}

func isVolumePartExt(ext string) bool {
	_, _, ok := parseVolumePartExt(ext)
	return ok
}

func parseVolumePartExt(ext string) (part int, width int, ok bool) {
	if len(ext) < 4 || ext[0] != '.' {
		return 0, 0, false
	}
	if ext[1] != 'a' && ext[1] != 'A' {
		return 0, 0, false
	}
	digits := ext[2:]
	width = len(digits)
	if width < 2 {
		return 0, 0, false
	}
	if width > 3 && digits[0] == '0' {
		return 0, 0, false
	}
	n, err := strconv.Atoi(digits)
	if err != nil || n < 1 {
		return 0, 0, false
	}
	return n, width, true
}

func continuationWidths(preferred int) []int {
	widths := make([]int, 0, 3)
	appendWidth := func(width int) {
		if width < 2 {
			return
		}
		for _, existing := range widths {
			if existing == width {
				return
			}
		}
		widths = append(widths, width)
	}

	appendWidth(preferred)
	appendWidth(2)
	appendWidth(3)
	return widths
}

func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

type concatenatedReaderAt struct {
	readers []io.ReaderAt
	sizes   []int64
	starts  []int64
	total   int64
	// volumeIndexHook is test-only instrumentation for search-call counting.
	volumeIndexHook func()
}

func newConcatenatedReaderAt(readers []io.ReaderAt, sizes []int64) (*concatenatedReaderAt, error) {
	if len(readers) != len(sizes) || len(readers) == 0 {
		return nil, ErrFormat
	}

	starts := make([]int64, len(readers))
	var total int64
	for i, size := range sizes {
		if size < 0 {
			return nil, ErrFormat
		}
		starts[i] = total
		if size > math.MaxInt64-total {
			return nil, ErrFormat
		}
		total += size
	}

	return &concatenatedReaderAt{
		readers: readers,
		sizes:   append([]int64(nil), sizes...),
		starts:  starts,
		total:   total,
	}, nil
}

func (r *concatenatedReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, ErrFormat
	}
	if len(p) == 0 {
		return 0, nil
	}
	if off >= r.total {
		return 0, io.EOF
	}

	idx := r.volumeIndex(off)
	if idx < 0 {
		return 0, io.EOF
	}

	read := 0
	for len(p) > 0 && off < r.total {
		for idx < len(r.readers) {
			end := r.starts[idx] + r.sizes[idx]
			if off < end {
				break
			}
			idx++
		}
		if idx >= len(r.readers) {
			return read, io.EOF
		}

		local := off - r.starts[idx]
		remain := r.sizes[idx] - local
		if remain <= 0 {
			return read, io.EOF
		}

		chunk := int64(len(p))
		if chunk > remain {
			chunk = remain
		}

		n, err := r.readers[idx].ReadAt(p[:int(chunk)], local)
		read += n
		off += int64(n)
		p = p[n:]

		if err != nil && !errors.Is(err, io.EOF) {
			return read, err
		}
		if int64(n) < chunk {
			return read, io.EOF
		}
		if local+int64(n) == r.sizes[idx] {
			idx++
		}
	}

	if len(p) > 0 {
		return read, io.EOF
	}
	return read, nil
}

func (r *concatenatedReaderAt) volumeIndex(off int64) int {
	if off < 0 || off >= r.total || len(r.starts) == 0 {
		return -1
	}
	if r.volumeIndexHook != nil {
		r.volumeIndexHook()
	}

	idx := sort.Search(len(r.starts), func(i int) bool {
		return r.starts[i] > off
	})
	return idx - 1
}

func validateVolumeMainHeaderCoherence(volumes []*Reader) error {
	if len(volumes) == 0 {
		return ErrFormat
	}
	base := volumes[0].ArchiveHeader
	baseFlags := base.Flags &^ FlagVolume
	baseName := normalizeVolumeArchiveName(volumes[0].ArchiveName)
	baseComment := volumes[0].Comment
	baseMainExt := base.MainExtendedHeaders
	baseFirstHeaderExtra := base.FirstHeaderExtra
	sawVolumeFlagClear := base.Flags&FlagVolume == 0

	for i := 1; i < len(volumes); i++ {
		h := volumes[i].ArchiveHeader
		if normalizeVolumeArchiveName(volumes[i].ArchiveName) != baseName {
			return inconsistentVolumeMainHeaderError("Name", i)
		}
		if volumes[i].Comment != baseComment {
			return inconsistentVolumeMainHeaderError("Comment", i)
		}
		if !equalRawExtendedHeaders(h.MainExtendedHeaders, baseMainExt) {
			return inconsistentVolumeMainHeaderError("MainExtendedHeaders", i)
		}
		if !bytes.Equal(h.FirstHeaderExtra, baseFirstHeaderExtra) {
			return inconsistentVolumeMainHeaderError("FirstHeaderExtra", i)
		}
		hasVolumeFlag := h.Flags&FlagVolume != 0
		if sawVolumeFlagClear && hasVolumeFlag {
			return inconsistentVolumeMainHeaderError("Flags", i)
		}
		if !hasVolumeFlag {
			sawVolumeFlagClear = true
		}

		switch {
		case h.FirstHeaderSize != base.FirstHeaderSize:
			return inconsistentVolumeMainHeaderError("FirstHeaderSize", i)
		case h.ArchiverVersion != base.ArchiverVersion:
			return inconsistentVolumeMainHeaderError("ArchiverVersion", i)
		case h.MinVersion != base.MinVersion:
			return inconsistentVolumeMainHeaderError("MinVersion", i)
		case h.HostOS != base.HostOS:
			return inconsistentVolumeMainHeaderError("HostOS", i)
		case h.Reserved != base.Reserved:
			return inconsistentVolumeMainHeaderError("Reserved", i)
		case h.FileType != base.FileType:
			return inconsistentVolumeMainHeaderError("FileType", i)
		case (h.Flags &^ FlagVolume) != baseFlags:
			return inconsistentVolumeMainHeaderError("Flags", i)
		case h.SecurityVersion != base.SecurityVersion:
			return inconsistentVolumeMainHeaderError("SecurityVersion", i)
		case h.SecurityEnvelopePos != base.SecurityEnvelopePos:
			return inconsistentVolumeMainHeaderError("SecurityEnvelopePos", i)
		case h.FilespecPos != base.FilespecPos:
			return inconsistentVolumeMainHeaderError("FilespecPos", i)
		case h.SecurityEnvelopeSize != base.SecurityEnvelopeSize:
			return inconsistentVolumeMainHeaderError("SecurityEnvelopeSize", i)
		case h.ExtFlags != base.ExtFlags:
			return inconsistentVolumeMainHeaderError("ExtFlags", i)
		case h.ChapterNumber != base.ChapterNumber:
			return inconsistentVolumeMainHeaderError("ChapterNumber", i)
		case h.ProtectionBlocks != base.ProtectionBlocks:
			return inconsistentVolumeMainHeaderError("ProtectionBlocks", i)
		case h.ProtectionFlags != base.ProtectionFlags:
			return inconsistentVolumeMainHeaderError("ProtectionFlags", i)
		case h.ProtectionReserved != base.ProtectionReserved:
			return inconsistentVolumeMainHeaderError("ProtectionReserved", i)
		}
	}
	return nil
}

func inconsistentVolumeMainHeaderError(field string, volumeIdx int) error {
	return fmt.Errorf("%w: inconsistent multi-volume main header %s at volume index %d", ErrFormat, field, volumeIdx)
}

func equalRawExtendedHeaders(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

func normalizeVolumeArchiveName(name string) string {
	ext := filepath.Ext(name)
	if strings.EqualFold(ext, ".arj") || isVolumePartExt(ext) {
		return strings.TrimSuffix(name, ext)
	}
	return name
}

func mergeVolumeReaders(merged *Reader, volumes []*Reader, starts []int64, r io.ReaderAt, maxEntries int) error {
	if merged == nil || len(volumes) == 0 || len(volumes) != len(starts) {
		return ErrFormat
	}
	if maxEntries <= 0 {
		maxEntries = normalizeParserLimits(ParserLimits{}).MaxEntries
	}

	merged.r = r
	merged.File = nil
	merged.ArchiveHeader = volumes[0].ArchiveHeader
	merged.ArchiveName = volumes[0].ArchiveName
	merged.Comment = volumes[0].Comment
	merged.baseOffset = starts[0] + volumes[0].baseOffset

	pending := make(map[string][]*File)
	sawSplitTopology := len(volumes) == 1
	for volumeIdx, volume := range volumes {
		base := starts[volumeIdx]
		for _, sf := range volume.File {
			segment := fileSegment{
				dataOffset:       base + sf.dataOffset,
				method:           sf.Method,
				flags:            sf.Flags,
				extFlags:         sf.ExtFlags,
				passwordModifier: sf.PasswordModifier,
				compressedSize:   sf.CompressedSize64,
				uncompressedSize: sf.UncompressedSize64,
				crc32:            sf.CRC32,
			}
			if segment.flags&(FlagVolume|FlagExtFile) != 0 {
				sawSplitTopology = true
			}

			if sf.Flags&FlagExtFile == 0 {
				if len(merged.File) >= maxEntries {
					return parserEntryLimitError(maxEntries)
				}
				fh := cloneFileHeader(sf.FileHeader)
				fh.Flags &^= FlagVolume | FlagExtFile
				lf := &File{
					FileHeader: fh,
					arj:        merged,
					dataOffset: segment.dataOffset,
					segments:   []fileSegment{segment},
				}
				merged.File = append(merged.File, lf)

				if segment.flags&FlagVolume != 0 {
					pending[sf.Name] = append(pending[sf.Name], lf)
				}
				continue
			}

			queue := pending[sf.Name]
			if len(queue) == 0 {
				return ErrFormat
			}
			lf, queueIdx, err := selectPendingSplitLogicalFile(queue, sf)
			if err != nil {
				return err
			}
			queue = append(queue[:queueIdx], queue[queueIdx+1:]...)
			pending[sf.Name] = queue

			lf.segments = append(lf.segments, segment)
			if lf.UncompressedSize64 > math.MaxUint64-segment.uncompressedSize {
				return ErrFormat
			}
			if lf.CompressedSize64 > math.MaxUint64-segment.compressedSize {
				return ErrFormat
			}
			lf.UncompressedSize64 += segment.uncompressedSize
			lf.CompressedSize64 += segment.compressedSize
			lf.CRC32 = segment.crc32
			lf.Flags &^= FlagVolume | FlagExtFile

			if segment.flags&FlagVolume != 0 {
				pending[sf.Name] = append(pending[sf.Name], lf)
			}
		}
	}

	for _, queue := range pending {
		if len(queue) != 0 {
			return ErrFormat
		}
	}
	if len(volumes) > 1 && !sawSplitTopology {
		return ErrFormat
	}

	return nil
}

func continuationResumeOffset(h FileHeader) (uint64, bool) {
	if h.Flags&FlagExtFile == 0 || len(h.firstHeaderExtra) < 4 {
		return 0, false
	}
	return uint64(binary.LittleEndian.Uint32(h.firstHeaderExtra[:4])), true
}

func selectPendingSplitLogicalFile(queue []*File, continuation *File) (*File, int, error) {
	if len(queue) == 0 || continuation == nil {
		return nil, -1, ErrFormat
	}
	resumeOff, hasResume := continuationResumeOffset(continuation.FileHeader)
	if !hasResume {
		if len(queue) != 1 {
			return nil, -1, ErrFormat
		}
		return queue[0], 0, nil
	}
	matchIdx := -1
	for i := range queue {
		if queue[i] == nil {
			continue
		}
		if queue[i].UncompressedSize64 != resumeOff {
			continue
		}
		if matchIdx >= 0 {
			return nil, -1, ErrFormat
		}
		matchIdx = i
	}
	if matchIdx < 0 {
		return nil, -1, ErrFormat
	}
	return queue[matchIdx], matchIdx, nil
}

func cloneFileHeader(in FileHeader) FileHeader {
	out := in
	out.LocalExtendedHeaders = cloneLocalExtendedHeaders(in.LocalExtendedHeaders)
	out.firstHeaderExtra = append([]byte(nil), in.firstHeaderExtra...)
	return out
}
