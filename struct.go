// Package arj provides support for reading and writing ARJ archives.
//
// The API mirrors archive/zip where practical.
package arj

import (
	"errors"
	"io/fs"
	"math"
	"path"
	"strings"
	"time"
)

// Compression method IDs.
//
// Package defaults mirror classic ARJ built-ins: methods 0..4
// (Store + Method1..Method4). Additional method IDs can be wired in via
// RegisterCompressor/RegisterDecompressor.
const (
	Store   uint16 = 0 // no compression
	Method1 uint16 = 1 // ARJ method 1
	Method2 uint16 = 2 // ARJ method 2
	Method3 uint16 = 3 // ARJ method 3
	Method4 uint16 = 4 // ARJ method 4
)

// ArchiveHeader describes the archive main header in an ARJ archive.
type ArchiveHeader struct {
	// FirstHeaderSize is the size of the fixed main-header area.
	FirstHeaderSize uint8

	// ArchiverVersion is the ARJ version that created this archive.
	ArchiverVersion uint8

	// MinVersion is the minimum ARJ version required to extract this archive.
	MinVersion uint8

	// HostOS is the originating host OS identifier.
	HostOS uint8

	// Flags are ARJ archive flags from the on-disk main header.
	Flags uint8

	// SecurityVersion is the security version field from the main header.
	SecurityVersion uint8

	// FileType is the ARJ file type from the main header (normally main type).
	FileType uint8

	// Reserved is the reserved byte from the main header.
	Reserved uint8

	// Created is the archive creation timestamp.
	Created time.Time

	// Modified is the archive modification timestamp.
	Modified time.Time

	// ArchiveSize is the archive size field from the main header.
	ArchiveSize uint32

	// SecurityEnvelopePos is the security envelope offset field.
	SecurityEnvelopePos uint32

	// FilespecPos is the file-spec offset field from the main header.
	FilespecPos uint16

	// SecurityEnvelopeSize is the security envelope size field.
	SecurityEnvelopeSize uint16

	// HostData is the legacy packed metadata field from the main header.
	// It stores ExtFlags in the low byte and ChapterNumber in the high byte.
	HostData uint16

	// ExtFlags is the main-header extended flags byte.
	ExtFlags uint8

	// ChapterNumber is the main-header chapter number byte.
	ChapterNumber uint8

	// ProtectionBlocks is the ARJ-PROTECT block multiplier from the
	// main-header v2.50+ fixed-extra bytes.
	ProtectionBlocks uint8

	// ProtectionFlags is the relocated ARJ-PROTECT/SFX flag byte from the
	// main-header v2.50+ fixed-extra bytes.
	ProtectionFlags uint8

	// ProtectionReserved is the reserved word in the main-header v2.50+
	// fixed-extra bytes.
	ProtectionReserved uint16

	// Name is the archive name string stored in the main header.
	Name string

	// Comment is the archive-level comment string.
	Comment string

	// MainExtendedHeaders stores raw main extended header payload blocks.
	// CRCs are verified while reading and regenerated while writing.
	MainExtendedHeaders [][]byte

	// FirstHeaderExtra preserves bytes between the fixed 30-byte minimum and
	// FirstHeaderSize for round-trip fidelity.
	FirstHeaderExtra []byte
}

// FileHeader describes a file within an ARJ archive.
type FileHeader struct {
	// Name is the name of the file.
	//
	// It must be a relative path and use forward slashes.
	// A trailing slash indicates a directory.
	// When extracting to disk, use SafeExtractPath to prevent path traversal.
	Name string

	// Comment is any arbitrary user-defined string.
	Comment string

	// Method is the compression method. If zero, Store is used.
	Method uint16

	// Modified is the modified time of the file.
	Modified time.Time

	// CRC32 is the checksum value stored in the local file header.
	//
	// For regular single-volume reads this is the CRC32 of the uncompressed
	// entry data. For logical entries merged by OpenMultiReader, this field is
	// the CRC32 value from the final continuation segment header.
	CRC32 uint32

	// CompressedSize64 is the compressed size of the file in bytes.
	CompressedSize64 uint64

	// UncompressedSize64 is the uncompressed size of the file in bytes.
	UncompressedSize64 uint64

	// HostOS is the originating host OS identifier stored in the archive.
	HostOS uint8

	// Flags are ARJ file flags from the on-disk file header.
	Flags uint8

	// PasswordModifier is the local-header byte used by ARJ garble modes.
	// For ENCRYPT_OLD/ENCRYPT_STD it participates in the XOR stream.
	PasswordModifier uint8

	// FirstHeaderSize is the size of the local fixed header area.
	FirstHeaderSize uint8

	// ArchiverVersion is the ARJ version that created this file header.
	ArchiverVersion uint8

	// MinVersion is the minimum ARJ version required to extract this file.
	MinVersion uint8

	// FilespecPos is the file-spec offset field from the local header.
	FilespecPos uint16

	// HostData is the legacy packed metadata field from the local header.
	// It stores ExtFlags in the low byte and ChapterNumber in the high byte.
	HostData uint16

	// ExtFlags is the local-header extended flags byte.
	ExtFlags uint8

	// ChapterNumber is the local-header chapter number byte.
	ChapterNumber uint8

	// LocalExtendedHeaders stores raw local extended header payload blocks.
	// CRCs are verified while reading and regenerated while writing.
	LocalExtendedHeaders [][]byte

	fileMode    uint16
	fileType    uint8
	modifiedDOS uint32

	firstHeaderExtra []byte
}

// FileInfo returns an fs.FileInfo for the FileHeader.
func (h *FileHeader) FileInfo() fs.FileInfo {
	return headerFileInfo{h}
}

// FileInfoHeader creates a partially-populated FileHeader from an fs.FileInfo.
func FileInfoHeader(fi fs.FileInfo) (*FileHeader, error) {
	size := fi.Size()
	if size < 0 {
		return nil, errors.New("arj: file size cannot be negative")
	}
	fh := &FileHeader{
		Name:               fi.Name(),
		Method:             Store,
		UncompressedSize64: uint64(size),
	}
	fh.SetModTime(fi.ModTime())
	fh.SetMode(fi.Mode())
	return fh, nil
}

// ModTime returns the modification time. It falls back to the DOS timestamp
// found in the ARJ header when Modified is zero.
func (h *FileHeader) ModTime() time.Time {
	if !h.Modified.IsZero() {
		return h.Modified.UTC()
	}
	return dosDateTimeToTime(h.modifiedDOS)
}

// SetModTime sets the modification time fields.
func (h *FileHeader) SetModTime(t time.Time) {
	h.Modified = t.UTC()
	h.modifiedDOS = timeToDosDateTime(h.Modified)
}

// Mode returns permission and mode bits for the FileHeader.
func (h *FileHeader) Mode() fs.FileMode {
	if h.fileMode != 0 {
		mode := unixModeToFileMode(uint32(h.fileMode))
		if h.isDir() && mode&fs.ModeDir == 0 {
			mode |= fs.ModeDir
		}
		return mode
	}
	if h.isDir() {
		return fs.ModeDir | 0o755
	}
	return 0o644
}

// SetMode changes permission and mode bits for the FileHeader.
func (h *FileHeader) SetMode(mode fs.FileMode) {
	h.fileMode = uint16(fileModeToUnixMode(mode))
	if mode&fs.ModeDir != 0 && !strings.HasSuffix(h.Name, "/") {
		h.Name += "/"
	}
}

func (h *FileHeader) isDir() bool {
	return h.fileType == arjFileTypeDirectory || strings.HasSuffix(h.Name, "/")
}

// EncryptionVersion returns the lower nibble of ExtFlags.
func (h ArchiveHeader) EncryptionVersion() uint8 {
	return h.ExtFlags & 0x0f
}

// HasSecurityEnvelope reports whether the archive header advertises an
// ARJ-SECURITY envelope mode.
func (h ArchiveHeader) HasSecurityEnvelope() bool {
	return h.Flags&FlagSecured != 0
}

// HasSecuritySignature reports whether the archive header advertises an
// ARJ-SECURITY signature mode.
func (h ArchiveHeader) HasSecuritySignature() bool {
	return h.Flags&FlagOldSecured != 0
}

// HasProtectionData reports whether the archive header advertises ARJ-PROTECT
// recovery data in the archive tail.
func (h ArchiveHeader) HasProtectionData() bool {
	return h.Flags&FlagProtection != 0
}

// HasSFXStub reports whether the relocated protection flags indicate an SFX
// stub marker in the main-header v2.50+ fixed-extra bytes.
func (h ArchiveHeader) HasSFXStub() bool {
	return h.ProtectionFlags&ProtectionFlagSFXStub != 0
}

// EncryptionVersion returns the lower nibble of ExtFlags.
func (h FileHeader) EncryptionVersion() uint8 {
	return h.ExtFlags & 0x0f
}

type headerFileInfo struct {
	fh *FileHeader
}

func (fi headerFileInfo) Name() string { return path.Base(strings.TrimSuffix(fi.fh.Name, "/")) }
func (fi headerFileInfo) Size() int64 {
	if fi.fh.UncompressedSize64 > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(fi.fh.UncompressedSize64)
}
func (fi headerFileInfo) IsDir() bool        { return fi.Mode().IsDir() }
func (fi headerFileInfo) ModTime() time.Time { return fi.fh.ModTime() }
func (fi headerFileInfo) Mode() fs.FileMode  { return fi.fh.Mode() }
func (fi headerFileInfo) Type() fs.FileMode  { return fi.fh.Mode().Type() }
func (fi headerFileInfo) Sys() any           { return fi.fh }

func (fi headerFileInfo) Info() (fs.FileInfo, error) { return fi, nil }
