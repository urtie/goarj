package arj

import (
	"errors"
	"fmt"
	"io/fs"
	"runtime"
	"time"
)

var (
	// ErrFormat indicates the archive is malformed.
	ErrFormat = errors.New("arj: not a valid arj file")
	// ErrAlgorithm indicates a compression method is not supported.
	ErrAlgorithm = errors.New("arj: unsupported compression algorithm")
	// ErrChecksum indicates a file checksum mismatch.
	ErrChecksum = errors.New("arj: checksum error")
	// ErrEncrypted indicates encrypted entry data cannot be processed in
	// the current operation (for example, write/copy paths).
	ErrEncrypted = errors.New("arj: encrypted entries are not supported")
	// ErrPasswordRequired indicates an encrypted file was opened without a password.
	ErrPasswordRequired = errors.New("arj: password required for encrypted entry")
	// ErrUnsupportedEncryption indicates the encryption mode is not supported.
	ErrUnsupportedEncryption = errors.New("arj: unsupported encryption mode")
	// ErrSecurityMode indicates ARJ security envelope/signature/protection
	// modes are not supported.
	ErrSecurityMode = errors.New("arj: secured mode is not supported")
	// ErrInsecurePath indicates an archive entry name is not safe for
	// extraction to the local filesystem.
	ErrInsecurePath = errors.New("arj: insecure file path")
	// ErrStrictModeUnsupported indicates strict extraction mode is not
	// available on the current platform.
	ErrStrictModeUnsupported = errors.New("arj: strict extraction mode is unsupported on this platform")
)

// UnsupportedModeKind identifies unsupported security/encryption modes.
type UnsupportedModeKind uint8

const (
	UnsupportedModeKindSecured UnsupportedModeKind = iota + 1
	UnsupportedModeKindEncrypted
)

func (k UnsupportedModeKind) String() string {
	switch k {
	case UnsupportedModeKindSecured:
		return "secured"
	case UnsupportedModeKindEncrypted:
		return "encrypted"
	default:
		return "unknown"
	}
}

// UnsupportedSecurityMode identifies unsupported ARJ security sub-modes.
type UnsupportedSecurityMode uint8

const (
	UnsupportedSecurityModeNone UnsupportedSecurityMode = iota
	UnsupportedSecurityModeEnvelope
	UnsupportedSecurityModeSignature
	UnsupportedSecurityModeProtection
)

func (m UnsupportedSecurityMode) String() string {
	switch m {
	case UnsupportedSecurityModeEnvelope:
		return "envelope"
	case UnsupportedSecurityModeSignature:
		return "signature"
	case UnsupportedSecurityModeProtection:
		return "protection"
	default:
		return "unknown"
	}
}

// UnsupportedModeError classifies unsupported security/encryption modes.
type UnsupportedModeError struct {
	Kind              UnsupportedModeKind
	SecurityMode      UnsupportedSecurityMode
	Flags             uint8
	EncryptionVersion uint8
}

func (e *UnsupportedModeError) Error() string {
	if e == nil {
		return "arj: unsupported mode"
	}
	if e.Kind == UnsupportedModeKindEncrypted {
		return fmt.Sprintf("arj: unsupported %s mode (flags=0x%02x, encryption_version=%d)", e.Kind, e.Flags, e.EncryptionVersion)
	}
	if e.Kind == UnsupportedModeKindSecured && e.SecurityMode != UnsupportedSecurityModeNone {
		return fmt.Sprintf("arj: unsupported %s mode (security_mode=%s, flags=0x%02x)", e.Kind, e.SecurityMode, e.Flags)
	}
	return fmt.Sprintf("arj: unsupported %s mode (flags=0x%02x)", e.Kind, e.Flags)
}

func (e *UnsupportedModeError) Unwrap() error {
	if e == nil {
		return nil
	}
	switch e.Kind {
	case UnsupportedModeKindEncrypted:
		return ErrEncrypted
	case UnsupportedModeKindSecured:
		return ErrSecurityMode
	default:
		return nil
	}
}

func (e *UnsupportedModeError) Is(target error) bool {
	if e == nil {
		return false
	}
	if target == ErrUnsupportedEncryption {
		return e.Kind == UnsupportedModeKindEncrypted &&
			e.EncryptionVersion != EncryptOld &&
			e.EncryptionVersion != EncryptStd
	}
	t, ok := target.(*UnsupportedModeError)
	if !ok {
		return false
	}
	return e.Kind == t.Kind &&
		e.Flags == t.Flags &&
		e.EncryptionVersion == t.EncryptionVersion &&
		(t.SecurityMode == UnsupportedSecurityModeNone || e.SecurityMode == t.SecurityMode)
}

const (
	arjHeaderID1 = 0x60
	arjHeaderID2 = 0xEA

	arjMaxBasicHeaderSize = 2600
	arjMinFirstHeaderSize = 30

	arjVersionCurrent = 11
	arjVersionNeeded  = 1

	arjFileTypeBinary    = 0
	arjFileTypeText      = 1
	arjFileTypeMain      = 2
	arjFileTypeDirectory = 3
)

// ARJ header flag bits related to security/encryption modes.
const (
	FlagGarbled    uint8 = 0x01 // GARBLED_FLAG
	FlagOldSecured uint8 = 0x02 // OLD_SECURED_FLAG
	FlagVolume     uint8 = 0x04 // VOLUME_FLAG
	FlagExtFile    uint8 = 0x08 // EXTFILE_FLAG
	FlagProtection uint8 = 0x08 // PROT_FLAG (main header only)
	FlagSecured    uint8 = 0x40 // SECURED_FLAG
)

// ARJ protection metadata flags from the main-header v2.50+ extra bytes.
const (
	ProtectionFlagSFXStub uint8 = 0x01 // SFXSTUB_FLAG
)

// ARJ encryption version values stored in the lower nibble of ExtFlags.
const (
	EncryptOld      uint8 = 0 // ENCRYPT_OLD
	EncryptStd      uint8 = 1 // ENCRYPT_STD
	EncryptGOST256  uint8 = 2 // ENCRYPT_GOST256
	EncryptGOST256L uint8 = 3 // ENCRYPT_GOST256L
	EncryptGOST40   uint8 = 4 // ENCRYPT_GOST40
)

const (
	sIFMT   = 0xf000
	sIFSOCK = 0xc000
	sIFLNK  = 0xa000
	sIFREG  = 0x8000
	sIFBLK  = 0x6000
	sIFDIR  = 0x4000
	sIFCHR  = 0x2000
	sIFIFO  = 0x1000
	sISUID  = 0x800
	sISGID  = 0x400
	sISVTX  = 0x200
)

type headerFlagScope uint8

const (
	scopeLocalHeader headerFlagScope = iota
	scopeMainHeader
)

func unsupportedSecurityFlagsError(flags, version uint8) error {
	return unsupportedSecurityFlagsErrorForScope(flags, version, scopeLocalHeader)
}

func unsupportedMainSecurityFlagsError(flags, version uint8) error {
	return unsupportedSecurityFlagsErrorForScope(flags, version, scopeMainHeader)
}

func unsupportedSecurityFlagsErrorForScope(flags, version uint8, scope headerFlagScope) error {
	if flags&FlagGarbled != 0 {
		return &UnsupportedModeError{
			Kind:              UnsupportedModeKindEncrypted,
			Flags:             flags,
			EncryptionVersion: version,
		}
	}
	return unsupportedSecuredFlagsErrorForScope(flags, scope)
}

func unsupportedSecuredFlagsError(flags uint8) error {
	return unsupportedSecuredFlagsErrorForScope(flags, scopeLocalHeader)
}

func unsupportedMainSecuredFlagsError(flags uint8) error {
	return unsupportedSecuredFlagsErrorForScope(flags, scopeMainHeader)
}

func unsupportedSecuredFlagsErrorForScope(flags uint8, scope headerFlagScope) error {
	switch {
	case flags&FlagSecured != 0:
		return &UnsupportedModeError{
			Kind:         UnsupportedModeKindSecured,
			SecurityMode: UnsupportedSecurityModeEnvelope,
			Flags:        flags,
		}
	case flags&FlagOldSecured != 0:
		return &UnsupportedModeError{
			Kind:         UnsupportedModeKindSecured,
			SecurityMode: UnsupportedSecurityModeSignature,
			Flags:        flags,
		}
	case scope == scopeMainHeader && flags&FlagProtection != 0:
		return &UnsupportedModeError{
			Kind:         UnsupportedModeKindSecured,
			SecurityMode: UnsupportedSecurityModeProtection,
			Flags:        flags,
		}
	default:
		return nil
	}
}

func currentHostOS() uint8 {
	switch runtime.GOOS {
	case "windows":
		return 11 // WIN32
	case "darwin":
		return 4 // Mac OS
	default:
		return 2 // UNIX
	}
}

func dosDateTimeToTime(v uint32) time.Time {
	if v == 0 {
		return time.Time{}
	}
	d := uint16(v >> 16)
	t := uint16(v)

	day := int(d & 0x1f)
	month := time.Month((d >> 5) & 0xf)
	year := int(d>>9) + 1980
	hour := int((t >> 11) & 0x1f)
	minute := int((t >> 5) & 0x3f)
	second := int(t&0x1f) * 2
	return time.Date(year, month, day, hour, minute, second, 0, time.UTC)
}

func timeToDosDateTime(t time.Time) uint32 {
	if t.IsZero() {
		return 0
	}
	t = t.UTC()
	year := t.Year()
	if year < 1980 {
		year = 1980
		t = time.Date(1980, t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, time.UTC)
	}
	if year > 2107 {
		year = 2107
		t = time.Date(2107, t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, time.UTC)
	}
	date := uint16(t.Day() + int(t.Month())<<5 + (year-1980)<<9)
	clock := uint16(t.Second()/2 + t.Minute()<<5 + t.Hour()<<11)
	return uint32(date)<<16 | uint32(clock)
}

func fileModeToUnixMode(mode fs.FileMode) uint32 {
	var m uint32
	switch mode & fs.ModeType {
	default:
		m = sIFREG
	case fs.ModeDir:
		m = sIFDIR
	case fs.ModeSymlink:
		m = sIFLNK
	case fs.ModeNamedPipe:
		m = sIFIFO
	case fs.ModeSocket:
		m = sIFSOCK
	case fs.ModeDevice:
		m = sIFBLK
	case fs.ModeDevice | fs.ModeCharDevice:
		m = sIFCHR
	}
	if mode&fs.ModeSetuid != 0 {
		m |= sISUID
	}
	if mode&fs.ModeSetgid != 0 {
		m |= sISGID
	}
	if mode&fs.ModeSticky != 0 {
		m |= sISVTX
	}
	return m | uint32(mode&0o777)
}

func unixModeToFileMode(m uint32) fs.FileMode {
	mode := fs.FileMode(m & 0o777)
	switch m & sIFMT {
	case sIFBLK:
		mode |= fs.ModeDevice
	case sIFCHR:
		mode |= fs.ModeDevice | fs.ModeCharDevice
	case sIFDIR:
		mode |= fs.ModeDir
	case sIFIFO:
		mode |= fs.ModeNamedPipe
	case sIFLNK:
		mode |= fs.ModeSymlink
	case sIFSOCK:
		mode |= fs.ModeSocket
	}
	if m&sISGID != 0 {
		mode |= fs.ModeSetgid
	}
	if m&sISUID != 0 {
		mode |= fs.ModeSetuid
	}
	if m&sISVTX != 0 {
		mode |= fs.ModeSticky
	}
	return mode
}
