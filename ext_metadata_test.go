package arj

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
)

func TestMainHeaderExtMetadataRoundTrip(t *testing.T) {
	const (
		extFlags      uint8 = 0x9d
		chapterNumber uint8 = 0x34
	)
	src := writeArchiveWithMainHeader(t, &ArchiveHeader{
		FirstHeaderSize: arjMinFirstHeaderSize,
		FileType:        arjFileTypeMain,
		Name:            "meta-main.arj",
		Comment:         "meta-main",
		ExtFlags:        extFlags,
		ChapterNumber:   chapterNumber,
	})

	mainBasic, _ := readMainHeaderBlock(t, src)
	if got := mainBasic[28]; got != extFlags {
		t.Fatalf("source ext flags byte = 0x%02x, want 0x%02x", got, extFlags)
	}
	if got := mainBasic[29]; got != chapterNumber {
		t.Fatalf("source chapter number byte = 0x%02x, want 0x%02x", got, chapterNumber)
	}

	r1, err := NewReader(bytes.NewReader(src), int64(len(src)))
	if err != nil {
		t.Fatalf("NewReader source: %v", err)
	}
	gotHeader := r1.ArchiveHeader
	if gotHeader.ExtFlags != extFlags {
		t.Fatalf("source ExtFlags = 0x%02x, want 0x%02x", gotHeader.ExtFlags, extFlags)
	}
	if gotHeader.ChapterNumber != chapterNumber {
		t.Fatalf("source ChapterNumber = 0x%02x, want 0x%02x", gotHeader.ChapterNumber, chapterNumber)
	}
	if got, want := gotHeader.HostData, packExtMetadata(extFlags, chapterNumber); got != want {
		t.Fatalf("source HostData = 0x%04x, want 0x%04x", got, want)
	}
	if got, want := gotHeader.EncryptionVersion(), uint8(extFlags&0x0f); got != want {
		t.Fatalf("source EncryptionVersion = 0x%02x, want 0x%02x", got, want)
	}

	var dst bytes.Buffer
	w := NewWriter(&dst)
	if err := w.SetArchiveHeader(&gotHeader); err != nil {
		t.Fatalf("SetArchiveHeader destination: %v", err)
	}
	fw, err := w.Create("payload.txt")
	if err != nil {
		t.Fatalf("Create destination payload: %v", err)
	}
	if _, err := io.WriteString(fw, "payload"); err != nil {
		t.Fatalf("Write destination payload: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close destination writer: %v", err)
	}

	mainBasic2, _ := readMainHeaderBlock(t, dst.Bytes())
	if got := mainBasic2[28]; got != extFlags {
		t.Fatalf("destination ext flags byte = 0x%02x, want 0x%02x", got, extFlags)
	}
	if got := mainBasic2[29]; got != chapterNumber {
		t.Fatalf("destination chapter number byte = 0x%02x, want 0x%02x", got, chapterNumber)
	}

	r2, err := NewReader(bytes.NewReader(dst.Bytes()), int64(dst.Len()))
	if err != nil {
		t.Fatalf("NewReader destination: %v", err)
	}
	gotHeader2 := r2.ArchiveHeader
	if gotHeader2.ExtFlags != extFlags {
		t.Fatalf("destination ExtFlags = 0x%02x, want 0x%02x", gotHeader2.ExtFlags, extFlags)
	}
	if gotHeader2.ChapterNumber != chapterNumber {
		t.Fatalf("destination ChapterNumber = 0x%02x, want 0x%02x", gotHeader2.ChapterNumber, chapterNumber)
	}
	if got, want := gotHeader2.EncryptionVersion(), uint8(extFlags&0x0f); got != want {
		t.Fatalf("destination EncryptionVersion = 0x%02x, want 0x%02x", got, want)
	}
}

func TestLocalHeaderExtMetadataRoundTrip(t *testing.T) {
	const (
		extFlags      uint8 = 0x2f
		chapterNumber uint8 = 0x10
	)
	src := writeSingleFileArchive(t, &FileHeader{
		Name:          "meta-local.bin",
		Method:        Store,
		ExtFlags:      extFlags,
		ChapterNumber: chapterNumber,
	}, "meta-local-payload")

	localBasic := readFirstLocalBasicHeader(t, src)
	if got := localBasic[28]; got != extFlags {
		t.Fatalf("source ext flags byte = 0x%02x, want 0x%02x", got, extFlags)
	}
	if got := localBasic[29]; got != chapterNumber {
		t.Fatalf("source chapter number byte = 0x%02x, want 0x%02x", got, chapterNumber)
	}

	r1, err := NewReader(bytes.NewReader(src), int64(len(src)))
	if err != nil {
		t.Fatalf("NewReader source: %v", err)
	}
	gotHeader := &r1.File[0].FileHeader
	if gotHeader.ExtFlags != extFlags {
		t.Fatalf("source ExtFlags = 0x%02x, want 0x%02x", gotHeader.ExtFlags, extFlags)
	}
	if gotHeader.ChapterNumber != chapterNumber {
		t.Fatalf("source ChapterNumber = 0x%02x, want 0x%02x", gotHeader.ChapterNumber, chapterNumber)
	}
	if got, want := gotHeader.HostData, packExtMetadata(extFlags, chapterNumber); got != want {
		t.Fatalf("source HostData = 0x%04x, want 0x%04x", got, want)
	}
	if got, want := gotHeader.EncryptionVersion(), uint8(extFlags&0x0f); got != want {
		t.Fatalf("source EncryptionVersion = 0x%02x, want 0x%02x", got, want)
	}

	dst := copySingleFileArchive(t, r1.File[0])
	localBasic2 := readFirstLocalBasicHeader(t, dst)
	if got := localBasic2[28]; got != extFlags {
		t.Fatalf("destination ext flags byte = 0x%02x, want 0x%02x", got, extFlags)
	}
	if got := localBasic2[29]; got != chapterNumber {
		t.Fatalf("destination chapter number byte = 0x%02x, want 0x%02x", got, chapterNumber)
	}

	r2, err := NewReader(bytes.NewReader(dst), int64(len(dst)))
	if err != nil {
		t.Fatalf("NewReader destination: %v", err)
	}
	gotHeader2 := &r2.File[0].FileHeader
	if gotHeader2.ExtFlags != extFlags {
		t.Fatalf("destination ExtFlags = 0x%02x, want 0x%02x", gotHeader2.ExtFlags, extFlags)
	}
	if gotHeader2.ChapterNumber != chapterNumber {
		t.Fatalf("destination ChapterNumber = 0x%02x, want 0x%02x", gotHeader2.ChapterNumber, chapterNumber)
	}
	if got, want := gotHeader2.EncryptionVersion(), uint8(extFlags&0x0f); got != want {
		t.Fatalf("destination EncryptionVersion = 0x%02x, want 0x%02x", got, want)
	}
}

func TestMainHeaderSecurityExtraMetadataRoundTrip(t *testing.T) {
	const (
		protectionBlocks   uint8  = 7
		protectionFlags    uint8  = ProtectionFlagSFXStub | 0x20
		protectionReserved uint16 = 0xbeef
	)

	src := writeArchiveWithMainHeader(t, &ArchiveHeader{
		FirstHeaderSize:    arjMinFirstHeaderSize + 6,
		FileType:           arjFileTypeMain,
		Name:               "security-extra.arj",
		Comment:            "security-extra",
		ProtectionBlocks:   protectionBlocks,
		ProtectionFlags:    protectionFlags,
		ProtectionReserved: protectionReserved,
		FirstHeaderExtra:   []byte{0, 0, 0, 0, 0xaa, 0xbb},
	})

	mainBasic, _ := readMainHeaderBlock(t, src)
	if got := mainBasic[30]; got != protectionBlocks {
		t.Fatalf("source protection blocks byte = 0x%02x, want 0x%02x", got, protectionBlocks)
	}
	if got := mainBasic[31]; got != protectionFlags {
		t.Fatalf("source protection flags byte = 0x%02x, want 0x%02x", got, protectionFlags)
	}
	if got := binary.LittleEndian.Uint16(mainBasic[32:34]); got != protectionReserved {
		t.Fatalf("source protection reserved word = 0x%04x, want 0x%04x", got, protectionReserved)
	}
	if got := mainBasic[34]; got != 0xaa {
		t.Fatalf("source extra[34] = 0x%02x, want 0xaa", got)
	}
	if got := mainBasic[35]; got != 0xbb {
		t.Fatalf("source extra[35] = 0x%02x, want 0xbb", got)
	}

	r1, err := NewReader(bytes.NewReader(src), int64(len(src)))
	if err != nil {
		t.Fatalf("NewReader source: %v", err)
	}
	gotHeader := r1.ArchiveHeader
	if got := gotHeader.ProtectionBlocks; got != protectionBlocks {
		t.Fatalf("source ProtectionBlocks = 0x%02x, want 0x%02x", got, protectionBlocks)
	}
	if got := gotHeader.ProtectionFlags; got != protectionFlags {
		t.Fatalf("source ProtectionFlags = 0x%02x, want 0x%02x", got, protectionFlags)
	}
	if got := gotHeader.ProtectionReserved; got != protectionReserved {
		t.Fatalf("source ProtectionReserved = 0x%04x, want 0x%04x", got, protectionReserved)
	}
	if !gotHeader.HasSFXStub() {
		t.Fatalf("source HasSFXStub = false, want true")
	}
	if len(gotHeader.FirstHeaderExtra) != 6 {
		t.Fatalf("source FirstHeaderExtra len = %d, want %d", len(gotHeader.FirstHeaderExtra), 6)
	}

	var dst bytes.Buffer
	w := NewWriter(&dst)
	if err := w.SetArchiveHeader(&gotHeader); err != nil {
		t.Fatalf("SetArchiveHeader destination: %v", err)
	}
	fw, err := w.Create("payload.txt")
	if err != nil {
		t.Fatalf("Create destination payload: %v", err)
	}
	if _, err := io.WriteString(fw, "payload"); err != nil {
		t.Fatalf("Write destination payload: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close destination writer: %v", err)
	}

	mainBasic2, _ := readMainHeaderBlock(t, dst.Bytes())
	if got := mainBasic2[30]; got != protectionBlocks {
		t.Fatalf("destination protection blocks byte = 0x%02x, want 0x%02x", got, protectionBlocks)
	}
	if got := mainBasic2[31]; got != protectionFlags {
		t.Fatalf("destination protection flags byte = 0x%02x, want 0x%02x", got, protectionFlags)
	}
	if got := binary.LittleEndian.Uint16(mainBasic2[32:34]); got != protectionReserved {
		t.Fatalf("destination protection reserved word = 0x%04x, want 0x%04x", got, protectionReserved)
	}
	if got := mainBasic2[34]; got != 0xaa {
		t.Fatalf("destination extra[34] = 0x%02x, want 0xaa", got)
	}
	if got := mainBasic2[35]; got != 0xbb {
		t.Fatalf("destination extra[35] = 0x%02x, want 0xbb", got)
	}
}

func TestExtMetadataLegacyHostDataCompatibility(t *testing.T) {
	const mainHostData = 0x8b6c
	mainSrc := writeArchiveWithMainHeader(t, &ArchiveHeader{
		FirstHeaderSize: arjMinFirstHeaderSize,
		FileType:        arjFileTypeMain,
		Name:            "legacy-main.arj",
		Comment:         "legacy-main",
		HostData:        mainHostData,
	})
	mainReader, err := NewReader(bytes.NewReader(mainSrc), int64(len(mainSrc)))
	if err != nil {
		t.Fatalf("NewReader legacy main: %v", err)
	}
	mainHeader := mainReader.ArchiveHeader
	if got, want := mainHeader.HostData, uint16(mainHostData); got != want {
		t.Fatalf("legacy main HostData = 0x%04x, want 0x%04x", got, want)
	}
	if got, want := mainHeader.ExtFlags, uint8(mainHostData&0x00ff); got != want {
		t.Fatalf("legacy main ExtFlags = 0x%02x, want 0x%02x", got, want)
	}
	if got, want := mainHeader.ChapterNumber, uint8((mainHostData>>8)&0x00ff); got != want {
		t.Fatalf("legacy main ChapterNumber = 0x%02x, want 0x%02x", got, want)
	}
	if got, want := mainHeader.EncryptionVersion(), uint8(mainHostData&0x00ff)&0x0f; got != want {
		t.Fatalf("legacy main EncryptionVersion = 0x%02x, want 0x%02x", got, want)
	}

	const localHostData = 0x417e
	localSrc := writeSingleFileArchive(t, &FileHeader{
		Name:     "legacy-local.bin",
		Method:   Store,
		HostData: localHostData,
	}, "legacy-local")
	localReader, err := NewReader(bytes.NewReader(localSrc), int64(len(localSrc)))
	if err != nil {
		t.Fatalf("NewReader legacy local: %v", err)
	}
	localHeader := &localReader.File[0].FileHeader
	if got, want := localHeader.HostData, uint16(localHostData); got != want {
		t.Fatalf("legacy local HostData = 0x%04x, want 0x%04x", got, want)
	}
	if got, want := localHeader.ExtFlags, uint8(localHostData&0x00ff); got != want {
		t.Fatalf("legacy local ExtFlags = 0x%02x, want 0x%02x", got, want)
	}
	if got, want := localHeader.ChapterNumber, uint8((localHostData>>8)&0x00ff); got != want {
		t.Fatalf("legacy local ChapterNumber = 0x%02x, want 0x%02x", got, want)
	}
	if got, want := localHeader.EncryptionVersion(), uint8(localHostData&0x00ff)&0x0f; got != want {
		t.Fatalf("legacy local EncryptionVersion = 0x%02x, want 0x%02x", got, want)
	}
}

func readFirstLocalBasicHeader(t *testing.T, archive []byte) []byte {
	t.Helper()

	mainOff := skipHeaderBlock(t, archive, 0)
	if mainOff+4 > len(archive) {
		t.Fatalf("missing local header prefix at offset %d", mainOff)
	}
	if archive[mainOff] != arjHeaderID1 || archive[mainOff+1] != arjHeaderID2 {
		t.Fatalf("invalid local header signature at offset %d", mainOff)
	}
	basicSize := int(binary.LittleEndian.Uint16(archive[mainOff+2 : mainOff+4]))
	if basicSize < arjMinFirstHeaderSize {
		t.Fatalf("local basic size = %d, want >= %d", basicSize, arjMinFirstHeaderSize)
	}
	start := mainOff + 4
	end := start + basicSize
	if end > len(archive) {
		t.Fatalf("local basic header exceeds archive bounds")
	}
	return append([]byte(nil), archive[start:end]...)
}
