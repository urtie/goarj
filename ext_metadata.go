package arj

import "encoding/binary"

const archiveHeaderSecurityExtraSize = 4

func packExtMetadata(extFlags, chapterNumber uint8) uint16 {
	return uint16(extFlags) | uint16(chapterNumber)<<8
}

func unpackExtMetadata(hostData uint16) (extFlags, chapterNumber uint8) {
	return uint8(hostData), uint8(hostData >> 8)
}

func syncArchiveHeaderExtMetadata(h *ArchiveHeader) {
	if h.ExtFlags == 0 && h.ChapterNumber == 0 && h.HostData != 0 {
		h.ExtFlags, h.ChapterNumber = unpackExtMetadata(h.HostData)
	}
	h.HostData = packExtMetadata(h.ExtFlags, h.ChapterNumber)
}

func unpackArchiveSecurityExtra(extra []byte) (blocks, flags uint8, reserved uint16) {
	if len(extra) < archiveHeaderSecurityExtraSize {
		return 0, 0, 0
	}
	return extra[0], extra[1], binary.LittleEndian.Uint16(extra[2:4])
}

func syncArchiveHeaderSecurityMetadata(h *ArchiveHeader) {
	if len(h.FirstHeaderExtra) < archiveHeaderSecurityExtraSize {
		return
	}
	if h.ProtectionBlocks == 0 && h.ProtectionFlags == 0 && h.ProtectionReserved == 0 {
		h.ProtectionBlocks, h.ProtectionFlags, h.ProtectionReserved = unpackArchiveSecurityExtra(h.FirstHeaderExtra)
	}
	h.FirstHeaderExtra[0] = h.ProtectionBlocks
	h.FirstHeaderExtra[1] = h.ProtectionFlags
	binary.LittleEndian.PutUint16(h.FirstHeaderExtra[2:4], h.ProtectionReserved)
}

func syncFileHeaderExtMetadata(h *FileHeader) {
	if h.ExtFlags == 0 && h.ChapterNumber == 0 && h.HostData != 0 {
		h.ExtFlags, h.ChapterNumber = unpackExtMetadata(h.HostData)
	}
	h.HostData = packExtMetadata(h.ExtFlags, h.ChapterNumber)
}
