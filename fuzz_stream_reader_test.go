package arj

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

const (
	fuzzStreamReaderMaxInputBytes = 1 << 18
	fuzzStreamReaderMaxNextCalls  = 32
	fuzzStreamReaderMaxReadBytes  = 1 << 14
	fuzzStreamReaderMaxEntries    = 16
)

func FuzzStreamReaderWithOptions(f *testing.F) {
	for _, seed := range fuzzStreamReaderSeeds() {
		f.Add(append([]byte(nil), seed...))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > fuzzStreamReaderMaxInputBytes {
			return
		}

		defer func() {
			if recovered := recover(); recovered != nil {
				t.Fatalf("stream reader panic for %d-byte input: %v", len(data), recovered)
			}
		}()

		blob := append([]byte(nil), data...)
		opts := StreamReaderOptions{
			ParserLimits: ParserLimits{
				MaxEntries:             1 + int(fuzzStreamReaderByte(blob, 0)%fuzzStreamReaderMaxEntries),
				MaxExtendedHeaders:     1 + int(fuzzStreamReaderByte(blob, 1)%8),
				MaxExtendedHeaderBytes: int64(64 + int(fuzzStreamReaderByte(blob, 2))*256),
			},
		}

		sr, err := NewStreamReaderWithOptions(bytes.NewReader(blob), opts)
		if err != nil {
			return
		}

		passwordLen := 8
		if passwordLen > len(blob) {
			passwordLen = len(blob)
		}
		sr.SetPassword(string(blob[:passwordLen]))
		sr.SetMethod14DecodeLimits(Method14DecodeLimits{
			MaxCompressedSize:   uint64(1 + int(fuzzStreamReaderByte(blob, 3))*128),
			MaxUncompressedSize: uint64(1 + int(fuzzStreamReaderByte(blob, 4))*256),
		})

		for i := 0; i < fuzzStreamReaderMaxNextCalls; i++ {
			h, rc, err := sr.Next()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				return
			}
			if h == nil || rc == nil {
				t.Fatalf("Next(%d) returned nil header/reader", i)
			}

			if fuzzStreamReaderByte(blob, 5+i)%2 == 0 {
				readCap := int64(1 + int(fuzzStreamReaderByte(blob, 11+i)%32)*512)
				if readCap > fuzzStreamReaderMaxReadBytes {
					readCap = fuzzStreamReaderMaxReadBytes
				}
				_, _ = io.Copy(io.Discard, io.LimitReader(rc, readCap))
			}

			_ = rc.Close()
		}
	})
}

type fuzzStreamEntry struct {
	header  FileHeader
	payload []byte
}

func fuzzStreamReaderSeeds() [][]byte {
	base := fuzzBuildStreamArchiveSeed([]fuzzStreamEntry{
		{
			header: FileHeader{Name: "docs/", Method: Store, fileType: arjFileTypeDirectory},
		},
		{
			header:  FileHeader{Name: "docs/readme.txt", Method: Store},
			payload: []byte("hello stream"),
		},
		{
			header:  FileHeader{Name: "bin/data.bin", Method: Method1},
			payload: bytes.Repeat([]byte{0x00, 0xff, 0x13, 0x7a}, 1024),
		},
	})

	seeds := [][]byte{
		nil,
		{0x00},
		{arjHeaderID1},
		{arjHeaderID1, arjHeaderID2, 0x00, 0x00},
		bytes.Repeat([]byte{0xff}, 64),
	}

	if len(base) == 0 {
		return seeds
	}

	seeds = append(
		seeds,
		base,
		append(bytes.Repeat([]byte("MZ"), 24), base...),
		append(append([]byte(nil), base...), bytes.Repeat([]byte{0x00}, 32)...),
	)
	if len(base) > 1 {
		seeds = append(seeds, append([]byte(nil), base[:len(base)-1]...))
	}
	if len(base) > 8 {
		seeds = append(seeds, append([]byte(nil), base[:8]...))
	}

	mainOff, err := findMainHeaderOffset(bytes.NewReader(base), int64(len(base)))
	if err != nil {
		return seeds
	}

	if mutated, ok := fuzzParserCorruptHeaderCRC(base, mainOff); ok {
		seeds = append(seeds, mutated)
	}
	if mutated, ok := fuzzParserMutateHeaderSize(base, mainOff, 0xffff); ok {
		seeds = append(seeds, mutated)
	}

	_, _, localOff, err := readHeaderBlock(bytes.NewReader(base), int64(len(base)), mainOff)
	if err != nil {
		return seeds
	}
	if mutated, ok := fuzzParserCorruptHeaderCRC(base, localOff); ok {
		seeds = append(seeds, mutated)
	}
	if mutated, ok := fuzzParserMutateHeaderSize(base, localOff, 0xffff); ok {
		seeds = append(seeds, mutated)
	}

	return seeds
}

func fuzzBuildStreamArchiveSeed(entries []fuzzStreamEntry) []byte {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	for _, entry := range entries {
		h := entry.header
		fw, err := w.CreateHeader(&h)
		if err != nil {
			return nil
		}
		if _, err := fw.Write(entry.payload); err != nil {
			return nil
		}
	}
	if err := w.Close(); err != nil {
		return nil
	}
	return append([]byte(nil), buf.Bytes()...)
}

func fuzzStreamReaderByte(data []byte, idx int) byte {
	if len(data) == 0 {
		return byte(idx*29 + 17)
	}
	return data[idx%len(data)]
}
