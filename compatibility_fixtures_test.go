package arj

import (
	"bytes"
	"io"
	"path/filepath"
	"testing"
)

func compatibilityFixturePayload() []byte {
	return bytes.Repeat([]byte("goarj-fixture-compatibility-block-0123456789\n"), 400)
}

func TestCompatibilityFixtures(t *testing.T) {
	wantPayload := compatibilityFixturePayload()
	fixtures := []struct {
		name   string
		method uint16
	}{
		{name: "compat_method1.arj", method: Method1},
		{name: "compat_method2.arj", method: Method2},
		{name: "compat_method3.arj", method: Method3},
		{name: "compat_method4.arj", method: Method4},
	}

	for _, tc := range fixtures {
		t.Run(tc.name, func(t *testing.T) {
			rc, err := OpenReader(filepath.Join("testdata", tc.name))
			if err != nil {
				t.Fatalf("OpenReader: %v", err)
			}
			defer rc.Close()

			if got, want := len(rc.File), 1; got != want {
				t.Fatalf("file count = %d, want %d", got, want)
			}

			f := rc.File[0]
			if got, want := f.Method, tc.method; got != want {
				t.Fatalf("method = %d, want %d", got, want)
			}
			if got, want := f.UncompressedSize64, uint64(len(wantPayload)); got != want {
				t.Fatalf("uncompressed size = %d, want %d", got, want)
			}

			frc, err := f.Open()
			if err != nil {
				t.Fatalf("File.Open: %v", err)
			}
			gotPayload, err := io.ReadAll(frc)
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			if err := frc.Close(); err != nil {
				t.Fatalf("Close file reader: %v", err)
			}

			if !bytes.Equal(gotPayload, wantPayload) {
				t.Fatalf("payload mismatch")
			}
		})
	}
}
