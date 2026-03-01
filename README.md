# goarj

`goarj` is a pure Go package for reading and writing ARJ archives, with no cgo dependency.

It tries to feel close to `archive/zip`, so most of the flow is familiar:
`NewWriter` -> `Create` -> write bytes -> `Close`, and `NewReader`/`OpenReader` on the read side.

## Features

- Read ARJ files from disk (`OpenReader`) or any `io.ReaderAt` (`NewReader`).
- Write ARJ files to any `io.Writer` (`NewWriter`).
- `fs.FS` style access on archives via `Reader.Open`, `ReadDir`, `WalkDir`.
- Safer extraction helpers: `ExtractAll`, `ExtractAllWithOptions`, `SafeExtractPath`.
- Multi-volume support for both split reads and writes (`OpenMultiReader`, `NewMultiVolumeWriter`).
- Add whole directory trees with `AddFS`.

## CLI

A minimal command-line tool is available at `cmd/goarj`.
It archives file entries using ARJ `Method4` compression, and is just meant for testing.

Archive a file or directory:

```sh
go run ./cmd/goarj archive backup.arj ./data
```

Extract an archive:

```sh
go run ./cmd/goarj extract backup.arj ./out
```

## Quick usage

### Write an archive

```go
package main

import (
	"bytes"
	"io"
	"log"

	arj "github.com/urtie/goarj"
)

func main() {
	var buf bytes.Buffer
	w := arj.NewWriter(&buf)

	fw, err := w.Create("hello.txt")
	if err != nil {
		log.Fatal(err)
	}
	if _, err := io.WriteString(fw, "hello from goarj\n"); err != nil {
		log.Fatal(err)
	}

	if err := w.Close(); err != nil {
		log.Fatal(err)
	}

	_ = buf.Bytes() // ARJ payload
}
```

### Read an archive

```go
package main

import (
	"bytes"
	"io"
	"log"

	arj "github.com/urtie/goarj"
)

func main() {
	archiveBytes := []byte{/* ... ARJ data ... */}
	r, err := arj.NewReader(bytes.NewReader(archiveBytes), int64(len(archiveBytes)))
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range r.File {
		rc, err := f.Open()
		if err != nil {
			log.Fatal(err)
		}
		_, err = io.Copy(io.Discard, rc)
		closeErr := rc.Close()
		if err != nil || closeErr != nil {
			log.Fatal(err, closeErr)
		}
	}
}
```

### Extract to disk (safe-by-default, with limits)

```go
package main

import (
	"log"

	arj "github.com/urtie/goarj"
)

func main() {
	r, err := arj.OpenReader("input.arj")
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()

	// Default quotas are applied by ExtractAll.
	if err := r.ExtractAll("./out"); err != nil {
		log.Fatal(err)
	}
}
```

### Multi-volume write + read

```go
package main

import (
	"io"
	"log"
	"os"

	arj "github.com/urtie/goarj"
)

func main() {
	mw, err := arj.NewMultiVolumeWriter("backup.arj", arj.MultiVolumeWriterOptions{
		VolumeSize: 8 << 20, // 8 MiB per part
	})
	if err != nil {
		log.Fatal(err)
	}

	fw, err := mw.Create("big.bin")
	if err != nil {
		log.Fatal(err)
	}

	src, err := os.Open("big.bin")
	if err != nil {
		log.Fatal(err)
	}
	defer src.Close()

	if _, err := io.Copy(fw, src); err != nil {
		log.Fatal(err)
	}
	if err := mw.Close(); err != nil {
		log.Fatal(err)
	}

	log.Printf("parts: %v", mw.Parts())

	mr, err := arj.OpenMultiReader("backup.arj")
	if err != nil {
		log.Fatal(err)
	}
	defer mr.Close()

	rc, err := mr.Open("big.bin")
	if err != nil {
		log.Fatal(err)
	}
	defer rc.Close()
}
```

### Password-protected entries

```go
r, err := arj.OpenReader("secret.arj")
if err != nil {
	log.Fatal(err)
}
defer r.Close()

r.SetPassword("password123")
rc, err := r.File[0].Open() // uses reader default password
if err != nil {
	log.Fatal(err)
}
defer rc.Close()
```

Or per-file:

```go
rc, err := r.File[0].OpenWithPassword("password123")
```
