package arj

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const interopARJCommandTimeout = 30 * time.Second
const interopMaxARJOutputBytes = 1 << 20

func runInteropARJCommand(t *testing.T, arjPath string, args ...string) []byte {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), interopARJCommandTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, arjPath, args...)
	cmd.Env = interopARJEnv()
	out := newInteropCappedOutputBuffer(interopMaxARJOutputBytes)
	cmd.Stdout = out
	cmd.Stderr = out
	err := cmd.Run()
	if ctx.Err() == context.DeadlineExceeded {
		t.Fatalf(
			"arj command timed out after %s\nargs: %s\noutput:\n%s",
			interopARJCommandTimeout,
			strings.Join(cmd.Args, " "),
			out.String(),
		)
	}
	if err != nil {
		t.Fatalf("arj command failed (%v)\nargs: %s\noutput:\n%s", err, strings.Join(cmd.Args, " "), out.String())
	}
	return out.Bytes()
}

type interopCappedOutputBuffer struct {
	max       int
	buf       bytes.Buffer
	truncated bool
}

func newInteropCappedOutputBuffer(max int) *interopCappedOutputBuffer {
	if max <= 0 {
		max = 1
	}
	return &interopCappedOutputBuffer{max: max}
}

func (b *interopCappedOutputBuffer) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if b.buf.Len() >= b.max {
		b.truncated = true
		return len(p), nil
	}
	remaining := b.max - b.buf.Len()
	if len(p) > remaining {
		_, _ = b.buf.Write(p[:remaining])
		b.truncated = true
		return len(p), nil
	}
	_, _ = b.buf.Write(p)
	return len(p), nil
}

func (b *interopCappedOutputBuffer) String() string {
	if b == nil {
		return ""
	}
	if !b.truncated {
		return b.buf.String()
	}
	return fmt.Sprintf("%s\n[output truncated at %d bytes]", b.buf.String(), b.max)
}

func (b *interopCappedOutputBuffer) Bytes() []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b.buf.Bytes()...)
}
