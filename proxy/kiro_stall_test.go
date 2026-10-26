package proxy

import (
	"io"
	"strings"
	"testing"
	"time"
)

// TestStallTimeoutReaderAbortsSilentUpstream pins the 5-minute-hang fix
// (production incident 2026-10-25): a connected-but-silent upstream must
// produce a read error after the idle window instead of blocking forever.
func TestStallTimeoutReaderAbortsSilentUpstream(t *testing.T) {
	// A reader that never returns data and never returns EOF.
	silent := &silentReader{}
	sr := newStallTimeoutReader(silent, 150*time.Millisecond, "test/stall")

	// Mirror parseEventStream's io.ReadFull loop: keep reading until an error.
	start := time.Now()
	buf := make([]byte, 16)
	var stallErr error
	for {
		_, err := sr.Read(buf)
		if err != nil {
			stallErr = err
			break
		}
		if time.Since(start) > 2*time.Second {
			t.Fatal("no stall error within 2s")
		}
	}
	elapsed := time.Since(start)
	if !strings.Contains(stallErr.Error(), "upstream stalled") {
		t.Fatalf("unexpected stall error: %v", stallErr)
	}
	if elapsed > time.Second {
		t.Fatalf("stall abort took too long: %v", elapsed)
	}
}

// TestStallTimeoutReaderPassesDataAndRearms verifies steady streams are never
// interrupted: slow-but-alive readers keep resetting the watchdog.
func TestStallTimeoutReaderPassesDataAndRearms(t *testing.T) {
	data := strings.Repeat("chunk;", 50) // 350 bytes delivered in small slices
	sr := newStallTimeoutReader(strings.NewReader(data), 120*time.Millisecond, "test/steady")

	var got strings.Builder
	buf := make([]byte, 7) // small reads spaced by reader latency
	for {
		n, err := sr.Read(buf)
		got.Write(buf[:n])
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("steady stream must not stall: %v", err)
		}
		time.Sleep(30 * time.Millisecond) // < idle window: watchdog keeps re-arming
	}
	if got.String() != data {
		t.Fatalf("data mismatch: got %d bytes, want %d", got.Len(), len(data))
	}
}

type silentReader struct{}

func (silentReader) Read([]byte) (int, error) {
	time.Sleep(10 * time.Millisecond)
	return 0, nil // no data, no EOF: a true stall
}
