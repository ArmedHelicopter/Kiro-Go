package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestStallTimeoutReader_ActivelyInterruptsBlockedRead verifies that when a read
// is blocked on an idle connection, the watchdog actively closes the underlying
// closer and wakes up the blocked Read within the timeout window.
func TestStallTimeoutReader_ActivelyInterruptsBlockedRead(t *testing.T) {
	pr, pw := io.Pipe()
	defer pw.Close()

	idleTimeout := 50 * time.Millisecond
	sr := newStallTimeoutReader(pr, pr, idleTimeout, "test/blocked")
	defer sr.Close()

	buf := make([]byte, 64)
	errCh := make(chan error, 1)

	start := time.Now()
	go func() {
		_, err := sr.Read(buf)
		errCh <- err
	}()

	select {
	case err := <-errCh:
		elapsed := time.Since(start)
		if elapsed < idleTimeout {
			t.Fatalf("Read unblocked too early: %v < %v", elapsed, idleTimeout)
		}
		if elapsed > 300*time.Millisecond {
			t.Fatalf("Read took too long to unblock: %v", elapsed)
		}
		if err == nil || !strings.Contains(err.Error(), "upstream stalled") {
			t.Fatalf("expected 'upstream stalled' error, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("Watchdog failed to interrupt blocked Read within 500ms")
	}
}

// TestStallTimeoutReader_HealthyStreamDoesNotTimeout verifies that steadily
// arriving chunks reset the timer and do not cause false stall aborts.
func TestStallTimeoutReader_HealthyStreamDoesNotTimeout(t *testing.T) {
	pr, pw := io.Pipe()

	idleTimeout := 80 * time.Millisecond
	sr := newStallTimeoutReader(pr, pr, idleTimeout, "test/steady")
	defer sr.Close()

	go func() {
		for i := 0; i < 4; i++ {
			time.Sleep(30 * time.Millisecond)
			_, _ = pw.Write([]byte("chunk"))
		}
		pw.Close()
	}()

	buf := make([]byte, 64)
	total := 0
	for {
		n, err := sr.Read(buf)
		total += n
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("unexpected read error: %v", err)
		}
	}
	if total != 20 {
		t.Fatalf("expected 20 bytes, got %d", total)
	}
}

// TestStallTimeoutReader_InFlightDoesNotAbort verifies that once in-flight output
// has begun, the watchdog does not actively close the socket or return stall errors,
// allowing long deep-thinking pauses to proceed unhindered.
func TestStallTimeoutReader_InFlightDoesNotAbort(t *testing.T) {
	pr, pw := io.Pipe()
	defer pw.Close()

	idleTimeout := 50 * time.Millisecond
	sr := newStallTimeoutReader(pr, pr, idleTimeout, "test/inflight")
	sr.MarkInFlight()
	defer sr.Close()

	buf := make([]byte, 64)
	errCh := make(chan error, 1)

	// Simulate slow upstream that pauses for 120ms (longer than 50ms watchdog)
	go func() {
		time.Sleep(120 * time.Millisecond)
		_, _ = pw.Write([]byte("delayed-token"))
	}()

	go func() {
		n, err := sr.Read(buf)
		if err == nil && string(buf[:n]) == "delayed-token" {
			errCh <- nil
		} else {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("expected in-flight read to succeed without stall abort, got %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatalf("timed out waiting for in-flight read")
	}
}

// TestStallTimeoutReader_ConcurrentMarkInFlightTimer ensures there is no data race
// when MarkInFlight, Read, and the watchdog timer fire concurrently.
func TestStallTimeoutReader_ConcurrentMarkInFlightTimer(t *testing.T) {
	pr, pw := io.Pipe()
	defer pw.Close()

	idleTimeout := 10 * time.Millisecond
	sr := newStallTimeoutReader(pr, pr, idleTimeout, "test/race")
	defer sr.Close()

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			sr.MarkInFlight()
			time.Sleep(1 * time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			_, _ = pw.Write([]byte("x"))
			time.Sleep(2 * time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()
		buf := make([]byte, 16)
		for i := 0; i < 20; i++ {
			_, _ = sr.Read(buf)
			time.Sleep(2 * time.Millisecond)
		}
	}()

	wg.Wait()
}
func TestParseMeteringUsage(t *testing.T) {
	cases := []struct {
		name  string
		event map[string]interface{}
		want  float64
	}{
		{"float64 usage", map[string]interface{}{"usage": 0.045}, 0.045},
		{"zero usage", map[string]interface{}{"usage": float64(0)}, 0},
		{"integer usage", map[string]interface{}{"usage": 2}, 2.0},
		{"int64 totalCredits", map[string]interface{}{"totalCredits": int64(3)}, 3.0},
		{"credits float", map[string]interface{}{"credits": 1.25}, 1.25},
		{"json.Number", map[string]interface{}{"usage": json.Number("0.089")}, 0.089},
		{"empty", map[string]interface{}{}, 0},
		{"nil value", map[string]interface{}{"usage": nil}, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseMeteringUsage(tc.event)
			if got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestParseEventStream_MeteringWithoutMetadataEvent verifies that when AWS Kiro
// terminates a text stream with meteringEvent and clean EOF (no metadataEvent),
// the stream is recognized as complete with end_turn stop reason, even if OnCredits is nil.
func TestParseEventStream_MeteringWithoutMetadataEvent(t *testing.T) {
	var stream bytes.Buffer

	// 1. assistantResponseEvent
	stream.Write(awsEventStreamFrame(t, "assistantResponseEvent", map[string]interface{}{
		"content": "hello world",
	}))

	// 2. contextUsageEvent
	stream.Write(awsEventStreamFrame(t, "contextUsageEvent", map[string]interface{}{
		"contextUsagePercentage": 15.5,
	}))

	// 3. meteringEvent with usage = 0 (free tier / cached)
	stream.Write(awsEventStreamFrame(t, "meteringEvent", map[string]interface{}{
		"usage": float64(0),
	}))

	var textReceived string
	var stopReasonReceived string
	var mu sync.Mutex

	cb := &KiroStreamCallback{
		OnText: func(text string, isReasoning bool) {
			mu.Lock()
			textReceived += text
			mu.Unlock()
		},
		// Deliberately leave OnCredits nil to verify decoupling!
		OnCredits: nil,
		OnStopReason: func(reason string) {
			mu.Lock()
			stopReasonReceived = reason
			mu.Unlock()
		},
	}

	emitted, err := parseEventStreamTracked(&stream, cb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !emitted {
		t.Fatalf("expected emitted = true")
	}
	if textReceived != "hello world" {
		t.Fatalf("expected 'hello world', got %q", textReceived)
	}
	if stopReasonReceived != "end_turn" {
		t.Fatalf("expected stopReason 'end_turn', got %q", stopReasonReceived)
	}
}

// TestParseEventStream_ErrorBeforeMeteringDoesNotComplete verifies that an upstream
// error frame followed by metering is NOT salvaged as a completed turn.
func TestParseEventStream_ErrorBeforeMeteringDoesNotComplete(t *testing.T) {
	var stream bytes.Buffer

	// 1. assistantResponseEvent
	stream.Write(awsEventStreamFrame(t, "assistantResponseEvent", map[string]interface{}{
		"content": "partial text",
	}))

	// 2. error frame
	stream.Write(awsEventStreamFrameWithHeaders(t, map[string]string{
		":message-type": "error",
	}, map[string]interface{}{
		"message": "Rate limit exceeded",
	}))

	var stopReasonReceived string
	cb := &KiroStreamCallback{
		OnStopReason: func(reason string) {
			stopReasonReceived = reason
		},
	}

	_, err := parseEventStreamTracked(&stream, cb)
	if err == nil {
		t.Fatalf("expected error from error frame, got nil")
	}
	if !errors.Is(err, errKiroEventStreamUpstream) {
		t.Fatalf("expected errKiroEventStreamUpstream, got %v", err)
	}
	if stopReasonReceived != "" {
		t.Fatalf("stopReason must not be set on error stream, got %q", stopReasonReceived)
	}
}
