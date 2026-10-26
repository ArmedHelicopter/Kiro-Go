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

// TestParseMeteringUsage verifies robust extraction of usage across schema variants.
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
