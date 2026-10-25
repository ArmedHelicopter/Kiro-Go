package proxy

import (
	"strings"
	"testing"

	"kiro-go/config"
)

// TestCallKiroAPIProfileFailureIsClassifiable pins the production-incident
// fix (2026-10-25): when an account's profile ARN cannot be resolved,
// CallKiroAPI must fail with an error the account-failover classifier
// recognizes ("no available Kiro profile") instead of sending an ARN-less
// request that upstream rejects with an unclassifiable INVALID_MODEL_ID —
// which kept affinity-pinned sessions looping on the broken account.
//
// This file is named so the test runs after kiro_test.go: the transport test
// initializes Go's process-wide proxy-environment cache first, and the
// black-hole proxy below makes the resolver's connection attempts fail fast
// instead of waiting on DNS timeouts.
func TestCallKiroAPIProfileFailureIsClassifiable(t *testing.T) {
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")

	account := &config.Account{Email: "broken@example.com"} // no stored ProfileArn; upstream unreachable
	payload := &KiroPayload{}
	payload.ConversationState.ChatTriggerType = "MANUAL"
	payload.ConversationState.CurrentMessage.UserInputMessage = KiroUserInputMessage{
		Content: "hi",
		ModelID: "claude-opus-5.5",
		Origin:  "AI_EDITOR",
	}

	err := CallKiroAPI(account, payload, nil)
	if err == nil {
		t.Fatal("expected an error when profile ARN cannot be resolved")
	}
	if !isProfileUnavailableErrorMessage(err.Error()) {
		t.Fatalf("error must be classifiable as profile-unavailable so the caller rotates accounts, got: %v", err)
	}
	if !strings.Contains(err.Error(), "broken@example.com") {
		t.Fatalf("error should identify the failing account, got: %v", err)
	}
	// The payload must not be mutated with a stale/partial ARN.
	if strings.TrimSpace(payload.ProfileArn) != "" {
		t.Fatalf("payload ProfileArn should stay empty on failure, got %q", payload.ProfileArn)
	}
}
