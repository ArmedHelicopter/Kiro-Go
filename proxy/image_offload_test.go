package proxy

import (
	"encoding/base64"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// smallImage is a tiny valid base64 payload (content is irrelevant to the
// archiver: it only decodes base64 and whitelists the extension).
const smallImageB64 = "aGVsbG8taW1hZ2UtYnl0ZXM="

func testKiroImage(b64 string) KiroImage {
	return KiroImage{Format: "png", Source: struct {
		Bytes string `json:"bytes"`
	}{Bytes: b64}}
}

func TestArchiveKiroImageReturnsStableOpaqueID(t *testing.T) {
	dir := t.TempDir()
	img := testKiroImage(smallImageB64)

	path, id, err := archiveKiroImage(img, dir)
	if err != nil {
		t.Fatalf("archive failed: %v", err)
	}
	if !strings.HasPrefix(id, "img_") {
		t.Fatalf("id %q missing img_ prefix", id)
	}
	hash := strings.TrimPrefix(id, "img_")
	if len(hash) != 64 {
		t.Fatalf("id hash length = %d, want 64 (full sha256)", len(hash))
	}
	if _, err := hex.DecodeString(hash); err != nil {
		t.Fatalf("id hash not hex: %v", err)
	}
	// Content-addressed: the id hash must equal the file stem.
	stem := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	if stem != hash {
		t.Fatalf("file stem %q != id hash %q", stem, hash)
	}

	// Dedupe: archiving identical bytes twice must reuse the same file.
	path2, id2, err := archiveKiroImage(img, dir)
	if err != nil {
		t.Fatalf("second archive failed: %v", err)
	}
	if path2 != path || id2 != id {
		t.Fatalf("dedupe broken: got (%q,%q), want (%q,%q)", path2, id2, path, id)
	}
}

func TestLookupArchivedImageExactMatchOnly(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("KIRO_IMAGE_OFFLOAD_DIR", dir)
	_, id, err := archiveKiroImage(testKiroImage(smallImageB64), dir)
	if err != nil {
		t.Fatalf("archive failed: %v", err)
	}

	// Valid full id resolves and round-trips the bytes.
	img, err := lookupArchivedImage(id)
	if err != nil {
		t.Fatalf("lookup failed: %v", err)
	}
	raw, _ := base64.StdEncoding.DecodeString(smallImageB64)
	got, err := base64.StdEncoding.DecodeString(img.Source.Bytes)
	if err != nil || string(got) != string(raw) {
		t.Fatalf("round-trip mismatch: %q vs %q (err=%v)", got, raw, err)
	}

	// Uppercase id is accepted (normalized to lowercase).
	if _, err := lookupArchivedImage(strings.ToUpper(id)); err != nil {
		t.Fatalf("uppercase id should resolve: %v", err)
	}

	// Short/prefix ids are rejected (collision surface — astra audit).
	if _, err := lookupArchivedImage(strings.TrimPrefix(id, "img_")[:8]); err == nil {
		t.Fatal("bare 8-hex prefix should be rejected")
	}
	if _, err := lookupArchivedImage("img_" + strings.TrimPrefix(id, "img_")[:8]); err == nil {
		t.Fatal("8-hex id should be rejected; exact full sha256 required")
	}
	// Non-hex and unknown-but-well-formed ids are rejected.
	if _, err := lookupArchivedImage("img_" + strings.Repeat("z", 64)); err == nil {
		t.Fatal("non-hex id should be rejected")
	}
	if _, err := lookupArchivedImage("img_" + strings.Repeat("ab", 32)); err == nil {
		t.Fatal("unknown id should be reported as not found")
	}
	if _, err := lookupArchivedImage(""); err == nil {
		t.Fatal("empty id should be rejected")
	}
}

func TestApplyCurrentImageBudgetOpaqueIndex(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("KIRO_IMAGE_OFFLOAD_DIR", dir)

	huge := base64.StdEncoding.EncodeToString(make([]byte, 600*1024)) // > 512KB inline cap
	kept, index := applyCurrentImageBudget([]KiroImage{
		testKiroImage(smallImageB64), // small: stays inline
		testKiroImage(huge),          // oversized: archived
	})
	if len(kept) != 1 {
		t.Fatalf("expected 1 inline image, got %d", len(kept))
	}
	if !strings.Contains(index, "[IMAGE_INDEX]") || !strings.Contains(index, "img_") {
		t.Fatalf("index missing IMAGE_INDEX/id block:\n%s", index)
	}
	// No filesystem paths may leak into the model-visible index.
	if strings.Contains(index, "path:") || strings.Contains(index, string(filepath.Separator)) {
		t.Fatalf("index leaks a filesystem path:\n%s", index)
	}
	// The archived file must exist on disk (content-addressed).
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("expected exactly 1 archived file, got %d (err=%v)", len(entries), err)
	}
}

func TestApplyCurrentImageBudgetKeepsUnparseableInline(t *testing.T) {
	t.Setenv("KIRO_IMAGE_OFFLOAD_DIR", t.TempDir())
	kept, index := applyCurrentImageBudget([]KiroImage{testKiroImage("not-base64!!!")})
	if len(kept) != 1 || index != "" {
		t.Fatalf("unparseable image must stay inline: kept=%d index=%q", len(kept), index)
	}
}

func TestOffloadHistoryImagesReplacesWithPathFreeReference(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("KIRO_IMAGE_OFFLOAD_DIR", dir)
	EnableHistoryImageOffload()
	t.Cleanup(disableHistoryImageOffload)

	payload := &KiroPayload{}
	payload.ConversationState.History = []KiroHistoryMessage{
		{UserInputMessage: &KiroUserInputMessage{
			Content: "earlier turn",
			Images:  []KiroImage{testKiroImage(smallImageB64)},
		}},
	}

	count := offloadHistoryImages(payload)
	if count != 1 {
		t.Fatalf("expected 1 archived history image, got %d", count)
	}
	h := payload.ConversationState.History[0]
	if len(h.UserInputMessage.Images) != 0 {
		t.Fatalf("history image should be removed from payload, got %d", len(h.UserInputMessage.Images))
	}
	if !strings.Contains(h.UserInputMessage.Content, "[image archived: img_") {
		t.Fatalf("missing opaque id reference:\n%s", h.UserInputMessage.Content)
	}
	// The reference must be path-free (clients cannot resolve proxy-relative
	// paths — production incident 2026-10-25) — only the opaque id is exposed.
	if strings.Contains(h.UserInputMessage.Content, "viewable-images") || strings.Contains(h.UserInputMessage.Content, "read_image") {
		t.Fatalf("reference leaks path/tool expectations:\n%s", h.UserInputMessage.Content)
	}
}

func TestTruncateRunes(t *testing.T) {
	if got := truncateRunes("short", 10); got != "short" {
		t.Fatalf("short string must pass through, got %q", got)
	}
	long := strings.Repeat("汉", 5000)
	const marker = "…[description truncated]"
	got := truncateRunes(long, 4000)
	// Exact equality against a rune-boundary cut also proves multi-byte
	// safety: no replacement garbage can match byte-for-byte.
	if got != string([]rune(long)[:4000])+marker {
		t.Fatalf("truncation result mismatch: got %d runes ending %q", len([]rune(got)), got[len(got)-60:])
	}
}
