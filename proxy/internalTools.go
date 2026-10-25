// internalTools implements proxy-managed tools that are injected into upstream
// requests and intercepted before they ever reach the client. The first one is
// view_image: the model inspects archived images (see translator.go's image
// offload) by opaque id, and the proxy serves the actual image bytes in a
// minimal side upstream request, feeding the description back as the tool
// result. This keeps multi-megabyte images out of the main conversation while
// letting the model inspect them serially, one per upstream call.
package proxy

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"kiro-go/config"

	"github.com/google/uuid"
)

const viewImageToolName = "view_image"

// maxViewImagesPerTurn bounds internal view_image executions per client request.
const maxViewImagesPerTurn = 3

// maxViewRounds bounds the hidden continuation rounds per client request, so a
// model that keeps calling view_image cannot loop upstream calls forever
// (astra audit 2026-10-25: viewRound/viewsUsed were incremented but never
// checked). When the cap hits, remaining view_image calls are swallowed and
// the turn ends normally — tool_use never leaks to the client.
const maxViewRounds = 4

// viewImageToolSpec builds the tool definition injected into upstream payloads
// when archived images are present in the conversation.
func viewImageToolSpec() KiroToolWrapper {
	spec := KiroToolWrapper{}
	spec.ToolSpecification.Name = viewImageToolName
	spec.ToolSpecification.Description = "Inspect one archived image by its image id. Use this when an IMAGE_INDEX entry says visual inspection is needed. Call at most one image per tool call, and never guess ids or paths: use the exact id from IMAGE_INDEX. The tool returns the actual image for you to look at, not a text summary."
	spec.ToolSpecification.InputSchema = InputSchema{
		JSON: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"image_id": map[string]interface{}{
					"type":        "string",
					"description": "Exact image id from IMAGE_INDEX: img_ followed by the full 64-char sha256 hex, for example img_1a2b...full-hash...9f3e",
				},
				"reason": map[string]interface{}{
					"type":        "string",
					"description": "One short sentence: what you need to verify in this image",
				},
			},
			"required":             []string{"image_id"},
			"additionalProperties": false,
		},
	}
	return spec
}

// lookupArchivedImage resolves an image id (img_<sha256-hex>) against the
// offload directory (content-addressed: <sha256-hex>.<ext>) and returns the
// image. The id must be exactly "img_" + the full 64-char sha256 hex, and the
// match against directory entries is exact — short-prefix or partial matches
// are rejected (astra audit 2026-10-25: an 8-hex prefix with HasPrefix could
// mismatch two archived images). The model never passes filesystem paths —
// only opaque ids that map inside the offload directory.
func lookupArchivedImage(id string) (KiroImage, error) {
	id = strings.ToLower(strings.TrimSpace(id))
	hash, ok := strings.CutPrefix(id, "img_")
	if !ok || len(hash) != 64 {
		return KiroImage{}, fmt.Errorf("invalid image id %q", id)
	}
	if _, err := hex.DecodeString(hash); err != nil {
		return KiroImage{}, fmt.Errorf("invalid image id %q", id)
	}
	entries, err := os.ReadDir(imageOffloadDir())
	if err != nil {
		return KiroImage{}, fmt.Errorf("offload dir unavailable: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		stem := strings.TrimSuffix(e.Name(), filepath.Ext(e.Name()))
		if stem != hash {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(imageOffloadDir(), e.Name()))
		if err != nil {
			return KiroImage{}, err
		}
		ext := strings.TrimPrefix(filepath.Ext(e.Name()), ".")
		return KiroImage{
			Format: ext,
			Source: struct {
				Bytes string `json:"bytes"`
			}{Bytes: base64.StdEncoding.EncodeToString(raw)},
		}, nil
	}
	return KiroImage{}, fmt.Errorf("image %q not found in archive", id)
}

// executeViewImageSideCall runs a minimal image-only upstream request and
// returns the model's textual description of the image. Uses a separate
// conversation id so the side call never pollutes the main conversation's
// prefix-cache, and reuses the affinity-pinned account.
func (h *Handler) executeViewImageSideCall(account *config.Account, modelID string, img KiroImage, reason string) (string, error) {
	if strings.TrimSpace(reason) == "" {
		reason = "Describe this image precisely: any text, objects, colors, layout, and anything notable."
	}
	payload := &KiroPayload{}
	payload.ConversationState.ChatTriggerType = "MANUAL"
	payload.ConversationState.AgentTaskType = "vibe"
	payload.ConversationState.ConversationID = uuid.New().String()
	payload.ConversationState.AgentContinuationId = uuid.New().String()
	payload.ConversationState.CurrentMessage.UserInputMessage = KiroUserInputMessage{
		Content: reason,
		ModelID: modelID,
		Origin:  "AI_EDITOR",
		Images:  []KiroImage{img},
	}
	payload.InferenceConfig = &InferenceConfig{MaxTokens: 1024}

	var description strings.Builder
	callback := &KiroStreamCallback{
		OnText: func(text string, isThinking bool) {
			if !isThinking {
				description.WriteString(text)
			}
		},
	}
	if err := CallKiroAPI(account, payload, callback); err != nil {
		return "", err
	}
	out := strings.TrimSpace(description.String())
	if out == "" {
		return "", fmt.Errorf("empty description from side call")
	}
	// 硬字符预算（astra 审计 2026-10-25）：MaxTokens=1024 只是软上限，模型仍可
	// 输出数千字符并进入主请求与日志；超限截断并标注。
	return truncateRunes(out, 4000), nil
}

// truncateRunes caps s at max runes, appending a visible truncation marker
// when cut. Rune-safe for multi-byte content.
func truncateRunes(s string, max int) string {
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	return string(runes[:max]) + "…[description truncated]"
}
