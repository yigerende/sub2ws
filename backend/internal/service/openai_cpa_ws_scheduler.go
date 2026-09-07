package service

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
)

func cpaWSStableID(kind string, values ...string) string {
	seed := "sub2api:cpa-ws:v1:" + strings.TrimSpace(kind) + ":" + strings.Join(values, "\x00")
	sum := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(sum[:16])
}

func cpaWSCanonicalModelFamily(model string) string {
	model = strings.ToLower(strings.TrimSpace(model))
	if model == "" {
		return "unknown"
	}
	return model
}

// cpaWSRouteKey is used only for soft conversation affinity. It is deliberately
// independent from prompt caching and the physical WebSocket pool.
func cpaWSRouteKey(accountID int64, sessionHash string) string {
	if accountID <= 0 || strings.TrimSpace(sessionHash) == "" {
		return ""
	}
	return cpaWSStableID("route", strconv.FormatInt(accountID, 10), strings.TrimSpace(sessionHash))
}

// cpaWSUpstreamPromptCacheKey preserves an explicit client key. When it is
// absent, a stable conversation/model key gives OpenAI the same cache identity
// across turns without coupling it to a request ID or a physical connection.
func cpaWSUpstreamPromptCacheKey(explicitKey, sessionHash, model string) string {
	if key := strings.TrimSpace(explicitKey); key != "" {
		return key
	}
	if strings.TrimSpace(sessionHash) == "" {
		return ""
	}
	return "cpa_" + cpaWSStableID("prompt-cache", cpaWSCanonicalModelFamily(model), strings.TrimSpace(sessionHash))
}

// cpaWSPoolAffinity deliberately excludes route and prompt-cache identities.
// The account pool already provides credential isolation; model family is the
// remaining physical slot boundary, matching CPA's stateless WS pool design.
func cpaWSPoolAffinity(model string) string {
	return "cpa_pool_" + cpaWSStableID("pool", cpaWSCanonicalModelFamily(model))
}
