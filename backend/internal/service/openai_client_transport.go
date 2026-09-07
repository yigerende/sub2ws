package service

import (
	"strings"

	"github.com/gin-gonic/gin"
)

// OpenAIClientTransport 表示客户端入站协议类型。
type OpenAIClientTransport string

const (
	OpenAIClientTransportUnknown OpenAIClientTransport = ""
	OpenAIClientTransportHTTP    OpenAIClientTransport = "http"
	OpenAIClientTransportWS      OpenAIClientTransport = "ws"
)

const openAIClientTransportContextKey = "openai_client_transport"

// SetOpenAIClientTransport 标记当前请求的客户端入站协议。
func SetOpenAIClientTransport(c *gin.Context, transport OpenAIClientTransport) {
	if c == nil {
		return
	}
	normalized := normalizeOpenAIClientTransport(transport)
	if normalized == OpenAIClientTransportUnknown {
		return
	}
	c.Set(openAIClientTransportContextKey, string(normalized))
}

// GetOpenAIClientTransport 读取当前请求的客户端入站协议。
func GetOpenAIClientTransport(c *gin.Context) OpenAIClientTransport {
	if c == nil {
		return OpenAIClientTransportUnknown
	}
	raw, ok := c.Get(openAIClientTransportContextKey)
	if !ok || raw == nil {
		return OpenAIClientTransportUnknown
	}

	switch v := raw.(type) {
	case OpenAIClientTransport:
		return normalizeOpenAIClientTransport(v)
	case string:
		return normalizeOpenAIClientTransport(OpenAIClientTransport(v))
	default:
		return OpenAIClientTransportUnknown
	}
}

func normalizeOpenAIClientTransport(transport OpenAIClientTransport) OpenAIClientTransport {
	switch strings.ToLower(strings.TrimSpace(string(transport))) {
	case string(OpenAIClientTransportHTTP), "http_sse", "sse":
		return OpenAIClientTransportHTTP
	case string(OpenAIClientTransportWS), "websocket":
		return OpenAIClientTransportWS
	default:
		return OpenAIClientTransportUnknown
	}
}

// resolveOpenAIWSDecisionForRequest applies endpoint-level transport constraints.
// An HTTP client is not itself a reason to downgrade an explicitly enabled
// account: HTTP/SSE clients can use the native upstream WebSocket pool as well.
func resolveOpenAIWSDecisionForRequest(
	decision OpenAIWSProtocolDecision,
	clientTransport OpenAIClientTransport,
	account *Account,
	compactPath bool,
) OpenAIWSProtocolDecision {
	if compactPath {
		return openAIWSHTTPDecision("responses_compact_requires_http")
	}
	if decision.Transport == OpenAIUpstreamTransportHTTPSSE {
		return decision
	}
	if clientTransport == OpenAIClientTransportHTTP {
		if decision.Transport == OpenAIUpstreamTransportResponsesWebsocketCPA {
			return decision
		}
		// A global ctx_pool default must not opt existing accounts into HTTP -> WS.
		// Only an explicit account mode/legacy enabled flag may change HTTP ingress.
		mode := account.ResolveOpenAIResponsesWebSocketV2Mode(OpenAIWSIngressModeOff)
		if mode == OpenAIWSIngressModeOff {
			return openAIWSHTTPDecision("account_http_ws_disabled")
		}
		if mode == OpenAIWSIngressModeHTTPBridge {
			return openAIWSHTTPDecision("ws_v2_mode_http_bridge")
		}
	}
	return decision
}
