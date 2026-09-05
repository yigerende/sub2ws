package service

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestOpenAIClientTransport_SetAndGet(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	require.Equal(t, OpenAIClientTransportUnknown, GetOpenAIClientTransport(c))

	SetOpenAIClientTransport(c, OpenAIClientTransportHTTP)
	require.Equal(t, OpenAIClientTransportHTTP, GetOpenAIClientTransport(c))

	SetOpenAIClientTransport(c, OpenAIClientTransportWS)
	require.Equal(t, OpenAIClientTransportWS, GetOpenAIClientTransport(c))
}

func TestOpenAIClientTransport_GetNormalizesRawContextValue(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name     string
		rawValue any
		want     OpenAIClientTransport
	}{
		{
			name:     "type_value_ws",
			rawValue: OpenAIClientTransportWS,
			want:     OpenAIClientTransportWS,
		},
		{
			name:     "http_sse_alias",
			rawValue: "http_sse",
			want:     OpenAIClientTransportHTTP,
		},
		{
			name:     "sse_alias",
			rawValue: "sSe",
			want:     OpenAIClientTransportHTTP,
		},
		{
			name:     "websocket_alias",
			rawValue: "WebSocket",
			want:     OpenAIClientTransportWS,
		},
		{
			name:     "invalid_string",
			rawValue: "tcp",
			want:     OpenAIClientTransportUnknown,
		},
		{
			name:     "invalid_type",
			rawValue: 123,
			want:     OpenAIClientTransportUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Set(openAIClientTransportContextKey, tt.rawValue)
			require.Equal(t, tt.want, GetOpenAIClientTransport(c))
		})
	}
}

func TestOpenAIClientTransport_NilAndUnknownInput(t *testing.T) {
	SetOpenAIClientTransport(nil, OpenAIClientTransportHTTP)
	require.Equal(t, OpenAIClientTransportUnknown, GetOpenAIClientTransport(nil))

	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	SetOpenAIClientTransport(c, OpenAIClientTransportUnknown)
	_, exists := c.Get(openAIClientTransportContextKey)
	require.False(t, exists)

	SetOpenAIClientTransport(c, OpenAIClientTransport("   "))
	_, exists = c.Get(openAIClientTransportContextKey)
	require.False(t, exists)
}

func TestResolveOpenAIWSDecisionForRequest(t *testing.T) {
	base := OpenAIWSProtocolDecision{
		Transport: OpenAIUpstreamTransportResponsesWebsocketV2,
		Reason:    "ws_v2_enabled",
	}
	enabledAccount := &Account{
		Platform: PlatformOpenAI,
		Type:     AccountTypeAPIKey,
		Extra: map[string]any{
			"openai_apikey_responses_websockets_v2_mode":    OpenAIWSIngressModeCtxPool,
			"openai_apikey_responses_websockets_v2_enabled": true,
		},
	}
	disabledAccount := &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey}

	responsesDecision := resolveOpenAIWSDecisionForRequest(base, OpenAIClientTransportHTTP, enabledAccount, false)
	require.Equal(t, base, responsesDecision)
	for _, accountType := range []string{AccountTypeOAuth, AccountTypeSetupToken} {
		oauthAccount := &Account{
			Platform: PlatformOpenAI,
			Type:     accountType,
			Extra: map[string]any{
				"openai_oauth_responses_websockets_v2_mode":    OpenAIWSIngressModeCtxPool,
				"openai_oauth_responses_websockets_v2_enabled": true,
			},
		}
		require.Equal(
			t,
			base,
			resolveOpenAIWSDecisionForRequest(base, OpenAIClientTransportHTTP, oauthAccount, false),
			"account type %s should allow HTTP ingress to use upstream WS when explicitly enabled",
			accountType,
		)
	}

	disabledDecision := resolveOpenAIWSDecisionForRequest(base, OpenAIClientTransportHTTP, disabledAccount, false)
	require.Equal(t, OpenAIUpstreamTransportHTTPSSE, disabledDecision.Transport)
	require.Equal(t, "account_http_ws_disabled", disabledDecision.Reason)

	compactDecision := resolveOpenAIWSDecisionForRequest(base, OpenAIClientTransportHTTP, enabledAccount, true)
	require.Equal(t, OpenAIUpstreamTransportHTTPSSE, compactDecision.Transport)
	require.Equal(t, "responses_compact_requires_http", compactDecision.Reason)
}
