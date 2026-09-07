package service

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	coderws "github.com/coder/websocket"
	"go.uber.org/zap"
)

const (
	openAIWSBetaV1Value = "responses_websockets=2026-02-04"
	openAIWSBetaV2Value = "responses_websockets=2026-02-06"

	openAIWSTurnStateHeader    = "x-codex-turn-state"
	openAIWSTurnMetadataHeader = "x-codex-turn-metadata"

	openAIWSLogValueMaxLen      = 160
	openAIWSHeaderValueMaxLen   = 120
	openAIWSIDValueMaxLen       = 64
	openAIWSEventLogHeadLimit   = 20
	openAIWSEventLogEveryN      = 50
	openAIWSBufferLogHeadLimit  = 8
	openAIWSBufferLogEveryN     = 20
	openAIWSPrewarmEventLogHead = 10
	openAIWSPayloadKeySizeTopN  = 6

	openAIWSPayloadSizeEstimateDepth    = 3
	openAIWSPayloadSizeEstimateMaxBytes = 64 * 1024
	openAIWSPayloadSizeEstimateMaxItems = 16

	openAIWSEventFlushBatchSizeDefault    = 4
	openAIWSEventFlushIntervalDefault     = 25 * time.Millisecond
	openAIWSPayloadLogSampleDefault       = 0.2
	openAIWSPassthroughIdleTimeoutDefault = time.Hour

	openAIWSStoreDisabledConnModeStrict   = "strict"
	openAIWSStoreDisabledConnModeAdaptive = "adaptive"
	openAIWSStoreDisabledConnModeOff      = "off"

	openAIWSIngressStagePreviousResponseNotFound = "previous_response_not_found"
	openAIWSMaxPrevResponseIDDeletePasses        = 8
)

var openAIWSLogValueReplacer = strings.NewReplacer(
	"error", "err",
	"fallback", "fb",
	"warning", "warnx",
	"failed", "fail",
)

var openAIWSIngressPreflightPingIdle = 20 * time.Second

// openAIWSFallbackError 表示可安全回退到 HTTP 的 WS 错误（尚未写下游）。
type openAIWSFallbackError struct {
	Reason string
	Err    error
}

func (e *openAIWSFallbackError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err == nil {
		return fmt.Sprintf("openai ws fallback: %s", strings.TrimSpace(e.Reason))
	}
	return fmt.Sprintf("openai ws fallback: %s: %v", strings.TrimSpace(e.Reason), e.Err)
}

func (e *openAIWSFallbackError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func wrapOpenAIWSFallback(reason string, err error) error {
	return &openAIWSFallbackError{Reason: strings.TrimSpace(reason), Err: err}
}

// OpenAIWSClientCloseError 表示应以指定 WebSocket close code 主动关闭客户端连接的错误。
type OpenAIWSClientCloseError struct {
	statusCode coderws.StatusCode
	reason     string
	err        error
}

type openAIWSIngressTurnError struct {
	stage           string
	cause           error
	wroteDownstream bool
}

type openAIWSCurrentTurnFailoverError struct {
	cause        error
	retryPayload []byte
}

func (e *openAIWSCurrentTurnFailoverError) Error() string {
	if e == nil || e.cause == nil {
		return "openai websocket current-turn failover"
	}
	return e.cause.Error()
}

func (e *openAIWSCurrentTurnFailoverError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func newOpenAIWSCurrentTurnFailoverError(cause error, retryPayload []byte) error {
	return &openAIWSCurrentTurnFailoverError{
		cause:        cause,
		retryPayload: append([]byte(nil), retryPayload...),
	}
}

// OpenAIWSCurrentTurnRetryPayload returns an isolated copy of the payload that
// may be retried on a replacement account without replaying the first turn.
func OpenAIWSCurrentTurnRetryPayload(err error) ([]byte, bool) {
	var retryErr *openAIWSCurrentTurnFailoverError
	if !errors.As(err, &retryErr) || retryErr == nil {
		return nil, false
	}
	return append([]byte(nil), retryErr.retryPayload...), true
}

func (e *openAIWSIngressTurnError) Error() string {
	if e == nil {
		return ""
	}
	if e.cause == nil {
		return strings.TrimSpace(e.stage)
	}
	return e.cause.Error()
}

func (e *openAIWSIngressTurnError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func wrapOpenAIWSIngressTurnError(stage string, cause error, wroteDownstream bool) error {
	if cause == nil {
		return nil
	}
	return &openAIWSIngressTurnError{
		stage:           strings.TrimSpace(stage),
		cause:           cause,
		wroteDownstream: wroteDownstream,
	}
}

func isOpenAIWSIngressTurnRetryable(err error) bool {
	var turnErr *openAIWSIngressTurnError
	if !errors.As(err, &turnErr) || turnErr == nil {
		return false
	}
	if errors.Is(turnErr.cause, context.Canceled) || errors.Is(turnErr.cause, context.DeadlineExceeded) {
		return false
	}
	if turnErr.wroteDownstream {
		return false
	}
	switch turnErr.stage {
	case "write_upstream", "read_upstream":
		return true
	default:
		return false
	}
}

func openAIWSIngressTurnRetryReason(err error) string {
	var turnErr *openAIWSIngressTurnError
	if !errors.As(err, &turnErr) || turnErr == nil {
		return "unknown"
	}
	if turnErr.stage == "" {
		return "unknown"
	}
	return turnErr.stage
}

func isOpenAIWSIngressPreviousResponseNotFound(err error) bool {
	var turnErr *openAIWSIngressTurnError
	if !errors.As(err, &turnErr) || turnErr == nil {
		return false
	}
	if strings.TrimSpace(turnErr.stage) != openAIWSIngressStagePreviousResponseNotFound {
		return false
	}
	return !turnErr.wroteDownstream
}

// NewOpenAIWSClientCloseError 创建一个客户端 WS 关闭错误。
func NewOpenAIWSClientCloseError(statusCode coderws.StatusCode, reason string, err error) error {
	return &OpenAIWSClientCloseError{
		statusCode: statusCode,
		reason:     strings.TrimSpace(reason),
		err:        err,
	}
}

func (e *OpenAIWSClientCloseError) Error() string {
	if e == nil {
		return ""
	}
	if e.err == nil {
		return fmt.Sprintf("openai ws client close: %d %s", int(e.statusCode), strings.TrimSpace(e.reason))
	}
	return fmt.Sprintf("openai ws client close: %d %s: %v", int(e.statusCode), strings.TrimSpace(e.reason), e.err)
}

func (e *OpenAIWSClientCloseError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func (e *OpenAIWSClientCloseError) StatusCode() coderws.StatusCode {
	if e == nil {
		return coderws.StatusInternalError
	}
	return e.statusCode
}

func (e *OpenAIWSClientCloseError) Reason() string {
	if e == nil {
		return ""
	}
	return strings.TrimSpace(e.reason)
}

// OpenAIWSIngressHooks 定义入站 WS 每个 turn 的生命周期回调。
type OpenAIWSIngressHooks struct {
	// ClientLifecycleContext is the request context before an ingress lease
	// adds its independent cancellation signal. Downstream writes bind to it
	// so shutdown and disconnect cancellation remain direct during lease loss.
	ClientLifecycleContext context.Context
	// InitialRequestModel is the client-facing model from the first frame,
	// before channel or account mapping. Ingress modes preserve it for usage
	// attribution while MapRequestModel determines the upstream model.
	InitialRequestModel string
	// InitialTurnStartedAt freezes when the first response.create was accepted.
	InitialTurnStartedAt time.Time
	// MaxReasoningEffort limits explicit reasoning effort values for this WS session.
	MaxReasoningEffort string
	// MaxReasoningEffortOverLimit is the access control when an explicit effort
	// exceeds the ceiling: downgrade (default) or deny.
	MaxReasoningEffortOverLimit string
	// ReasoningEffortMappings rewrites explicit effort values for this WS session.
	ReasoningEffortMappings []ReasoningEffortMapping
	TurnStarted             func(turn int, startedAt time.Time)
	BeforeTurn              func(turn int) error
	BeforeRequest           func(turn int, payload []byte, originalModel string) error
	// MapRequestModel resolves the current turn's client model to the model
	// that must be written into the upstream response.create frame.
	MapRequestModel func(turn int, originalModel string) (string, error)
	AfterTurn       func(turn int, result *OpenAIForwardResult, turnErr error)
}

func (s *OpenAIGatewayService) getOpenAIWSConnPool() *openAIWSConnPool {
	if s == nil {
		return nil
	}
	s.openaiWSPoolOnce.Do(func() {
		if s.openaiWSPool == nil {
			s.openaiWSPool = newOpenAIWSConnPool(s.cfg)
		}
	})
	return s.openaiWSPool
}

// getOpenAICPAWSConnPool returns the isolated CPA-style pool. It intentionally
// has its own connection namespace and CPA-compatible pool profile so
// enabling CPA WS on one account cannot change the existing Sub2API pool.
func (s *OpenAIGatewayService) getOpenAICPAWSConnPool() *openAIWSConnPool {
	if s == nil {
		return nil
	}
	s.openaiCPAWSPoolMu.Lock()
	defer s.openaiCPAWSPoolMu.Unlock()
	if s.openaiCPAWSPool == nil {
		cfg := s.cfg
		if cfg != nil {
			clone := *cfg
			clone.Gateway = cfg.Gateway
			clone.Gateway.OpenAIWS = cfg.Gateway.OpenAIWS
			clone.Gateway.OpenAIWS.MaxConnsPerAccount = cpaWSDefaultMaxConnsPerAccount
			clone.Gateway.OpenAIWS.MinIdlePerAccount = cpaWSDefaultMinIdlePerAccount
			clone.Gateway.OpenAIWS.MaxIdlePerAccount = cpaWSDefaultMaxIdlePerAccount
			clone.Gateway.OpenAIWS.QueueLimitPerConn = cpaWSDefaultQueueLimitPerConn
			clone.Gateway.OpenAIWS.PoolTargetUtilization = cpaWSDefaultPoolTargetUtilization
			clone.Gateway.OpenAIWS.MaxRequestsPerConn = cpaWSDefaultMaxRequestsPerConn
			clone.Gateway.OpenAIWS.MaxConnAgeSeconds = cpaWSDefaultMaxConnAgeSeconds
			clone.Gateway.OpenAIWS.DialTimeoutSeconds = cpaWSDefaultDialTimeoutSeconds
			clone.Gateway.OpenAIWS.ReadTimeoutSeconds = cpaWSDefaultReadTimeoutSeconds
			clone.Gateway.OpenAIWS.WriteTimeoutSeconds = cpaWSDefaultWriteTimeoutSeconds
			clone.Gateway.OpenAIWS.PrewarmCooldownMS = cpaWSDefaultPrewarmCooldownMS
			clone.Gateway.OpenAIWS.RetryBackoffInitialMS = cpaWSDefaultRetryBackoffInitialMS
			clone.Gateway.OpenAIWS.RetryBackoffMaxMS = cpaWSDefaultRetryBackoffMaxMS
			clone.Gateway.OpenAIWS.RetryJitterRatio = cpaWSDefaultRetryJitterRatio
			clone.Gateway.OpenAIWS.RetryTotalBudgetMS = cpaWSDefaultRetryTotalBudgetMS
			clone.Gateway.OpenAIWS.EventFlushBatchSize = cpaWSDefaultEventFlushBatchSize
			clone.Gateway.OpenAIWS.EventFlushIntervalMS = cpaWSDefaultEventFlushIntervalMS
			// Prefer persisted admin settings when available. The values are
			// read once when the isolated CPA pool is created; changing them
			// takes effect after the service restart/reload, while the ordinary
			// Sub2API pool is never affected.
			if s.settingService != nil {
				if settings, err := s.settingService.GetAllSettings(context.Background()); err == nil && settings != nil {
					clone.Gateway.OpenAIWS.MaxConnsPerAccount = settings.CPAWSMaxConnsPerAccount
					clone.Gateway.OpenAIWS.MinIdlePerAccount = settings.CPAWSMinIdlePerAccount
					clone.Gateway.OpenAIWS.MaxIdlePerAccount = settings.CPAWSMaxIdlePerAccount
					clone.Gateway.OpenAIWS.QueueLimitPerConn = settings.CPAWSQueueLimitPerConn
					clone.Gateway.OpenAIWS.PoolTargetUtilization = settings.CPAWSPoolTargetUtilization
					clone.Gateway.OpenAIWS.MaxRequestsPerConn = settings.CPAWSMaxRequestsPerConn
					clone.Gateway.OpenAIWS.MaxConnAgeSeconds = settings.CPAWSMaxConnAgeSeconds
					clone.Gateway.OpenAIWS.DialTimeoutSeconds = settings.CPAWSDialTimeoutSeconds
					clone.Gateway.OpenAIWS.ReadTimeoutSeconds = settings.CPAWSReadTimeoutSeconds
					clone.Gateway.OpenAIWS.WriteTimeoutSeconds = settings.CPAWSWriteTimeoutSeconds
					clone.Gateway.OpenAIWS.PrewarmCooldownMS = settings.CPAWSPrewarmCooldownMS
					clone.Gateway.OpenAIWS.RetryBackoffInitialMS = settings.CPAWSRetryBackoffInitialMS
					clone.Gateway.OpenAIWS.RetryBackoffMaxMS = settings.CPAWSRetryBackoffMaxMS
					clone.Gateway.OpenAIWS.RetryJitterRatio = settings.CPAWSRetryJitterRatio
					clone.Gateway.OpenAIWS.RetryTotalBudgetMS = settings.CPAWSRetryTotalBudgetMS
					clone.Gateway.OpenAIWS.EventFlushBatchSize = settings.CPAWSEventFlushBatchSize
					clone.Gateway.OpenAIWS.EventFlushIntervalMS = settings.CPAWSEventFlushIntervalMS
				}
			}
			if clone.Gateway.OpenAIWS.MaxConnsPerAccount <= 0 {
				clone.Gateway.OpenAIWS.MaxConnsPerAccount = cpaWSDefaultMaxConnsPerAccount
			}
			if clone.Gateway.OpenAIWS.MinIdlePerAccount < 0 {
				clone.Gateway.OpenAIWS.MinIdlePerAccount = cpaWSDefaultMinIdlePerAccount
			}
			if clone.Gateway.OpenAIWS.MaxIdlePerAccount < 0 {
				clone.Gateway.OpenAIWS.MaxIdlePerAccount = cpaWSDefaultMaxIdlePerAccount
			}
			if clone.Gateway.OpenAIWS.QueueLimitPerConn <= 0 {
				clone.Gateway.OpenAIWS.QueueLimitPerConn = cpaWSDefaultQueueLimitPerConn
			}
			if clone.Gateway.OpenAIWS.PoolTargetUtilization <= 0 {
				clone.Gateway.OpenAIWS.PoolTargetUtilization = cpaWSDefaultPoolTargetUtilization
			}
			if clone.Gateway.OpenAIWS.MaxRequestsPerConn <= 0 {
				clone.Gateway.OpenAIWS.MaxRequestsPerConn = cpaWSDefaultMaxRequestsPerConn
			}
			if clone.Gateway.OpenAIWS.MaxConnAgeSeconds <= 0 {
				clone.Gateway.OpenAIWS.MaxConnAgeSeconds = cpaWSDefaultMaxConnAgeSeconds
			}
			clone.Gateway.OpenAIWS.DynamicMaxConnsByAccountConcurrencyEnabled = false
			cfg = &clone
		}
		s.openaiCPAWSPool = newOpenAIWSConnPool(cfg)
		s.openaiCPAWSPool.cpaProfile = true
	}
	return s.openaiCPAWSPool
}

func (s *OpenAIGatewayService) reloadCPAWSRuntime() {
	if s == nil || s.settingService == nil {
		return
	}
	settings, err := s.settingService.GetAllSettings(context.Background())
	if err != nil || settings == nil {
		return
	}
	s.openaiCPAWSGlobalOAuthEnabled.Store(settings.CPAWSGlobalOAuthEnabled)
	s.openaiCPAWSPoolMu.Lock()
	old := s.openaiCPAWSPool
	if old == nil || cpaWSPoolConfigMatchesSettings(old.cfg, settings) {
		s.openaiCPAWSPoolMu.Unlock()
		return
	}
	s.openaiCPAWSPool = nil
	s.openaiCPAWSPoolMu.Unlock()
	old.Close()
}

func cpaWSPoolConfigMatchesSettings(cfg *config.Config, settings *SystemSettings) bool {
	if cfg == nil || settings == nil {
		return false
	}
	ws := cfg.Gateway.OpenAIWS
	return ws.MaxConnsPerAccount == settings.CPAWSMaxConnsPerAccount &&
		ws.MinIdlePerAccount == settings.CPAWSMinIdlePerAccount &&
		ws.MaxIdlePerAccount == settings.CPAWSMaxIdlePerAccount &&
		ws.QueueLimitPerConn == settings.CPAWSQueueLimitPerConn &&
		ws.PoolTargetUtilization == settings.CPAWSPoolTargetUtilization &&
		ws.MaxRequestsPerConn == settings.CPAWSMaxRequestsPerConn &&
		ws.MaxConnAgeSeconds == settings.CPAWSMaxConnAgeSeconds &&
		ws.DialTimeoutSeconds == settings.CPAWSDialTimeoutSeconds &&
		ws.ReadTimeoutSeconds == settings.CPAWSReadTimeoutSeconds &&
		ws.WriteTimeoutSeconds == settings.CPAWSWriteTimeoutSeconds &&
		ws.PrewarmCooldownMS == settings.CPAWSPrewarmCooldownMS &&
		ws.RetryBackoffInitialMS == settings.CPAWSRetryBackoffInitialMS &&
		ws.RetryBackoffMaxMS == settings.CPAWSRetryBackoffMaxMS &&
		ws.RetryJitterRatio == settings.CPAWSRetryJitterRatio &&
		ws.RetryTotalBudgetMS == settings.CPAWSRetryTotalBudgetMS &&
		ws.EventFlushBatchSize == settings.CPAWSEventFlushBatchSize &&
		ws.EventFlushIntervalMS == settings.CPAWSEventFlushIntervalMS
}

func (s *OpenAIGatewayService) getOpenAIWSPoolForDecision(decision OpenAIWSProtocolDecision) *openAIWSConnPool {
	if decision.Transport == OpenAIUpstreamTransportResponsesWebsocketCPA {
		return s.getOpenAICPAWSConnPool()
	}
	return s.getOpenAIWSConnPool()
}

func (s *OpenAIGatewayService) openAIWSRuntimeConfigForDecision(decision OpenAIWSProtocolDecision) *config.Config {
	if s == nil {
		return nil
	}
	if decision.Transport == OpenAIUpstreamTransportResponsesWebsocketCPA {
		if pool := s.getOpenAICPAWSConnPool(); pool != nil {
			return pool.cfg
		}
	}
	return s.cfg
}

func (s *OpenAIGatewayService) openAIWSReadTimeoutForDecision(decision OpenAIWSProtocolDecision) time.Duration {
	if cfg := s.openAIWSRuntimeConfigForDecision(decision); cfg != nil && cfg.Gateway.OpenAIWS.ReadTimeoutSeconds > 0 {
		return time.Duration(cfg.Gateway.OpenAIWS.ReadTimeoutSeconds) * time.Second
	}
	return s.openAIWSReadTimeout()
}

func (s *OpenAIGatewayService) openAIWSWriteTimeoutForDecision(decision OpenAIWSProtocolDecision) time.Duration {
	if cfg := s.openAIWSRuntimeConfigForDecision(decision); cfg != nil && cfg.Gateway.OpenAIWS.WriteTimeoutSeconds > 0 {
		return time.Duration(cfg.Gateway.OpenAIWS.WriteTimeoutSeconds) * time.Second
	}
	return s.openAIWSWriteTimeout()
}

func (s *OpenAIGatewayService) openAIWSAcquireTimeoutForDecision(decision OpenAIWSProtocolDecision) time.Duration {
	if cfg := s.openAIWSRuntimeConfigForDecision(decision); cfg != nil && cfg.Gateway.OpenAIWS.DialTimeoutSeconds > 0 {
		return time.Duration(cfg.Gateway.OpenAIWS.DialTimeoutSeconds)*time.Second + 2*time.Second
	}
	return s.openAIWSAcquireTimeout()
}

func (s *OpenAIGatewayService) openAIWSEventFlushBatchSizeForDecision(decision OpenAIWSProtocolDecision) int {
	if cfg := s.openAIWSRuntimeConfigForDecision(decision); cfg != nil && cfg.Gateway.OpenAIWS.EventFlushBatchSize > 0 {
		return cfg.Gateway.OpenAIWS.EventFlushBatchSize
	}
	return s.openAIWSEventFlushBatchSize()
}

func (s *OpenAIGatewayService) openAIWSEventFlushIntervalForDecision(decision OpenAIWSProtocolDecision) time.Duration {
	if cfg := s.openAIWSRuntimeConfigForDecision(decision); cfg != nil && cfg.Gateway.OpenAIWS.EventFlushIntervalMS >= 0 {
		return time.Duration(cfg.Gateway.OpenAIWS.EventFlushIntervalMS) * time.Millisecond
	}
	return s.openAIWSEventFlushInterval()
}

func (s *OpenAIGatewayService) getOpenAIWSPassthroughDialer() openAIWSClientDialer {
	if s == nil {
		return nil
	}
	s.openaiWSPassthroughDialerOnce.Do(func() {
		if s.openaiWSPassthroughDialer == nil {
			s.openaiWSPassthroughDialer = newDefaultOpenAIWSClientDialer()
		}
	})
	return s.openaiWSPassthroughDialer
}

func (s *OpenAIGatewayService) SnapshotOpenAIWSPoolMetrics() OpenAIWSPoolMetricsSnapshot {
	pool := s.getOpenAIWSConnPool()
	if pool == nil {
		return OpenAIWSPoolMetricsSnapshot{}
	}
	return pool.SnapshotMetrics()
}

type OpenAIWSPerformanceMetricsSnapshot struct {
	Pool             OpenAIWSPoolMetricsSnapshot           `json:"pool"`
	Retry            OpenAIWSRetryMetricsSnapshot          `json:"retry"`
	Transport        OpenAIWSTransportMetricsSnapshot      `json:"transport"`
	CPAPool          OpenAIWSPoolMetricsSnapshot           `json:"cpa_pool"`
	CPATransport     OpenAIWSTransportMetricsSnapshot      `json:"cpa_transport"`
	CPACacheAffinity OpenAICPACacheAffinityMetricsSnapshot `json:"cpa_cache_affinity"`
}

func (s *OpenAIGatewayService) SnapshotOpenAIWSPerformanceMetrics() OpenAIWSPerformanceMetricsSnapshot {
	if s == nil {
		return OpenAIWSPerformanceMetricsSnapshot{}
	}
	pool := s.getOpenAIWSConnPool()
	snapshot := OpenAIWSPerformanceMetricsSnapshot{
		Retry: s.SnapshotOpenAIWSRetryMetrics(),
	}
	if pool == nil {
		// CPAWS has an isolated pool and can be active even when the original
		// Sub2API pool has never been initialized.
	} else {
		snapshot.Pool = pool.SnapshotMetrics()
		snapshot.Transport = pool.SnapshotTransportMetrics()
	}
	s.openaiCPAWSPoolMu.Lock()
	cpaPool := s.openaiCPAWSPool
	s.openaiCPAWSPoolMu.Unlock()
	if cpaPool != nil {
		snapshot.CPAPool = cpaPool.SnapshotMetrics()
		snapshot.CPATransport = cpaPool.SnapshotTransportMetrics()
	}
	snapshot.CPACacheAffinity = s.SnapshotOpenAICPACacheAffinityMetrics()
	return snapshot
}

func (s *OpenAIGatewayService) getOpenAIWSStateStore() OpenAIWSStateStore {
	if s == nil {
		return nil
	}
	s.openaiWSStateStoreOnce.Do(func() {
		if s.openaiWSStateStore == nil {
			s.openaiWSStateStore = NewOpenAIWSStateStore(s.cache)
		}
	})
	return s.openaiWSStateStore
}

func (s *OpenAIGatewayService) openAIWSResponseStickyTTL() time.Duration {
	if s != nil && s.cfg != nil {
		seconds := s.cfg.Gateway.OpenAIWS.StickyResponseIDTTLSeconds
		if seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return time.Hour
}

func (s *OpenAIGatewayService) openAIWSIngressPreviousResponseRecoveryEnabled() bool {
	if s != nil && s.cfg != nil {
		return s.cfg.Gateway.OpenAIWS.IngressPreviousResponseRecoveryEnabled
	}
	return true
}

func (s *OpenAIGatewayService) openAIWSReadTimeout() time.Duration {
	if s != nil && s.cfg != nil && s.cfg.Gateway.OpenAIWS.ReadTimeoutSeconds > 0 {
		return time.Duration(s.cfg.Gateway.OpenAIWS.ReadTimeoutSeconds) * time.Second
	}
	return 15 * time.Minute
}

func (s *OpenAIGatewayService) openAIWSPassthroughIdleTimeout() time.Duration {
	if timeout := s.openAIWSReadTimeout(); timeout > 0 {
		return timeout
	}
	return openAIWSPassthroughIdleTimeoutDefault
}

func (s *OpenAIGatewayService) openAIWSWriteTimeout() time.Duration {
	if s != nil && s.cfg != nil && s.cfg.Gateway.OpenAIWS.WriteTimeoutSeconds > 0 {
		return time.Duration(s.cfg.Gateway.OpenAIWS.WriteTimeoutSeconds) * time.Second
	}
	return 2 * time.Minute
}

func (s *OpenAIGatewayService) openAIWSEventFlushBatchSize() int {
	if s != nil && s.cfg != nil && s.cfg.Gateway.OpenAIWS.EventFlushBatchSize > 0 {
		return s.cfg.Gateway.OpenAIWS.EventFlushBatchSize
	}
	return openAIWSEventFlushBatchSizeDefault
}

func (s *OpenAIGatewayService) openAIWSEventFlushInterval() time.Duration {
	if s != nil && s.cfg != nil && s.cfg.Gateway.OpenAIWS.EventFlushIntervalMS >= 0 {
		if s.cfg.Gateway.OpenAIWS.EventFlushIntervalMS == 0 {
			return 0
		}
		return time.Duration(s.cfg.Gateway.OpenAIWS.EventFlushIntervalMS) * time.Millisecond
	}
	return openAIWSEventFlushIntervalDefault
}

func (s *OpenAIGatewayService) openAIWSPayloadLogSampleRate() float64 {
	if s != nil && s.cfg != nil {
		rate := s.cfg.Gateway.OpenAIWS.PayloadLogSampleRate
		if rate < 0 {
			return 0
		}
		if rate > 1 {
			return 1
		}
		return rate
	}
	return openAIWSPayloadLogSampleDefault
}

func (s *OpenAIGatewayService) shouldLogOpenAIWSPayloadSchema(attempt int) bool {
	// 首次尝试保留一条完整 payload_schema 便于排障。
	if attempt <= 1 {
		return true
	}
	rate := s.openAIWSPayloadLogSampleRate()
	if rate <= 0 {
		return false
	}
	if rate >= 1 {
		return true
	}
	return rand.Float64() < rate
}

func (s *OpenAIGatewayService) shouldEmitOpenAIWSPayloadSchema(attempt int) bool {
	if !s.shouldLogOpenAIWSPayloadSchema(attempt) {
		return false
	}
	return logger.L().Core().Enabled(zap.DebugLevel)
}

func (s *OpenAIGatewayService) openAIWSDialTimeout() time.Duration {
	if s != nil && s.cfg != nil && s.cfg.Gateway.OpenAIWS.DialTimeoutSeconds > 0 {
		return time.Duration(s.cfg.Gateway.OpenAIWS.DialTimeoutSeconds) * time.Second
	}
	return 10 * time.Second
}

func (s *OpenAIGatewayService) openAIWSAcquireTimeout() time.Duration {
	// Acquire 覆盖“连接复用命中/排队/新建连接”三个阶段。
	// 这里不再叠加 write_timeout，避免高并发排队时把 TTFT 长尾拉到分钟级。
	dial := s.openAIWSDialTimeout()
	if dial <= 0 {
		dial = 10 * time.Second
	}
	return dial + 2*time.Second
}
