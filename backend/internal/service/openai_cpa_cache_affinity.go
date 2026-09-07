package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/gjson"
)

const (
	openAICPACacheAffinityTTL       = time.Hour
	openAICPAPrefixHeatTTL          = time.Hour
	openAICPACacheAffinityMaxRoutes = 65536
	openAICPAPrefixHeatMinBytes     = 4096
)

type openAICPAAffinityContextKey struct{}

type openAICPAAffinityRequestState struct {
	routeIdentity     string
	prefixFingerprint string
	modelFamily       string
	selectedAccountID int64
	selectedBy        string
	routeLookedUp     bool
	boundAccountID    int64
	prefixLookedUp    bool
	prefixCandidates  []int64
	mu                sync.Mutex
}

type openAICPAAffinityBinding struct {
	accountID int64
	expiresAt time.Time
}

type openAICPAPrefixAccountHeat struct {
	accountID int64
	successes uint64
	lastAt    time.Time
	expiresAt time.Time
}

type OpenAICPACacheAffinityMetricsSnapshot struct {
	RouteHits            int64 `json:"route_hits"`
	RouteMisses          int64 `json:"route_misses"`
	RouteEscapes         int64 `json:"route_escapes"`
	RouteBindings        int64 `json:"route_bindings"`
	PrefixLookups        int64 `json:"prefix_lookups"`
	PrefixHits           int64 `json:"prefix_hits"`
	PrefixSelections     int64 `json:"prefix_selections"`
	TrackedRoutes        int   `json:"tracked_routes"`
	TrackedPrefixEntries int   `json:"tracked_prefix_entries"`
}

type openAICPACacheAffinityMetrics struct {
	routeHits        atomic.Int64
	routeMisses      atomic.Int64
	routeEscapes     atomic.Int64
	routeBindings    atomic.Int64
	prefixLookups    atomic.Int64
	prefixHits       atomic.Int64
	prefixSelections atomic.Int64
}

type openAICPACacheAffinityCoordinator struct {
	mu          sync.Mutex
	routes      map[string]openAICPAAffinityBinding
	prefixHeat  map[string]map[int64]openAICPAPrefixAccountHeat
	lastCleanup time.Time
	metrics     openAICPACacheAffinityMetrics
}

func newOpenAICPACacheAffinityCoordinator() *openAICPACacheAffinityCoordinator {
	return &openAICPACacheAffinityCoordinator{
		routes:     make(map[string]openAICPAAffinityBinding),
		prefixHeat: make(map[string]map[int64]openAICPAPrefixAccountHeat),
	}
}

// WithOpenAICPACacheAffinityRequest attaches CPA-only routing identities to a
// request. They are inert unless the selected account explicitly opts in.
func WithOpenAICPACacheAffinityRequest(ctx context.Context, headers http.Header, body []byte, sessionHash, model string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	identity := resolveOpenAICPAAffinityIdentity(headers, body, sessionHash)
	if identity == "" {
		return ctx
	}
	state := &openAICPAAffinityRequestState{
		routeIdentity:     cpaWSStableID("account-affinity-route", identity),
		prefixFingerprint: openAICPAPrefixFingerprint(body, model),
		modelFamily:       cpaWSCanonicalModelFamily(model),
	}
	return context.WithValue(ctx, openAICPAAffinityContextKey{}, state)
}

func openAICPAAffinityStateFromContext(ctx context.Context) *openAICPAAffinityRequestState {
	if ctx == nil {
		return nil
	}
	state, _ := ctx.Value(openAICPAAffinityContextKey{}).(*openAICPAAffinityRequestState)
	return state
}

func resolveOpenAICPAAffinityIdentity(headers http.Header, body []byte, fallback string) string {
	for _, name := range []string{"X-Claude-Code-Session-Id", "Session-Id", "Session_id", "X-Session-ID", "X-Session-Affinity", "X-Codex-Window-Id"} {
		if value := strings.TrimSpace(headers.Get(name)); value != "" {
			return strings.ToLower(name) + ":" + value
		}
	}
	if raw := strings.TrimSpace(headers.Get("X-Codex-Turn-Metadata")); raw != "" {
		for _, path := range []string{"prompt_cache_key", "window_id", "conversation_id"} {
			if value := strings.TrimSpace(gjson.Get(raw, path).String()); value != "" {
				return "codex-turn:" + value
			}
		}
	}
	for _, path := range []string{"prompt_cache_key", "metadata.session_id", "conversation_id"} {
		if value := strings.TrimSpace(gjson.GetBytes(body, path).String()); value != "" {
			return path + ":" + value
		}
	}
	if value := strings.TrimSpace(fallback); value != "" {
		return "sub-session:" + value
	}
	return ""
}

func openAICPAPrefixFingerprint(body []byte, model string) string {
	if len(body) == 0 {
		return ""
	}
	var root map[string]any
	if err := json.Unmarshal(body, &root); err != nil {
		return ""
	}
	prefix := make(map[string]any, 10)
	for _, key := range []string{"instructions", "tools", "tool_choice", "parallel_tool_calls", "reasoning", "text", "include"} {
		if value, ok := root[key]; ok {
			prefix[key] = value
		}
	}
	for _, key := range []string{"messages", "input"} {
		items, ok := root[key].([]any)
		if !ok || len(items) == 0 {
			continue
		}
		stable := make([]any, 0, len(items))
		for _, item := range items {
			object, ok := item.(map[string]any)
			if !ok {
				break
			}
			role, _ := object["role"].(string)
			role = strings.ToLower(strings.TrimSpace(role))
			if role != "system" && role != "developer" {
				break
			}
			stable = append(stable, object)
		}
		if len(stable) > 0 {
			prefix[key] = stable
		}
	}
	prefix["model_family"] = cpaWSCanonicalModelFamily(model)
	canonical, err := json.Marshal(prefix)
	if err != nil || len(canonical) < openAICPAPrefixHeatMinBytes {
		return ""
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}

func openAICPAAffinityRouteKey(groupID *int64, state *openAICPAAffinityRequestState) string {
	if state == nil || state.routeIdentity == "" {
		return ""
	}
	return cpaWSStableID("account-affinity-binding", strings.TrimSpace(state.modelFamily), strings.TrimSpace(state.routeIdentity), strconv.FormatInt(derefGroupID(groupID), 10))
}

func openAICPAPrefixKey(groupID *int64, state *openAICPAAffinityRequestState) string {
	if state == nil || state.prefixFingerprint == "" {
		return ""
	}
	return cpaWSStableID("account-affinity-prefix", strings.TrimSpace(state.modelFamily), state.prefixFingerprint, strconv.FormatInt(derefGroupID(groupID), 10))
}

func (c *openAICPACacheAffinityCoordinator) lookupRoute(key string, now time.Time) (int64, bool) {
	if c == nil || key == "" {
		return 0, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cleanupLocked(now)
	binding, ok := c.routes[key]
	if !ok || !binding.expiresAt.After(now) {
		delete(c.routes, key)
		c.metrics.routeMisses.Add(1)
		return 0, false
	}
	c.metrics.routeHits.Add(1)
	return binding.accountID, true
}

func (c *openAICPACacheAffinityCoordinator) bindRoute(key string, accountID int64, now time.Time) {
	if c == nil || key == "" || accountID <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cleanupLocked(now)
	if len(c.routes) >= openAICPACacheAffinityMaxRoutes {
		c.evictOldestRouteLocked()
	}
	c.routes[key] = openAICPAAffinityBinding{accountID: accountID, expiresAt: now.Add(openAICPACacheAffinityTTL)}
	c.metrics.routeBindings.Add(1)
}

func (c *openAICPACacheAffinityCoordinator) lookupPrefix(key string, now time.Time) []int64 {
	if c == nil || key == "" {
		return nil
	}
	c.metrics.prefixLookups.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cleanupLocked(now)
	entries := c.prefixHeat[key]
	if len(entries) == 0 {
		return nil
	}
	values := make([]openAICPAPrefixAccountHeat, 0, len(entries))
	for accountID, entry := range entries {
		if !entry.expiresAt.After(now) {
			delete(entries, accountID)
			continue
		}
		values = append(values, entry)
	}
	sort.SliceStable(values, func(i, j int) bool {
		if values[i].successes != values[j].successes {
			return values[i].successes > values[j].successes
		}
		return values[i].lastAt.After(values[j].lastAt)
	})
	result := make([]int64, 0, len(values))
	for _, entry := range values {
		result = append(result, entry.accountID)
	}
	if len(result) > 0 {
		c.metrics.prefixHits.Add(1)
	}
	return result
}

func (c *openAICPACacheAffinityCoordinator) recordPrefix(key string, accountID int64, now time.Time) {
	if c == nil || key == "" || accountID <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cleanupLocked(now)
	entries := c.prefixHeat[key]
	if entries == nil {
		if len(c.prefixHeat) >= openAICPACacheAffinityMaxRoutes {
			c.evictOldestPrefixLocked()
		}
		entries = make(map[int64]openAICPAPrefixAccountHeat)
		c.prefixHeat[key] = entries
	}
	entry := entries[accountID]
	entry.accountID = accountID
	entry.successes++
	entry.lastAt = now
	entry.expiresAt = now.Add(openAICPAPrefixHeatTTL)
	entries[accountID] = entry
}

func (c *openAICPACacheAffinityCoordinator) cleanupLocked(now time.Time) {
	if !c.lastCleanup.IsZero() && now.Sub(c.lastCleanup) < time.Minute {
		return
	}
	for key, binding := range c.routes {
		if !binding.expiresAt.After(now) {
			delete(c.routes, key)
		}
	}
	for key, entries := range c.prefixHeat {
		for accountID, entry := range entries {
			if !entry.expiresAt.After(now) {
				delete(entries, accountID)
			}
		}
		if len(entries) == 0 {
			delete(c.prefixHeat, key)
		}
	}
	c.lastCleanup = now
}

func (c *openAICPACacheAffinityCoordinator) evictOldestRouteLocked() {
	var oldestKey string
	var oldestExpiry time.Time
	for key, binding := range c.routes {
		if oldestKey == "" || binding.expiresAt.Before(oldestExpiry) {
			oldestKey = key
			oldestExpiry = binding.expiresAt
		}
	}
	if oldestKey != "" {
		delete(c.routes, oldestKey)
	}
}

func (c *openAICPACacheAffinityCoordinator) evictOldestPrefixLocked() {
	var oldestKey string
	var oldest time.Time
	for key, entries := range c.prefixHeat {
		latest := time.Time{}
		for _, entry := range entries {
			if entry.lastAt.After(latest) {
				latest = entry.lastAt
			}
		}
		if oldestKey == "" || latest.Before(oldest) {
			oldestKey = key
			oldest = latest
		}
	}
	if oldestKey != "" {
		delete(c.prefixHeat, oldestKey)
	}
}

func (c *openAICPACacheAffinityCoordinator) snapshot() OpenAICPACacheAffinityMetricsSnapshot {
	if c == nil {
		return OpenAICPACacheAffinityMetricsSnapshot{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	prefixEntries := 0
	for _, entries := range c.prefixHeat {
		prefixEntries += len(entries)
	}
	return OpenAICPACacheAffinityMetricsSnapshot{
		RouteHits:            c.metrics.routeHits.Load(),
		RouteMisses:          c.metrics.routeMisses.Load(),
		RouteEscapes:         c.metrics.routeEscapes.Load(),
		RouteBindings:        c.metrics.routeBindings.Load(),
		PrefixLookups:        c.metrics.prefixLookups.Load(),
		PrefixHits:           c.metrics.prefixHits.Load(),
		PrefixSelections:     c.metrics.prefixSelections.Load(),
		TrackedRoutes:        len(c.routes),
		TrackedPrefixEntries: prefixEntries,
	}
}

func (s *OpenAIGatewayService) getOpenAICPACacheAffinityCoordinator() *openAICPACacheAffinityCoordinator {
	if s == nil {
		return nil
	}
	s.openaiCPAAffinityOnce.Do(func() {
		s.openaiCPAAffinity.Store(newOpenAICPACacheAffinityCoordinator())
	})
	return s.openaiCPAAffinity.Load()
}

func (s *OpenAIGatewayService) SnapshotOpenAICPACacheAffinityMetrics() OpenAICPACacheAffinityMetricsSnapshot {
	if s == nil || s.openaiCPAAffinity.Load() == nil {
		return OpenAICPACacheAffinityMetricsSnapshot{}
	}
	return s.openaiCPAAffinity.Load().snapshot()
}

func noteOpenAICPAAffinitySelection(ctx context.Context, account *Account, source string) {
	state := openAICPAAffinityStateFromContext(ctx)
	if state == nil || account == nil || !account.IsOpenAICPACacheAffinityEnabled() {
		return
	}
	state.mu.Lock()
	state.selectedAccountID = account.ID
	state.selectedBy = strings.TrimSpace(source)
	state.mu.Unlock()
}

func (s *OpenAIGatewayService) trySelectOpenAICPACacheAffinity(
	ctx context.Context,
	scheduler *defaultOpenAIAccountScheduler,
	groupID *int64,
	platform string,
	requestedModel string,
	requiredTransport OpenAIUpstreamTransport,
	requiredCapability OpenAIEndpointCapability,
	requiredImageCapability OpenAIImagesCapability,
	requireCompact bool,
	excludedIDs map[int64]struct{},
) (*AccountSelectionResult, OpenAIAccountScheduleDecision, bool, error) {
	decision := OpenAIAccountScheduleDecision{}
	state := openAICPAAffinityStateFromContext(ctx)
	if s == nil || scheduler == nil || state == nil || state.routeIdentity == "" {
		return nil, decision, false, nil
	}
	// Do not initialize the coordinator on ordinary Sub2API traffic. The first
	// successful opt-in CPAWS request creates it from the report path below.
	coordinator := s.openaiCPAAffinity.Load()
	if coordinator == nil {
		return nil, decision, false, nil
	}
	now := time.Now()
	state.mu.Lock()
	if !state.routeLookedUp {
		state.boundAccountID, _ = coordinator.lookupRoute(openAICPAAffinityRouteKey(groupID, state), now)
		state.routeLookedUp = true
	}
	boundAccountID := state.boundAccountID
	if !state.prefixLookedUp {
		state.prefixCandidates = coordinator.lookupPrefix(openAICPAPrefixKey(groupID, state), now)
		state.prefixLookedUp = true
	}
	prefixCandidates := append([]int64(nil), state.prefixCandidates...)
	state.mu.Unlock()

	tryAccount := func(accountID int64, source string) (*AccountSelectionResult, bool, error) {
		if accountID <= 0 {
			return nil, false, nil
		}
		if _, excluded := excludedIDs[accountID]; excluded {
			return nil, false, nil
		}
		account, err := s.getSchedulableAccount(ctx, accountID)
		if err != nil || account == nil || !account.IsOpenAICPACacheAffinityEnabled() {
			return nil, false, nil
		}
		if source == "prefix" && !account.IsOpenAICPAPrefixHeatEnabled() {
			return nil, false, nil
		}
		selection, escaped, selectErr := scheduler.selectBySessionHash(ctx, OpenAIAccountScheduleRequest{
			GroupID:                 groupID,
			Platform:                platform,
			SessionHash:             state.routeIdentity,
			StickyAccountID:         accountID,
			PreserveStickyBinding:   true,
			RequestedModel:          requestedModel,
			RequiredTransport:       requiredTransport,
			RequiredCapability:      requiredCapability,
			RequiredImageCapability: requiredImageCapability,
			RequireCompact:          requireCompact,
			ExcludedIDs:             excludedIDs,
			RequirePrivacySet:       s.openAIGroupRequiresPrivacySet(ctx, groupID),
		})
		if selectErr != nil {
			return nil, false, selectErr
		}
		if escaped || selection == nil || selection.Account == nil {
			return nil, false, nil
		}
		// Returning a short WaitPlan would turn its timeout into a client error
		// instead of falling back through the scheduler. Cache-first is the only
		// explicit mode allowed to wait for an affinity account.
		if selection.WaitPlan != nil {
			switch account.OpenAICPACacheAffinityMode() {
			case OpenAICPACacheAffinityModeFirstToken, OpenAICPACacheAffinityModeBalanced:
				return nil, false, nil
			}
		}
		noteOpenAICPAAffinitySelection(ctx, selection.Account, source)
		return selection, true, nil
	}

	if boundAccountID > 0 {
		selection, selected, err := tryAccount(boundAccountID, "route")
		if err != nil {
			return nil, decision, true, err
		}
		if selected {
			decision.Layer = "cpa_cache_affinity"
			decision.StickySessionHit = true
			decision.SelectedAccountID = selection.Account.ID
			decision.SelectedAccountType = selection.Account.Type
			return selection, decision, true, nil
		}
		coordinator.metrics.routeEscapes.Add(1)
	}

	for _, accountID := range prefixCandidates {
		if accountID == boundAccountID {
			continue
		}
		selection, selected, err := tryAccount(accountID, "prefix")
		if err != nil {
			return nil, decision, true, err
		}
		if selected {
			decision.Layer = "cpa_prefix_heat"
			decision.SelectedAccountID = selection.Account.ID
			decision.SelectedAccountType = selection.Account.Type
			return selection, decision, true, nil
		}
	}
	return nil, decision, false, nil
}

// ReportOpenAICPACacheAffinityResult commits route and prefix heat only after
// a successful upstream result. Selection failures never poison future routes.
func (s *OpenAIGatewayService) ReportOpenAICPACacheAffinityResult(ctx context.Context, groupID *int64, account *Account, success bool) {
	state := openAICPAAffinityStateFromContext(ctx)
	if state == nil || account == nil || !success || !account.IsOpenAICPACacheAffinityEnabled() {
		return
	}
	state.mu.Lock()
	selectedAccountID := state.selectedAccountID
	selectedBy := state.selectedBy
	state.mu.Unlock()
	if selectedAccountID != account.ID {
		return
	}
	coordinator := s.getOpenAICPACacheAffinityCoordinator()
	if coordinator == nil {
		return
	}
	now := time.Now()
	coordinator.bindRoute(openAICPAAffinityRouteKey(groupID, state), account.ID, now)
	if account.IsOpenAICPAPrefixHeatEnabled() {
		coordinator.recordPrefix(openAICPAPrefixKey(groupID, state), account.ID, now)
	}
	if selectedBy == "prefix" {
		coordinator.metrics.prefixSelections.Add(1)
	}
}
