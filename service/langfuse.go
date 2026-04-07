package service

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/dto"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/setting/system_setting"
	"github.com/QuantumNous/new-api/types"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const (
	langfuseOTLPPath             = "/api/public/otel/v1/traces"
	langfuseIngestionVersion     = "4"
	langfuseContextKeyUsage      = "langfuse_usage"
	langfuseContextKeyTaskQuota  = "langfuse_task_quota"
	langfuseContextKeyTaskResult = "langfuse_task_result"
	langfuseTracerName           = "github.com/QuantumNous/new-api/langfuse"
)

type LangfuseUsage struct {
	PromptTokens     int            `json:"prompt_tokens,omitempty"`
	CompletionTokens int            `json:"completion_tokens,omitempty"`
	TotalTokens      int            `json:"total_tokens,omitempty"`
	InputTokens      int            `json:"input_tokens,omitempty"`
	OutputTokens     int            `json:"output_tokens,omitempty"`
	Quota            int            `json:"quota,omitempty"`
	Details          map[string]any `json:"details,omitempty"`
}

type LangfuseTaskResult struct {
	Platform       string         `json:"platform,omitempty"`
	Action         string         `json:"action,omitempty"`
	UpstreamTaskID string         `json:"upstream_task_id,omitempty"`
	Quota          int            `json:"quota,omitempty"`
	TaskData       any            `json:"task_data,omitempty"`
	Metadata       map[string]any `json:"metadata,omitempty"`
}

type LangfuseTraceSnapshot struct {
	RequestID         string              `json:"request_id,omitempty"`
	Name              string              `json:"name"`
	UserID            string              `json:"user_id,omitempty"`
	SessionID         string              `json:"session_id,omitempty"`
	Route             string              `json:"route,omitempty"`
	Method            string              `json:"method,omitempty"`
	StatusCode        int                 `json:"status_code,omitempty"`
	StatusMessage     string              `json:"status_message,omitempty"`
	IsError           bool                `json:"is_error,omitempty"`
	StartTime         time.Time           `json:"start_time"`
	EndTime           time.Time           `json:"end_time"`
	FirstResponseTime *time.Time          `json:"first_response_time,omitempty"`
	Model             string              `json:"model,omitempty"`
	Input             any                 `json:"input,omitempty"`
	Output            any                 `json:"output,omitempty"`
	Metadata          map[string]any      `json:"metadata,omitempty"`
	Usage             *LangfuseUsage      `json:"usage,omitempty"`
	Level             string              `json:"level,omitempty"`
	TaskResult        *LangfuseTaskResult `json:"task_result,omitempty"`
}

type langfuseManager struct {
	mu      sync.RWMutex
	tp      *sdktrace.TracerProvider
	tracer  oteltrace.Tracer
	enabled bool
	baseURL string
}

var globalLangfuseManager = &langfuseManager{}

func InitLangfuse() {
	globalLangfuseManager.reload()
}

func ReloadLangfuse() {
	globalLangfuseManager.reload()
}

func (m *langfuseManager) reload() {
	setting := sanitizedLangfuseSetting()

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.tp != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := m.tp.Shutdown(ctx); err != nil {
			common.SysError("langfuse otel shutdown failed: " + err.Error())
		}
		cancel()
		m.tp = nil
		m.tracer = nil
	}

	if !langfuseConfigured(setting) {
		m.enabled = false
		m.baseURL = ""
		common.SysLog("langfuse disabled or not configured, otel exporter stopped")
		return
	}

	tp, tracer, err := newLangfuseTracerProvider(setting)
	if err != nil {
		m.enabled = false
		m.baseURL = ""
		common.SysError("langfuse otel init failed: " + err.Error())
		return
	}

	m.tp = tp
	m.tracer = tracer
	m.enabled = true
	m.baseURL = setting.BaseURL
	common.SysLog(fmt.Sprintf("langfuse otel enabled, base_url=%s, max_queue=%d, timeout=%ds, worker_count=%d(ignored by otel batch processor)", setting.BaseURL, setting.MaxQueueSize, setting.TimeoutSeconds, setting.WorkerCount))
}

func langfuseConfigured(setting *system_setting.LangfuseSetting) bool {
	if setting == nil {
		return false
	}

	if !setting.Enabled {
		return false
	}
	return strings.TrimSpace(setting.BaseURL) != "" && strings.TrimSpace(setting.PublicKey) != "" && strings.TrimSpace(setting.SecretKey) != ""
}

func sanitizedLangfuseSetting() *system_setting.LangfuseSetting {
	cfg := *system_setting.GetLangfuseSetting()
	cfg.BaseURL = strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if cfg.TimeoutSeconds <= 0 {
		cfg.TimeoutSeconds = 5
	}
	if cfg.MaxQueueSize <= 0 {
		cfg.MaxQueueSize = 256
	}
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 2
	}
	return &cfg
}

func newLangfuseTracerProvider(setting *system_setting.LangfuseSetting) (*sdktrace.TracerProvider, oteltrace.Tracer, error) {
	if err := validateLangfuseBaseURL(setting.BaseURL); err != nil {
		return nil, nil, err
	}

	parsedURL, err := url.Parse(setting.BaseURL)
	if err != nil {
		return nil, nil, fmt.Errorf("parse langfuse base url: %w", err)
	}

	otlpPath := strings.TrimRight(parsedURL.Path, "/") + langfuseOTLPPath
	clientOpts := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(parsedURL.Host),
		otlptracehttp.WithURLPath(otlpPath),
		otlptracehttp.WithHeaders(map[string]string{
			"Authorization":                "Basic " + basicLangfuseAuth(setting.PublicKey, setting.SecretKey),
			"x-langfuse-ingestion-version": langfuseIngestionVersion,
		}),
		otlptracehttp.WithTimeout(time.Duration(setting.TimeoutSeconds) * time.Second),
	}
	if strings.EqualFold(parsedURL.Scheme, "http") {
		clientOpts = append(clientOpts, otlptracehttp.WithInsecure())
	}

	exporter, err := otlptracehttp.New(context.Background(), clientOpts...)
	if err != nil {
		return nil, nil, fmt.Errorf("create otlp http exporter: %w", err)
	}

	res, err := resource.New(
		context.Background(),
		resource.WithAttributes(
			attribute.String("service.name", "new-api"),
			attribute.String("service.version", common.Version),
		),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create otel resource: %w", err)
	}

	processor := sdktrace.NewBatchSpanProcessor(
		&loggingSpanExporter{baseURL: setting.BaseURL, exporter: exporter},
		sdktrace.WithMaxQueueSize(setting.MaxQueueSize),
		sdktrace.WithExportTimeout(time.Duration(setting.TimeoutSeconds)*time.Second),
	)
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(processor),
	)
	return provider, provider.Tracer(langfuseTracerName), nil
}

type loggingSpanExporter struct {
	baseURL  string
	exporter sdktrace.SpanExporter
}

func (e *loggingSpanExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if len(spans) == 0 {
		return nil
	}
	common.SysLog(fmt.Sprintf("sending langfuse otel spans: count=%d endpoint=%s%s", len(spans), e.baseURL, langfuseOTLPPath))
	err := e.exporter.ExportSpans(ctx, spans)
	if err != nil {
		return fmt.Errorf("export langfuse otel spans failed: %w", err)
	}
	common.SysLog(fmt.Sprintf("langfuse otel spans sent: count=%d endpoint=%s%s", len(spans), e.baseURL, langfuseOTLPPath))
	return nil
}

func (e *loggingSpanExporter) Shutdown(ctx context.Context) error {
	return e.exporter.Shutdown(ctx)
}

func (e *loggingSpanExporter) ForceFlush(ctx context.Context) error {
	if flusher, ok := e.exporter.(interface{ ForceFlush(context.Context) error }); ok {
		return flusher.ForceFlush(ctx)
	}
	return nil
}

func EnqueueLangfuseTrace(snapshot *LangfuseTraceSnapshot) {
	if snapshot == nil {
		return
	}
	globalLangfuseManager.mu.RLock()
	tracer := globalLangfuseManager.tracer
	enabled := globalLangfuseManager.enabled
	globalLangfuseManager.mu.RUnlock()
	if !enabled || tracer == nil {
		return
	}

	attrs := buildLangfuseOTelAttributes(snapshot)
	ctx := context.Background()
	ctx, span := tracer.Start(ctx, snapshot.Name, oteltrace.WithTimestamp(snapshot.StartTime))
	span.SetAttributes(attrs...)
	if snapshot.IsError {
		span.SetStatus(codes.Error, snapshot.StatusMessage)
	}
	span.End(oteltrace.WithTimestamp(snapshot.EndTime))
	_ = ctx

	common.SysLog(fmt.Sprintf("langfuse otel span queued: request_id=%s name=%s", snapshot.RequestID, snapshot.Name))
}

func buildLangfuseOTelAttributes(snapshot *LangfuseTraceSnapshot) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 48)
	appendStr := func(key, value string) {
		value = strings.TrimSpace(value)
		if value != "" {
			attrs = append(attrs, attribute.String(key, value))
		}
	}
	appendInt := func(key string, value int) {
		if value != 0 {
			attrs = append(attrs, attribute.Int(key, value))
		}
	}
	appendInt64 := func(key string, value int64) {
		if value != 0 {
			attrs = append(attrs, attribute.Int64(key, value))
		}
	}

	appendStr("langfuse.trace.name", snapshot.Name)
	appendStr("langfuse.user.id", snapshot.UserID)
	appendStr("langfuse.session.id", snapshot.SessionID)
	appendStr("request_id", snapshot.RequestID)
	appendStr("http.method", snapshot.Method)
	appendStr("http.route", snapshot.Route)
	appendStr("langfuse.observation.type", langfuseObservationType(snapshot))
	appendStr("langfuse.observation.level", snapshot.Level)
	appendStr("langfuse.observation.model.name", snapshot.Model)
	appendStr("gen_ai.request.model", snapshot.Model)
	appendInt("http.status_code", snapshot.StatusCode)
	appendInt64("latency.ms", snapshot.EndTime.Sub(snapshot.StartTime).Milliseconds())
	if snapshot.FirstResponseTime != nil {
		appendStr("langfuse.observation.completion_start_time", snapshot.FirstResponseTime.UTC().Format(time.RFC3339Nano))
		appendInt64("first_token_latency.ms", snapshot.FirstResponseTime.Sub(snapshot.StartTime).Milliseconds())
	}

	if input := toLangfuseJSONString(snapshot.Input); input != "" {
		appendStr("langfuse.trace.input", input)
		appendStr("langfuse.observation.input", input)
	}
	if output := toLangfuseJSONString(snapshot.Output); output != "" {
		appendStr("langfuse.trace.output", output)
		appendStr("langfuse.observation.output", output)
	}
	if taskResult := toLangfuseJSONString(snapshot.TaskResult); taskResult != "" {
		appendStr("langfuse.observation.metadata.task_result", taskResult)
	}
	appendStr("langfuse.observation.status_message", snapshot.StatusMessage)
	appendTraceMetadataAttr(&attrs, "request_id", snapshot.RequestID)
	appendTraceMetadataAttr(&attrs, "route", snapshot.Route)
	appendTraceMetadataAttr(&attrs, "method", snapshot.Method)
	appendObservationMetadataAttr(&attrs, "status_code", snapshot.StatusCode)
	appendObservationMetadataAttr(&attrs, "latency_ms", snapshot.EndTime.Sub(snapshot.StartTime).Milliseconds())
	if snapshot.FirstResponseTime != nil {
		appendObservationMetadataAttr(&attrs, "first_token_latency_ms", snapshot.FirstResponseTime.Sub(snapshot.StartTime).Milliseconds())
	}
	appendSelectedMetadataAttrs(&attrs, "langfuse.trace.metadata.", snapshot.Metadata, []string{
		"relay_format",
		"relay_mode",
		"user_group",
		"using_group",
		"token_id",
		"token_name",
		"channel_id",
		"channel_type",
		"channel_name",
		"upstream_model_name",
		"origin_model_name",
		"billing_source",
		"task_platform",
		"task_action",
	})
	appendSelectedMetadataAttrs(&attrs, "langfuse.observation.metadata.", snapshot.Metadata, []string{
		"request_content_type",
		"response_content_type",
		"status_code",
		"is_stream",
		"retry_index",
		"use_channel",
		"request_conversion_chain",
		"send_response_count",
		"received_response_count",
		"estimate_prompt_tokens",
		"first_response_latency_ms",
		"duration_ms",
		"price_data",
		"other_ratios",
		"final_preconsumed_quota",
		"error_code",
		"error_type",
	})
	appendUsefulOperationalAttrs(&attrs, snapshot.Metadata)
	if usage := snapshot.Usage; usage != nil {
		appendInt("gen_ai.usage.input_tokens", nonZeroInt(usage.InputTokens, usage.PromptTokens))
		appendInt("gen_ai.usage.output_tokens", nonZeroInt(usage.OutputTokens, usage.CompletionTokens))
		appendInt("gen_ai.usage.total_tokens", usage.TotalTokens)
		appendObservationMetadataAttr(&attrs, "quota", usage.Quota)
		appendStr("langfuse.observation.usage_details", langfuseUsageDetailsJSON(usage))
	}
	return attrs
}

func langfuseObservationType(snapshot *LangfuseTraceSnapshot) string {
	if snapshot == nil {
		return "span"
	}
	if strings.TrimSpace(snapshot.Model) != "" {
		return "generation"
	}
	return "span"
}

func nonZeroInt(v int, fallback int) int {
	if v != 0 {
		return v
	}
	return fallback
}

func toLangfuseJSONString(v any) string {
	if v == nil {
		return ""
	}
	switch value := v.(type) {
	case string:
		return strings.TrimSpace(value)
	default:
		data, err := common.Marshal(v)
		if err != nil {
			return ""
		}
		return string(data)
	}
}

func appendTraceMetadataAttr(attrs *[]attribute.KeyValue, key string, value any) {
	appendLangfuseMetadataAttr(attrs, "langfuse.trace.metadata."+key, value)
}

func appendObservationMetadataAttr(attrs *[]attribute.KeyValue, key string, value any) {
	appendLangfuseMetadataAttr(attrs, "langfuse.observation.metadata."+key, value)
}

func appendSelectedMetadataAttrs(attrs *[]attribute.KeyValue, prefix string, metadata map[string]any, keys []string) {
	if len(metadata) == 0 {
		return
	}
	for _, key := range keys {
		value, ok := metadata[key]
		if !ok {
			continue
		}
		appendLangfuseMetadataAttr(attrs, prefix+key, value)
	}
}

func appendLangfuseMetadataAttr(attrs *[]attribute.KeyValue, key string, value any) {
	if attrs == nil {
		return
	}
	if valueStr := langfuseMetadataString(value); valueStr != "" {
		*attrs = append(*attrs, attribute.String(key, valueStr))
	}
}

func langfuseMetadataString(v any) string {
	if v == nil {
		return ""
	}
	switch value := v.(type) {
	case string:
		return strings.TrimSpace(value)
	case fmt.Stringer:
		return strings.TrimSpace(value.String())
	default:
		data, err := common.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(data)
	}
}

func appendUsefulOperationalAttrs(attrs *[]attribute.KeyValue, metadata map[string]any) {
	if len(metadata) == 0 || attrs == nil {
		return
	}
	appendStringAttrFromMetadata(attrs, "relay.format", metadata, "relay_format")
	appendIntAttrFromMetadata(attrs, "relay.mode", metadata, "relay_mode")
	appendBoolAttrFromMetadata(attrs, "relay.stream", metadata, "is_stream")
	appendIntAttrFromMetadata(attrs, "relay.retry_index", metadata, "retry_index")
	appendStringAttrFromMetadata(attrs, "error.code", metadata, "error_code")
	appendStringAttrFromMetadata(attrs, "error.type", metadata, "error_type")
}

func appendStringAttrFromMetadata(attrs *[]attribute.KeyValue, attrKey string, metadata map[string]any, metadataKey string) {
	value, ok := metadata[metadataKey]
	if !ok {
		return
	}
	if valueStr := langfuseMetadataString(value); valueStr != "" {
		*attrs = append(*attrs, attribute.String(attrKey, valueStr))
	}
}

func appendIntAttrFromMetadata(attrs *[]attribute.KeyValue, attrKey string, metadata map[string]any, metadataKey string) {
	value, ok := metadata[metadataKey]
	if !ok {
		return
	}
	switch v := value.(type) {
	case int:
		*attrs = append(*attrs, attribute.Int(attrKey, v))
	case int64:
		*attrs = append(*attrs, attribute.Int64(attrKey, v))
	case float64:
		*attrs = append(*attrs, attribute.Int64(attrKey, int64(v)))
	}
}

func appendBoolAttrFromMetadata(attrs *[]attribute.KeyValue, attrKey string, metadata map[string]any, metadataKey string) {
	value, ok := metadata[metadataKey]
	if !ok {
		return
	}
	if v, ok := value.(bool); ok {
		*attrs = append(*attrs, attribute.Bool(attrKey, v))
	}
}

func langfuseUsageDetailsJSON(usage *LangfuseUsage) string {
	if usage == nil {
		return ""
	}
	details := map[string]any{
		"input":  nonZeroInt(usage.InputTokens, usage.PromptTokens),
		"output": nonZeroInt(usage.OutputTokens, usage.CompletionTokens),
		"total":  usage.TotalTokens,
	}
	if usage.Quota != 0 {
		details["quota"] = usage.Quota
	}
	if len(usage.Details) > 0 {
		details["details"] = usage.Details
	}
	return langfuseMetadataString(details)
}

func SetLangfuseUsage(c *gin.Context, usage *LangfuseUsage) {
	if c == nil || usage == nil {
		return
	}
	c.Set(langfuseContextKeyUsage, usage)
}

func SetLangfuseTaskResult(c *gin.Context, result *LangfuseTaskResult) {
	if c == nil || result == nil {
		return
	}
	c.Set(langfuseContextKeyTaskResult, result)
}

func GetLangfuseUsage(c *gin.Context) *LangfuseUsage {
	if c == nil {
		return nil
	}
	if v, ok := c.Get(langfuseContextKeyUsage); ok {
		if usage, ok := v.(*LangfuseUsage); ok {
			return usage
		}
	}
	return nil
}

func GetLangfuseTaskResult(c *gin.Context) *LangfuseTaskResult {
	if c == nil {
		return nil
	}
	if v, ok := c.Get(langfuseContextKeyTaskResult); ok {
		if result, ok := v.(*LangfuseTaskResult); ok {
			return result
		}
	}
	return nil
}

type LangfuseResponseCapture struct {
	gin.ResponseWriter
	body bytes.Buffer
}

func NewLangfuseResponseCapture(w gin.ResponseWriter) *LangfuseResponseCapture {
	return &LangfuseResponseCapture{ResponseWriter: w}
}

func (w *LangfuseResponseCapture) Write(data []byte) (int, error) {
	_, _ = w.body.Write(data)
	return w.ResponseWriter.Write(data)
}

func (w *LangfuseResponseCapture) WriteString(s string) (int, error) {
	_, _ = w.body.WriteString(s)
	return w.ResponseWriter.WriteString(s)
}

func (w *LangfuseResponseCapture) BodyBytes() []byte {
	return w.body.Bytes()
}

func BuildLangfuseRelaySnapshot(c *gin.Context, info *relaycommon.RelayInfo, apiErr *types.NewAPIError, capture *LangfuseResponseCapture) *LangfuseTraceSnapshot {
	if c == nil || info == nil {
		return nil
	}

	endTime := time.Now()
	requestID := info.RequestId
	if requestID == "" {
		requestID = c.GetString(common.RequestIdKey)
	}
	if requestID == "" {
		requestID = uuid.NewString()
	}

	requestInput, requestContentType := requestPayloadFromContext(c)
	responseOutput, responseContentType := responsePayloadFromCapture(capture)
	statusCode := http.StatusOK
	statusMessage := "ok"
	isError := false
	level := "DEFAULT"
	if capture != nil && capture.Status() > 0 {
		statusCode = capture.Status()
	}
	if apiErr != nil {
		statusCode = apiErr.StatusCode
		statusMessage = apiErr.Error()
		isError = true
		level = "ERROR"
	}
	if statusCode >= 400 {
		isError = true
		if level != "ERROR" {
			level = "WARNING"
		}
	}

	metadata := langfuseMetadataFromRelay(c, info, statusCode, requestContentType, responseContentType, apiErr)
	if taskResult := GetLangfuseTaskResult(c); taskResult != nil {
		metadata["task_platform"] = taskResult.Platform
		metadata["task_action"] = taskResult.Action
	}

	snapshot := &LangfuseTraceSnapshot{
		RequestID:     requestID,
		Name:          langfuseTraceName(c, info),
		UserID:        fmt.Sprintf("%d", info.UserId),
		SessionID:     fmt.Sprintf("token:%d", info.TokenId),
		Route:         info.RequestURLPath,
		Method:        c.Request.Method,
		StatusCode:    statusCode,
		StatusMessage: statusMessage,
		IsError:       isError,
		StartTime:     info.StartTime,
		EndTime:       endTime,
		Model:         langfuseModelName(info),
		Input:         requestInput,
		Output:        responseOutput,
		Metadata:      metadata,
		Usage:         GetLangfuseUsage(c),
		Level:         level,
		TaskResult:    GetLangfuseTaskResult(c),
	}
	if info.HasSendResponse() {
		firstResponse := info.FirstResponseTime
		snapshot.FirstResponseTime = &firstResponse
	}
	return snapshot
}

func langfuseTraceName(c *gin.Context, info *relaycommon.RelayInfo) string {
	if info == nil {
		return "relay"
	}
	parts := []string{"relay"}
	if c != nil && c.Request != nil {
		parts = append(parts, c.Request.Method)
	}
	if info.OriginModelName != "" {
		parts = append(parts, info.OriginModelName)
	}
	if info.RelayMode > 0 {
		parts = append(parts, fmt.Sprintf("mode:%d", info.RelayMode))
	}
	return strings.Join(parts, " ")
}

func langfuseModelName(info *relaycommon.RelayInfo) string {
	if info == nil {
		return ""
	}
	if info.ChannelMeta != nil && strings.TrimSpace(info.UpstreamModelName) != "" {
		return info.UpstreamModelName
	}
	return info.OriginModelName
}

func requestPayloadFromContext(c *gin.Context) (any, string) {
	if c == nil || c.Request == nil {
		return nil, ""
	}
	storage, err := common.GetBodyStorage(c)
	if err != nil {
		return nil, c.ContentType()
	}
	payload, err := storage.Bytes()
	if err != nil {
		return nil, c.ContentType()
	}
	return normalizeRequestPayload(payload, c.ContentType()), c.ContentType()
}

func responsePayloadFromCapture(capture *LangfuseResponseCapture) (any, string) {
	if capture == nil {
		return nil, ""
	}
	contentType := capture.Header().Get("Content-Type")
	return normalizePayload(capture.BodyBytes(), contentType), contentType
}

func normalizePayload(payload []byte, contentType string) any {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 {
		return nil
	}
	if strings.Contains(contentType, "application/octet-stream") || strings.Contains(contentType, "audio/") || strings.Contains(contentType, "image/") || strings.Contains(contentType, "video/") {
		return nil
	}
	if strings.Contains(contentType, "application/json") || strings.Contains(contentType, "+json") {
		var parsed any
		if err := common.Unmarshal(trimmed, &parsed); err == nil {
			return parsed
		}
	}
	if strings.Contains(contentType, "text/event-stream") || strings.Contains(contentType, "text/") || utf8.Valid(trimmed) {
		return string(trimmed)
	}
	return nil
}

func normalizeRequestPayload(payload []byte, contentType string) any {
	normalized := normalizePayload(payload, contentType)
	currentInput, ok := normalized.(map[string]any)
	if !ok {
		return normalized
	}
	if messages, exists := currentInput["messages"]; exists && messages != nil {
		return messages
	}
	return currentInput
}

func langfuseMetadataFromRelay(c *gin.Context, info *relaycommon.RelayInfo, statusCode int, requestContentType, responseContentType string, apiErr *types.NewAPIError) map[string]any {
	channelID := 0
	channelType := 0
	channelBaseURL := ""
	upstreamModelName := ""
	if info.ChannelMeta != nil {
		channelID = info.ChannelId
		channelType = info.ChannelType
		channelBaseURL = info.ChannelBaseUrl
		upstreamModelName = info.UpstreamModelName
	}
	groupRatio := info.PriceData.GroupRatioInfo.GroupRatio

	metadata := map[string]any{
		"relay_format":              info.RelayFormat,
		"relay_mode":                info.RelayMode,
		"request_path":              info.RequestURLPath,
		"request_content_type":      requestContentType,
		"response_content_type":     responseContentType,
		"status_code":               statusCode,
		"is_stream":                 info.IsStream,
		"retry_index":               info.RetryIndex,
		"using_group":               info.UsingGroup,
		"user_group":                info.UserGroup,
		"token_id":                  info.TokenId,
		"token_name":                c.GetString("token_name"),
		"channel_id":                channelID,
		"channel_type":              channelType,
		"channel_base_url":          channelBaseURL,
		"channel_name":              c.GetString("channel_name"),
		"upstream_model_name":       upstreamModelName,
		"origin_model_name":         info.OriginModelName,
		"request_conversion_chain":  info.RequestConversionChain,
		"send_response_count":       info.SendResponseCount,
		"received_response_count":   info.ReceivedResponseCount,
		"estimate_prompt_tokens":    info.GetEstimatePromptTokens(),
		"first_response_latency_ms": latencyMilliseconds(info.StartTime, info.FirstResponseTime),
		"duration_ms":               endDurationMs(info.StartTime),
		"price_data": map[string]any{
			"use_price":        info.PriceData.UsePrice,
			"model_price":      info.PriceData.ModelPrice,
			"model_ratio":      info.PriceData.ModelRatio,
			"group_ratio":      groupRatio,
			"quota":            info.PriceData.Quota,
			"quota_preconsume": info.PriceData.QuotaToPreConsume,
		},
	}
	useChannel := c.GetStringSlice("use_channel")
	if len(useChannel) > 0 {
		metadata["use_channel"] = useChannel
	}
	if info.BillingSource != "" {
		metadata["billing_source"] = info.BillingSource
	}
	if info.FinalPreConsumedQuota > 0 {
		metadata["final_preconsumed_quota"] = info.FinalPreConsumedQuota
	}
	if len(info.PriceData.OtherRatios) > 0 {
		metadata["other_ratios"] = info.PriceData.OtherRatios
	}
	if apiErr != nil {
		metadata["error_code"] = apiErr.GetErrorCode()
		metadata["error_type"] = apiErr.GetErrorType()
	}
	return metadata
}

func latencyMilliseconds(startTime, firstResponseTime time.Time) int64 {
	if startTime.IsZero() || firstResponseTime.IsZero() || !firstResponseTime.After(startTime) {
		return 0
	}
	return firstResponseTime.Sub(startTime).Milliseconds()
}

func endDurationMs(startTime time.Time) int64 {
	if startTime.IsZero() {
		return 0
	}
	return time.Since(startTime).Milliseconds()
}

func validateLangfuseBaseURL(urlStr string) error {
	fetchSetting := system_setting.GetFetchSetting()
	return common.ValidateURLWithFetchSetting(urlStr, fetchSetting.EnableSSRFProtection, fetchSetting.AllowPrivateIp, fetchSetting.DomainFilterMode, fetchSetting.IpFilterMode, fetchSetting.DomainList, fetchSetting.IpList, fetchSetting.AllowedPorts, fetchSetting.ApplyIPFilterForDomain)
}

func ValidateLangfuseBaseURLForOption(urlStr string) error {
	trimmed := strings.TrimSpace(urlStr)
	if trimmed == "" {
		return nil
	}
	return validateLangfuseBaseURL(trimmed)
}

func basicLangfuseAuth(publicKey, secretKey string) string {
	return base64.StdEncoding.EncodeToString([]byte(strings.TrimSpace(publicKey) + ":" + strings.TrimSpace(secretKey)))
}

func BuildTaskLangfuseResult(info *relaycommon.RelayInfo, result any, quota int) *LangfuseTaskResult {
	langfuseResult := &LangfuseTaskResult{Quota: quota}
	if info != nil {
		langfuseResult.Action = info.Action
	}
	switch v := result.(type) {
	case map[string]any:
		langfuseResult.TaskData = v
	}
	return langfuseResult
}

func NewLangfuseTaskSubmitResult(info *relaycommon.RelayInfo, platform string, upstreamTaskID string, quota int, taskData []byte) *LangfuseTaskResult {
	langfuseResult := BuildTaskLangfuseResult(info, nil, quota)
	langfuseResult.Platform = platform
	langfuseResult.UpstreamTaskID = upstreamTaskID
	langfuseResult.TaskData = normalizePayload(taskData, "application/json")
	langfuseResult.Metadata = map[string]any{"quota": quota}
	return langfuseResult
}

func SetLangfuseTaskQuota(c *gin.Context, quota int) {
	if c == nil {
		return
	}
	c.Set(langfuseContextKeyTaskQuota, quota)
}

func GetLangfuseTaskQuota(c *gin.Context) int {
	if c == nil {
		return 0
	}
	if v, ok := c.Get(langfuseContextKeyTaskQuota); ok {
		if quota, ok := v.(int); ok {
			return quota
		}
	}
	return 0
}

func LangfuseUsageFromDTO(usage *dto.Usage, quota int, details map[string]any) *LangfuseUsage {
	if usage == nil {
		return &LangfuseUsage{Quota: quota, Details: details}
	}
	return &LangfuseUsage{
		PromptTokens:     usage.PromptTokens,
		CompletionTokens: usage.CompletionTokens,
		TotalTokens:      usage.TotalTokens,
		InputTokens:      usage.InputTokens,
		OutputTokens:     usage.OutputTokens,
		Quota:            quota,
		Details:          details,
	}
}

func LangfuseUsageFromRealtime(usage *dto.RealtimeUsage, quota int, details map[string]any) *LangfuseUsage {
	if usage == nil {
		return &LangfuseUsage{Quota: quota, Details: details}
	}
	return &LangfuseUsage{
		PromptTokens:     usage.InputTokenDetails.TextTokens + usage.InputTokenDetails.AudioTokens,
		CompletionTokens: usage.OutputTokenDetails.TextTokens + usage.OutputTokenDetails.AudioTokens,
		TotalTokens:      usage.TotalTokens,
		InputTokens:      usage.InputTokenDetails.TextTokens + usage.InputTokenDetails.AudioTokens,
		OutputTokens:     usage.OutputTokenDetails.TextTokens + usage.OutputTokenDetails.AudioTokens,
		Quota:            quota,
		Details:          details,
	}
}

func IsLangfuseEnabled() bool {
	globalLangfuseManager.mu.RLock()
	defer globalLangfuseManager.mu.RUnlock()
	return globalLangfuseManager.enabled
}

func LangfuseRouteTag(c *gin.Context) string {
	if c == nil || c.Request == nil {
		return "relay"
	}
	return c.Request.Method + " " + c.Request.URL.Path
}

func SetLangfuseRelayMetadata(c *gin.Context, info *relaycommon.RelayInfo, capture *LangfuseResponseCapture, apiErr *types.NewAPIError) {
	EnqueueLangfuseTrace(BuildLangfuseRelaySnapshot(c, info, apiErr, capture))
}

func ContextRequestStartTime(c *gin.Context) time.Time {
	if c == nil {
		return time.Time{}
	}
	return common.GetContextKeyTime(c, constant.ContextKeyRequestStartTime)
}
