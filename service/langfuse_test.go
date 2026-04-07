package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/setting/system_setting"
	"github.com/QuantumNous/new-api/types"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
)

func TestSanitizedLangfuseSettingDefaults(t *testing.T) {
	orig := *system_setting.GetLangfuseSetting()
	defer func() { *system_setting.GetLangfuseSetting() = orig }()

	*system_setting.GetLangfuseSetting() = system_setting.LangfuseSetting{
		Enabled:        true,
		BaseURL:        "https://langfuse.example.com/",
		PublicKey:      "pk",
		SecretKey:      "sk",
		TimeoutSeconds: 0,
		MaxQueueSize:   0,
		WorkerCount:    0,
	}

	cfg := sanitizedLangfuseSetting()
	require.NotNil(t, cfg)
	assert.Equal(t, "https://langfuse.example.com", cfg.BaseURL)
	assert.Equal(t, 5, cfg.TimeoutSeconds)
	assert.Equal(t, 256, cfg.MaxQueueSize)
	assert.Equal(t, 2, cfg.WorkerCount)
}

func TestBuildLangfuseRelaySnapshotSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", http.NoBody)
	c.Request.Header.Set("Content-Type", "application/json")
	storage, err := common.CreateBodyStorage([]byte(`{"model":"gpt-5","messages":[{"role":"user","content":"hello"}]}`))
	require.NoError(t, err)
	c.Set(common.KeyBodyStorage, storage)
	defer storage.Close()
	c.Set(common.RequestIdKey, "req-1")
	c.Set("channel_name", "primary")
	c.Set("token_name", "main-token")
	c.Set("use_channel", []string{"11", "12"})
	common.SetContextKey(c, constant.ContextKeyRequestStartTime, time.Now().Add(-2*time.Second))

	capture := NewLangfuseResponseCapture(c.Writer)
	capture.Header().Set("Content-Type", "application/json")
	_, err = capture.Write([]byte(`{"id":"resp_1","choices":[{"message":{"content":"hi"}}]}`))
	require.NoError(t, err)

	info := &relaycommon.RelayInfo{
		RequestId:             "req-1",
		UserId:                7,
		TokenId:               8,
		UsingGroup:            "default",
		UserGroup:             "default",
		OriginModelName:       "gpt-5",
		RequestURLPath:        "/v1/chat/completions",
		RelayFormat:           types.RelayFormatOpenAI,
		RelayMode:             1,
		StartTime:             time.Now().Add(-1500 * time.Millisecond),
		FirstResponseTime:     time.Now().Add(-1 * time.Second),
		SendResponseCount:     1,
		ReceivedResponseCount: 1,
		RetryIndex:            1,
		ChannelMeta: &relaycommon.ChannelMeta{
			ChannelId:         11,
			ChannelType:       1,
			ChannelBaseUrl:    "https://upstream.example.com",
			UpstreamModelName: "gpt-5",
		},
	}
	SetLangfuseUsage(c, &LangfuseUsage{PromptTokens: 10, CompletionTokens: 20, TotalTokens: 30, Quota: 99})

	snapshot := BuildLangfuseRelaySnapshot(c, info, nil, capture)
	require.NotNil(t, snapshot)
	assert.Equal(t, "req-1", snapshot.RequestID)
	assert.Equal(t, 200, snapshot.StatusCode)
	assert.Equal(t, "gpt-5", snapshot.Model)
	assert.NotNil(t, snapshot.Input)
	inputMessages, ok := snapshot.Input.([]any)
	require.True(t, ok)
	require.Len(t, inputMessages, 1)
	firstMessage, ok := inputMessages[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "user", firstMessage["role"])
	assert.Equal(t, "hello", firstMessage["content"])
	assert.NotNil(t, snapshot.Output)
	require.NotNil(t, snapshot.Usage)
	assert.Equal(t, 30, snapshot.Usage.TotalTokens)
	assert.Equal(t, []string{"11", "12"}, snapshot.Metadata["use_channel"])
	assert.Equal(t, "main-token", snapshot.Metadata["token_name"])
}

func TestBuildLangfuseRelaySnapshotFallsBackToFullInputWithoutMessages(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", http.NoBody)
	c.Request.Header.Set("Content-Type", "application/json")
	storage, err := common.CreateBodyStorage([]byte(`{"input":"hello","model":"gpt-5"}`))
	require.NoError(t, err)
	c.Set(common.KeyBodyStorage, storage)
	defer storage.Close()

	info := &relaycommon.RelayInfo{
		RequestId:       "req-fallback",
		UserId:          1,
		TokenId:         2,
		OriginModelName: "gpt-5",
		RequestURLPath:  "/v1/responses",
		StartTime:       time.Now().Add(-time.Second),
	}

	snapshot := BuildLangfuseRelaySnapshot(c, info, nil, nil)
	require.NotNil(t, snapshot)
	inputMap, ok := snapshot.Input.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "hello", inputMap["input"])
	assert.Equal(t, "gpt-5", inputMap["model"])
}

func TestBuildLangfuseRelaySnapshotError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", http.NoBody)
	storage, err := common.CreateBodyStorage([]byte(`{"input":"hello"}`))
	require.NoError(t, err)
	c.Set(common.KeyBodyStorage, storage)
	defer storage.Close()

	capture := NewLangfuseResponseCapture(c.Writer)
	capture.Header().Set("Content-Type", "application/json")
	capture.WriteHeader(http.StatusBadGateway)
	_, err = capture.Write([]byte(`{"error":{"message":"upstream down"}}`))
	require.NoError(t, err)

	info := &relaycommon.RelayInfo{
		RequestId:       "req-error",
		UserId:          1,
		TokenId:         2,
		UsingGroup:      "default",
		UserGroup:       "default",
		OriginModelName: "gpt-5",
		RequestURLPath:  "/v1/responses",
		RelayFormat:     types.RelayFormatOpenAIResponses,
		RelayMode:       2,
		StartTime:       time.Now().Add(-2 * time.Second),
	}

	apiErr := types.NewErrorWithStatusCode(assert.AnError, types.ErrorCodeBadResponseStatusCode, http.StatusBadGateway)
	snapshot := BuildLangfuseRelaySnapshot(c, info, apiErr, capture)
	require.NotNil(t, snapshot)
	assert.True(t, snapshot.IsError)
	assert.Equal(t, "ERROR", snapshot.Level)
	assert.Equal(t, http.StatusBadGateway, snapshot.StatusCode)
	assert.Equal(t, types.ErrorCodeBadResponseStatusCode, snapshot.Metadata["error_code"])

	attrs := buildLangfuseOTelAttributes(snapshot)
	attrMap := attrValuesByKey(attrs)
	assert.Equal(t, "ERROR", attrMap[attribute.Key("langfuse.observation.level")].AsString())
	assert.Contains(t, attrMap[attribute.Key("langfuse.observation.status_message")].AsString(), "assert.AnError")
}

func TestValidateLangfuseBaseURLForOption(t *testing.T) {
	fetch := system_setting.GetFetchSetting()
	orig := *fetch
	defer func() { *fetch = orig }()

	fetch.EnableSSRFProtection = true
	fetch.AllowPrivateIp = false
	fetch.AllowedPorts = []string{"80", "443", "8080", "8443", "3000"}

	err := ValidateLangfuseBaseURLForOption("http://127.0.0.1:3000")
	require.Error(t, err)

	fetch.AllowPrivateIp = true
	err = ValidateLangfuseBaseURLForOption("http://127.0.0.1:3000")
	require.NoError(t, err)
}

func TestBuildLangfuseOTelAttributesIncludesLangfuseMappings(t *testing.T) {
	firstResponse := time.Now().Add(-500 * time.Millisecond)
	snapshot := &LangfuseTraceSnapshot{
		RequestID:         "req-1",
		Name:              "relay POST gpt-5",
		UserID:            "7",
		SessionID:         "token:8",
		Method:            http.MethodPost,
		Route:             "/v1/chat/completions",
		StatusCode:        http.StatusOK,
		StatusMessage:     "ok",
		Level:             "DEFAULT",
		StartTime:         time.Now().Add(-time.Second),
		EndTime:           time.Now(),
		FirstResponseTime: &firstResponse,
		Model:             "gpt-5",
		Input:             map[string]any{"hello": "world"},
		Output:            map[string]any{"answer": "hi"},
		Usage:             &LangfuseUsage{PromptTokens: 10, CompletionTokens: 15, TotalTokens: 25, Quota: 30},
		TaskResult:        &LangfuseTaskResult{Platform: "openai", Action: "submit"},
		Metadata: map[string]any{
			"channel_name":             "primary",
			"token_name":               "main-token",
			"relay_format":             types.RelayFormatOpenAI,
			"relay_mode":               1,
			"request_content_type":     "application/json",
			"response_content_type":    "application/json",
			"status_code":              http.StatusOK,
			"is_stream":                true,
			"request_conversion_chain": []types.RelayFormat{types.RelayFormatOpenAI, types.RelayFormatOpenAIResponses},
		},
	}

	attrs := buildLangfuseOTelAttributes(snapshot)
	attrMap := attrValuesByKey(attrs)
	assert.Equal(t, "relay POST gpt-5", attrMap[attribute.Key("langfuse.trace.name")].AsString())
	assert.Equal(t, "7", attrMap[attribute.Key("langfuse.user.id")].AsString())
	assert.Equal(t, "token:8", attrMap[attribute.Key("langfuse.session.id")].AsString())
	assert.Equal(t, "generation", attrMap[attribute.Key("langfuse.observation.type")].AsString())
	assert.Equal(t, "DEFAULT", attrMap[attribute.Key("langfuse.observation.level")].AsString())
	assert.Equal(t, "gpt-5", attrMap[attribute.Key("langfuse.observation.model.name")].AsString())
	assert.Equal(t, "gpt-5", attrMap[attribute.Key("gen_ai.request.model")].AsString())
	assert.Equal(t, "req-1", attrMap[attribute.Key("langfuse.trace.metadata.request_id")].AsString())
	assert.Equal(t, "primary", attrMap[attribute.Key("langfuse.trace.metadata.channel_name")].AsString())
	assert.Equal(t, "main-token", attrMap[attribute.Key("langfuse.trace.metadata.token_name")].AsString())
	assert.Equal(t, "application/json", attrMap[attribute.Key("langfuse.observation.metadata.request_content_type")].AsString())
	assert.Equal(t, "application/json", attrMap[attribute.Key("langfuse.observation.metadata.response_content_type")].AsString())
	assert.NotEmpty(t, attrMap[attribute.Key("langfuse.observation.completion_start_time")].AsString())
	assert.Equal(t, int64(25), attrMap[attribute.Key("gen_ai.usage.total_tokens")].AsInt64())
	assert.Contains(t, attrMap[attribute.Key("langfuse.observation.usage_details")].AsString(), `"quota":30`)
	assert.Contains(t, attrMap[attribute.Key("langfuse.observation.metadata.task_result")].AsString(), `"platform":"openai"`)
	assert.Contains(t, attrMap[attribute.Key("langfuse.trace.input")].AsString(), `"hello":"world"`)
	assert.Contains(t, attrMap[attribute.Key("langfuse.trace.output")].AsString(), `"answer":"hi"`)
}

func TestNewLangfuseTracerProviderUsesLangfuseEndpointAndHeaders(t *testing.T) {
	orig := *system_setting.GetLangfuseSetting()
	defer func() { *system_setting.GetLangfuseSetting() = orig }()

	fetch := system_setting.GetFetchSetting()
	origFetch := *fetch
	defer func() { *fetch = origFetch }()
	fetch.EnableSSRFProtection = false

	requestCh := make(chan struct{}, 1)
	var gotPath string
	var gotAuth string
	var gotIngestionVersion string
	var gotBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotIngestionVersion = r.Header.Get("x-langfuse-ingestion-version")
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		gotBody = body
		w.WriteHeader(http.StatusOK)
		requestCh <- struct{}{}
	}))
	defer server.Close()

	setting := &system_setting.LangfuseSetting{
		Enabled:        true,
		BaseURL:        server.URL + "/langfuse",
		PublicKey:      "pk",
		SecretKey:      "sk",
		TimeoutSeconds: 5,
		MaxQueueSize:   16,
		WorkerCount:    2,
	}

	tp, tracer, err := newLangfuseTracerProvider(setting)
	require.NoError(t, err)
	require.NotNil(t, tp)
	require.NotNil(t, tracer)

	ctx := context.Background()
	_, span := tracer.Start(ctx, "langfuse-test-span")
	span.End()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, tp.Shutdown(ctx))
	select {
	case <-requestCh:
	case <-time.After(time.Second):
		t.Fatal("expected exporter request")
	}
	assert.Equal(t, "/langfuse/api/public/otel/v1/traces", gotPath)
	assert.Equal(t, "Basic "+basicLangfuseAuth("pk", "sk"), gotAuth)
	assert.Equal(t, langfuseIngestionVersion, gotIngestionVersion)
	assert.NotEmpty(t, gotBody)
}

func attrValuesByKey(attrs []attribute.KeyValue) map[attribute.Key]attribute.Value {
	attrMap := make(map[attribute.Key]attribute.Value, len(attrs))
	for _, kv := range attrs {
		attrMap[kv.Key] = kv.Value
	}
	return attrMap
}
