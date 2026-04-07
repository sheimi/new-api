package system_setting

import "github.com/QuantumNous/new-api/setting/config"

type LangfuseSetting struct {
	Enabled        bool   `json:"enabled"`
	BaseURL        string `json:"base_url"`
	PublicKey      string `json:"public_key"`
	SecretKey      string `json:"secret_key"`
	TimeoutSeconds int    `json:"timeout_seconds"`
	MaxQueueSize   int    `json:"max_queue_size"`
	WorkerCount    int    `json:"worker_count"`
}

var defaultLangfuseSetting = LangfuseSetting{
	Enabled:        false,
	BaseURL:        "",
	PublicKey:      "",
	SecretKey:      "",
	TimeoutSeconds: 5,
	MaxQueueSize:   256,
	WorkerCount:    2,
}

func init() {
	config.GlobalConfig.Register("langfuse", &defaultLangfuseSetting)
}

func GetLangfuseSetting() *LangfuseSetting {
	return &defaultLangfuseSetting
}
