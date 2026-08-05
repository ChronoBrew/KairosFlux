package service

import (
	"log/slog"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/pkg/admission"
)

// EnableAdmissionFromConfig 按配置在 router 上开启网关自适应准入（默认关闭直接返回）。
// 开启后请求先经并发上限准入，过载则 shed（回 overloaded，客户端可退避重试），并用延迟反馈
// 自适应探测容量上限——在过载压垮存储/内存前把超额请求挡在网关门外。
func EnableAdmissionFromConfig(r *Router) {
	if !config.G.AdmissionEnabled {
		return
	}
	l := admission.New(admission.Config{
		InitialLimit: config.G.AdmissionInitialLimit,
		MinLimit:     config.G.AdmissionMinLimit,
		MaxLimit:     config.G.AdmissionMaxLimit,
	})
	r.SetLimiter(l)
	slog.Info("gateway adaptive admission enabled",
		"initial", config.G.AdmissionInitialLimit, "min", config.G.AdmissionMinLimit, "max", config.G.AdmissionMaxLimit)
}
