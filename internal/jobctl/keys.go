// Package jobctl 实现 M3 对象模型与声明式 Job 控制面（docs/方案-BanDB-
// 时态内核与AI数据平面.md §M3）：daily 流水线从 shell 串联升级为"声明式
// 任务 + 本地 reconcile 循环"。Job 对象长在 KairosFlux 键空间里（job:spec/
// job:status/job:events 三段），走既有 v2 opcode（PUT_VERSIONED/GET_AS_OF），
// 不新增 opcode、不引入 k8s/Ray/cron 库。
package jobctl

import "fmt"

// SpecKey/StatusKey/EventsKey 是 job: 键空间的三段布局（契约原文）：
//
//	job:spec:{name}    期望状态（apply 写入）
//	job:status:{name}  观测状态（phase/last_run/retry/verdict）
//	job:events:{name}  事件账本（每次执行一条，走时态内核版本语义——
//	                    PUT_VERSIONED 自动分配 v{seq}，本包不自己拼 v{seq}
//	                    后缀：那是 internal/temporal.VersionStorageKey 的
//	                    职责，重复实现一遍就是本仓库明令禁止的"依赖偶然
//	                    实现细节"式胶水）。
func SpecKey(name string) string { return fmt.Sprintf("job:spec:%s", name) }

func StatusKey(name string) string { return fmt.Sprintf("job:status:%s", name) }

func EventsKey(name string) string { return fmt.Sprintf("job:events:%s", name) }
