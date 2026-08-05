# 迭代复盘：网关自适应并发准入——用延迟反馈自探容量，过载 shed 而非崩

日期：2026-08-05　范围：`pkg/admission`、`service/router.go`

## 背景与定位（和存储背压区分清楚）

给网关做一个"高级"优化。现有 `pkg/credit` 是**存储层字节背压**：MemTable 写路径按未刷盘字节**阻塞**，管的是**内存边界**。本次做的是**网关层自适应并发准入**：在网络入口按"并发在途请求数"准入，用**请求延迟反馈自适应探测系统容量上限**（类 TCP 拥塞控制 / Netflix gradient limiter），过载时**快速 shed（拒绝）** 而非无限排队阻塞。两者不同层、互补——准入在过载真正压垮存储/内存之前，就把超额请求挡在网关门外。

## 算法（gradient 自适应）

以观测到的最优 RTT（`minRTT`，周期性重探以跟随基准漂移）为"无排队基准"，采样 RTT 越大说明排队越重：

```
gradient = clamp(minRTT / sampleRTT, 0.5, 1)   // ≤1，越小排队越重
newLimit = limit*gradient + sqrt(limit)         // 排队重则收缩；轻则借 headroom 扩张
limit    = EWMA平滑(limit, newLimit)，clamp [min,max]
```

准入：在途 ≥ ⌊limit⌋ 即 shed。无固定阈值——上限随实测延迟自适应上下浮动。

## 接线

`Router.Handle` 入口先 `Acquire`：过载则回 `overloaded` 状态（客户端可退避重试）不进处理，否则 `defer Release` 记录 RTT 反哺算法。`config.AdmissionEnabled` 开关（默认关），`pkg/metrics` 加 `admission_shed` 计数。

## 验证：闭环过载压测（measure-first）

`TestLimiter_ClosedLoopBoundsConcurrencyUnderOverload`：200 并发打一个"延迟随并发上升"的模拟服务（排队模型 `延迟=base×(1+并发/10)`），对比无限流 vs 自适应准入：

| | 峰值并发 | 平均延迟 | shed | 收敛 limit |
| --- | --- | --- | --- | --- |
| 无限流 | 200 | **20.5 ms** | — | — |
| 自适应准入 | **51** | **1.58 ms**（约 13×↓） | 大量 | 8 |

限流器把并发从 200 压到 ~51、平均延迟降 ~13 倍、超额快速 shed、并自适应收敛到操作点。单测另覆盖：低延迟涨、高延迟缩、满载 shed。`-race` 干净。

## 诚实边界

- **延迟模型偏悲观**：本压测的延迟随并发**线性**上升（任何并发都加延迟），故 gradient 持续收缩、收敛 limit 偏低（8），牺牲了部分吞吐换低延迟。真实系统延迟曲线在容量拐点前较平，limiter 会找到更高的操作点。这是"延迟 vs 吞吐"的固有权衡，也是延迟型限流器的特性。
- **准入粒度**：当前对所有请求一视同仁；按操作类型/租户分级准入是后续。
- **与存储背压的协同**：两者独立生效；把准入信号与背压信号联动（如背压深时更激进 shed）是可做的进阶。
