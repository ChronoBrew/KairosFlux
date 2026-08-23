# RFC：时态内核 M0 —— 版本化写入、as-of 查询、自校验指纹

状态：**实现中**（`internal/temporal` 已落地纯语义层 + 测试）

## 目标

让 BanDB 具备时态内核的三个地基能力：

1. **写不覆盖**：同一逻辑记录的每次写入都是不可变版本；
2. **读按 as-of**：查询可以问“在某个时刻，系统当时知道什么”；
3. **状态可重放校验**：任何状态都能从版本集合重放出来，指纹一致才算数。

这是“AI 原生时序数据引擎”的数据模型地基：AI/Agent 只能读可验证的时点真相，
写入永远留下审计轨迹。

## 关键决策

### 1. 键布局

```
逻辑键（业务不变）      quote:2026-08-17:600000
版本存储键              quote:2026-08-17:600000:v0000000000000003
当前指针键              quote:2026-08-17:600000:current
```

- `seq` 是同一逻辑键内的严格递增写入序号（总序）；
- 版本号定宽 20 位十进制，保证**字典序 = 数值序**（LSM 扫描天然按版本升序）；
- `:current` 指针存 `(seq, payload_hash)`，供快速 GET 与对账；
- 崩溃安全：先写版本键，再写指针键；指针永远指向已落盘的版本，孤儿版本可回收。

### 2. as-of 语义

`AsOf(versions, as_of)` 返回“写入时间 <= as_of”的版本中 `seq` 最大的那一条；
同写入时刻按 `seq` 决胜。**绝不返回未来写入**——这是 PIT 红线的数据层保证。

### 3. 自校验指纹

`Fingerprint(entries)` 对 `(LogicalKey, Seq, Payload)` 做确定性 sha256：

- 按 `(LogicalKey, Seq)` 排序，与输入顺序无关；
- 每条记录带 payload 长度前缀，消除键/负载边界歧义；
- 用途：重放全量版本后对最新状态做指纹，与 `:current` 指针比对；跨进程验证
  “同一份账本产生同一状态”。

## 兼容性

- 不改现有 PUT/GET/SCAN/DELETE 帧格式与存储格式；
- 不改现有 `quote:` schema 校验；
- `internal/temporal` 是纯包，未接线到 router，现有行为零回归。

## 验收

- `go test ./internal/temporal/ -v` 全绿；
- 版本键 round-trip、定宽字典序 = 数值序；
- `Latest` / `AsOf` 语义（含未来写入不可见、同刻决胜）；
- 指纹确定性、顺序无关、长度前缀无歧义。

## 下一步

1. BANLV 接线：`PUT_VERSIONED` / `GET_AS_OF` / `LIST_VERSIONS` / `FINGERPRINT`；
2. schema registry 数据化（机器可读契约，含版本）；
3. QuantScout 版本化写入迁移（快照/财报/筛选/推荐）；
4. QuantBrew 侧 as-of 数据适配器。
