# KairosFlux

[English](README.md) | [中文](README.zh-CN.md)

![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)
![Go Version](https://img.shields.io/badge/go-1.26%2B-00ADD8?logo=go)
![Protocol](https://img.shields.io/badge/protocol-Kair-orange)
![Performance](https://img.shields.io/badge/Kair%20vs%20gRPC-2.7x%20faster%20(GET)-brightgreen)
<!-- CI badge: add once a GitHub Actions workflow is wired up -->

KairosFlux (formerly **BanDB**) is an AI-native temporal data engine: every
write is versioned and immutable, every read can ask "as of when", and every
state can be replayed from the version ledger and checked against its own
deterministic fingerprint. It is the temporal data-flow engine of
**ChronoBrew**, an org built around one idea — Time-series is the substance,
determinism is the differentiator, AI is the direction. QuantBrew (a
deterministic A-share backtesting kernel) and ChronoScout (a full-market
recon crawler) are its first two tenants — see
[ChronoBrew's architecture overview](https://github.com/ChronoBrew/QuantBrew/blob/main/docs/架构总览-ChronoBrew.md)
for how the three pieces fit together.

Underneath the temporal model sits a narrower, already-production substrate:
high-frequency writes headed for a data warehouse are bursty upstream and
easy to overwhelm downstream. KairosFlux absorbs the burst, validates and
cleans each record against a machine-readable contract, buffers it durably,
and (optionally) delivers it downstream at a pace the sink can handle.

Single binary. No third-party dependencies beyond gRPC/Protobuf, which is
only used for an optional benchmarking endpoint (see [Performance](#performance)).

> This repository has completed its product/protocol rename: `BanDB` →
> `KairosFlux`, protocol brand `BANLV` → `Kair`. Module path, package name
> (`bannet` → `kairnet`), `cmd/*` binaries, and docs have all moved. Frame
> format, opcodes, and cross-language test vector bytes are unchanged
> throughout, so nothing deployed today breaks. One file is deliberately
> still named `client/python/bandb_client.py` — it's slated to be retired
> outright in an upcoming cleanup rather than renamed in place, so its old
> identifiers (`BanDBClient`, `BanDBError`, ...) are left alone for now.
> Every command in this document was run against a real server while writing
> it — see [Quick Start](#quick-start).

## Why "AI-native"

Point-in-time correctness is the property an autonomous agent needs from its
storage layer and the property a plain key-value store cannot give it:

- **Writes never overwrite.** `PUT_VERSIONED` always creates a new immutable
  version of a logical key; nothing is ever mutated in place.
- **`as_of(t)` queries are point-in-time.** `GET_AS_OF(key, t)` returns the
  latest version whose write time is `<= t` and *never* a version written
  after `t` — an agent re-running an old decision sees exactly what was known
  at that instant, not what's true now.
- **State is replayable and self-checkable.** `REPLAY_FINGERPRINT` rebuilds
  the latest state of a key range from its version ledger and produces a
  deterministic SHA-256 over `(LogicalKey, Seq, Payload)`, so any two
  processes that replay the same ledger can prove they landed on the same
  state without comparing raw bytes.
- **Every write carries who/when.** Each version record carries an operation
  envelope (`seq`, `write_ts`, `source`, `schema_ver`, `payload_hash`) and is
  queryable via audit commands — "who wrote this key, and how many times, in
  this window" is an answered question, not a log-grepping exercise.
- **A data plane an agent can call directly is the direction, not a shipped
  wire feature yet.** The embedded API ([kairosflux.go](kairosflux.go))
  already exposes it: `BuildContext` (deterministic read bundles for agent
  research) and `SubmitProposal` (agent writes go through the same
  versioned/audited path). A network-visible agent plane remains [roadmap](#roadmap).

## Quick Start

Requires Go 1.26+. Every command below was run end-to-end against a real
`kairosflux-server` instance while writing this document; exit codes are
noted where they matter.

Build:

```console
$ go build ./...
```

Start a server (another terminal):

```console
$ cd cmd/kairosflux-server && go run .
...
2026/08/24 23:50:47 INFO kairnet server starting name=KairosFlux addr=127.0.0.1:8080
```

Write three versions of the same logical key — each call is a new, immutable
version, not an overwrite:

```console
$ go run ./cmd/kairosflux-cli -addr 127.0.0.1:8080 put-versioned order:1001 '{"amount":128}' scout
已写入版本 seq=1: order:1001 = {"amount":128}

$ go run ./cmd/kairosflux-cli -addr 127.0.0.1:8080 put-versioned order:1001 '{"amount":150}' scout
已写入版本 seq=2: order:1001 = {"amount":150}

$ go run ./cmd/kairosflux-cli -addr 127.0.0.1:8080 put-versioned order:1001 '{"amount":175}' scout
已写入版本 seq=3: order:1001 = {"amount":175}
```

(the trailing `scout` argument is the `source` field of the operation
envelope — "who wrote this", see [Roadmap / M2](#roadmap))

List the full version history, then read `as of` a timestamp that falls
strictly between v2 and v3's write time:

```console
$ go run ./cmd/kairosflux-cli -addr 127.0.0.1:8080 list-versions order:1001
seq=1 write_nanos=1787586686757950000 payload={"amount":128}
seq=2 write_nanos=1787586686829263000 payload={"amount":150}
seq=3 write_nanos=1787586686890053000 payload={"amount":175}

$ go run ./cmd/kairosflux-cli -addr 127.0.0.1:8080 get-as-of order:1001 1787586686850000000
seq=2 write_nanos=1787586686829263000 payload={"amount":150}
```

`as_of` returns **v2**, not v3 — even though v3 already exists in storage,
the query time falls before its write time, so it is invisible to this read.
This is the point-in-time guarantee, not an artifact of read timing.

Replay the ledger and self-check its fingerprint:

```console
$ go run ./cmd/kairosflux-cli -addr 127.0.0.1:8080 fingerprint order:
逻辑键数=1 不一致数=0 指纹=2c4e10cc1ab683b5dbcec51920641b4765d737d809e57d623018c79d8aa56788
```

`逻辑键数=1`（1 logical key）, `不一致数=0`（0 mismatches against the
`:current` pointer）, followed by the 64-hex-char SHA-256 fingerprint of the
replayed state. Two independent replays of the same ledger — including
across a server restart — produce the same 64 characters.

<details>
<summary>Write and read a market-data snapshot with the Python client (v1 protocol, no version history)</summary>

```console
$ python3 client/python/examples/write_quote.py --addr 127.0.0.1:8080 \
    --code 600000 --date 2026-08-17 \
    --open 10.0 --high 10.5 --low 9.8 --close 10.2 --volume 1000000 --prev-close 10.0
写入成功: key=quote:2026-08-17:600000
读回内容: {"code": "600000", "date": "2026-08-17", "open": 10.0, "high": 10.5, "low": 9.8, "close": 10.2, "volume": 1000000.0, "prev_close": 10.0}
```

A negative price is rejected by contract validation before it ever touches
the buffer:

```console
$ python3 client/python/examples/write_quote.py --addr 127.0.0.1:8080 \
    --code 600001 --date 2026-08-17 --open -1 --high 10.5 --low 9.8 --close 10.2 --volume 1000000
写入被拒绝（清洗/schema 校验未通过）: dropped
```

</details>

## Samples — deploy-as-code

KairosFlux is a library first: the same API surface
(`kairosflux.NewEmbedded` / `kairosflux.Open` / `kairosflux.Serve` in
[kairosflux.go](kairosflux.go), the module-root top-level package) runs
in-process or as a network shell. Three capability samples, each an
independent `main`, each one command to run (all outputs below were
captured by running them):

**1. Embedded — in-process full flow.** No network listener; put three
versioned writes, read as-of a point strictly between versions (must see
v1 only), list versions, replay the ledger fingerprint (self-check against
`:current`, zero mismatches), then list audit writes.

```console
$ go run ./cmd/kairosflux-sample-embedded -data-dir /tmp/kf-demo-data
写入版本 seq=1: {"code":"510300",...}
写入版本 seq=2: {"code":"510300",...}
写入版本 seq=3: {"code":"510300",...}
GET_AS_OF(9:30:00.5) → seq=1 payload={"code":"510300",...}
LIST_VERSIONS → 3 个版本（seq 升序）
REPLAY_FINGERPRINT → 逻辑键=1 指纹=26ae2bc46b88a30a4095810af8bed7002fcbf7d907ed64a1b0bf268f3dbbe668 对账不一致=0
LIST_WRITES → 3 条写入，按来源: sample-demo x3
[sample-embedded] 全链路通过（指纹对账零不一致）
```

**2. Server + Python client — cross-language.** `kairosflux.Serve` opens a
real listening port; the Go leg does `PUT_VERSIONED → GET_AS_OF` over the
wire via a thin v2 client, then the Python leg (repo
[client/python/bandb_client.py](client/python/bandb_client.py)) does a v1
put/get round trip and a v2 ack=window batch write with FLUSH reconciliation
and STAT counter check.

```console
$ go run ./cmd/kairosflux-sample-server -port 19090 -data-dir /tmp/kf-demo-srv
[sample-server] 服务已就绪: 127.0.0.1:19090（数据目录 /tmp/kf-demo-srv）
[sample-server] Go 腿：PUT_VERSIONED 成功 seq=1
[sample-server] Go 腿：GET_AS_OF 成功 seq=1 payload={"code":"510300",...}
[sample-server] Python v1 腿: put/get 往返 OK, value = b'{"code":"600519",...}'
[sample-server] Python v2 腿: FLUSH/WINDOW_ACK received=3 accepted=3 rejected=0
[sample-server] Python v2 腿: STAT 累计 received=3 accepted=3
[sample-server] Python 腿全部通过
[sample-server] 两条腿全部通过
```

`-python=off` skips the Python leg (and a missing `python3` skips it
automatically with an honest note); exit code 0 means both legs passed.

**3. Audit export.** Multi-source writes, then a `LIST_WRITES` export to an
append-only JSONL file with per-envelope hash self-check and a manifest
line carrying a deterministic `export_fingerprint` (same shape as
`kairosflux-cli export-writes`).

```console
$ go run ./cmd/kairosflux-sample-audit -data-dir /tmp/kf-demo-audit -out /tmp/kf-audit.jsonl
[sample-audit] LIST_WRITES → 5 条写入，按来源: jobctl-reconcile x2 quantscout-crawler x3
[sample-audit] 导出完成: 5 条 → /tmp/kf-audit.jsonl（export_fingerprint=68d79402e6e431c578b992dfb1955faa1180c486c13473a3147874974f87910e，全部信封 hash 自检通过）
```

All three exit 0 on success, 1 on any failed step or detected inconsistency
(e.g. an as-of read returning the wrong version, a fingerprint mismatch, a
failed envelope hash check).

## Architecture

```
   writers            Ingest         Clean              Temporal Store
 (Go / Python  ─Kair─▶ (kairnet TLV) ─▶ (contracts/  ─▶  (WAL + LSM, versioned)
  clients)                              schema, M1)
                                                           │
                                          PUT_VERSIONED ───┤──▶ new immutable version
                                          GET_AS_OF(k,t) ──┤──▶ latest version, write_ts<=t, never future
                                          LIST_VERSIONS ───┤──▶ full version history
                                          REPLAY_FINGERPRINT┤──▶ deterministic sha256 vs :current   (M0)
                                          LIST_WRITES /    ─┤──▶ audit: who wrote what, when          (M2,
                                          export-writes     │                                    merged)
                                                           │
                                    ┌──────────────────────┴───────────────────────┐
                                    ▼                                              ▼
                        Deliver (file / ClickHouse sink)                 AI Agent data plane
                        — existing v1 ingest pipeline,                   Context (read) / Proposal (write)
                          independent of the temporal opcodes            — embedded API shipped, wire surface
                                                                           remains roadmap
```

Two subsystems — a Multi-Raft sharded KV and a dubbo-go-inspired delivery
governance layer — are fully implemented and tested but excluded from the
default build (`//go:build experimental`); build with `-tags experimental`
to include them. Wire-format details: [docs/Kair-协议规范.md](docs/Kair-协议规范.md).
Temporal key-space and semantics: [docs/架构与语义总览.md](docs/架构与语义总览.md).

## Features

| Status | Capability |
|---|---|
| **Implemented — M0** | Versioned writes never overwrite; `PUT_VERSIONED` / `GET_AS_OF` / `LIST_VERSIONS` / `REPLAY_FINGERPRINT` opcodes wired end-to-end through server and CLI (shipped 2026-08-24). |
| **Implemented — M1** | Machine-readable per-record-type contracts (`contracts/*.schema.json`: key layout, PIT semantics, idempotency key, validation rules), fail-fast contract loading, structured validation sub-codes (`0x3001`–`0x3004`), timestamp-monotonicity checks dispatched by a declared time-kind instead of a colon-position heuristic on the key string. |
| **Merged — M2** | `LIST_WRITES` audit query (opcode 0x0D) and a per-version operation envelope (`seq`, `write_ts`, `source`, `schema_ver`, `payload_hash`) with an envelope version tag and lazily-migrated reads for pre-M2 records; `COUNT`-by-source aggregation; append-only, deterministically-ordered JSONL export; `REPLAY_FINGERPRINT` upgraded to a dataset/as-of-scoped callable service. Full test suite and race detector green (merged 2026-08-25). |
| **Merged — M3** | A declarative Job control plane (`job:spec:{name}` / `job:status:{name}` / `job:events:{name}:v{seq}`) built on the existing versioned opcodes, a single-process reconcile loop (`internal/jobctl` + `cmd/kairosflux-jobctl`), and an explicit strategy lifecycle state machine (Hypothesis → Gate → Candidate → Paper → Live/Retired). Verified by a 10,000-rerun idempotency test against a live server. |
| **Implemented — M4 (embedded API)** | An AI-native data plane: a `Context` surface for agents to *read* point-in-time state and a `Proposal` surface to *write* through the same versioned/audited path — implemented as `internal/aiplane` and exposed via `kairosflux.Engine.SubmitProposal` / `BuildContext`. A network-visible agent plane over the wire remains roadmap. |

## Deployment Notes

For the current production shape — single machine, a single writer
(QuantScout), daily batch writes — leave `AdmissionEnabled` and
`ShardRoutingEnabled` off in `config/config.json` (both already default to
`false`, see `config/global.go`):

- `AdmissionEnabled` guards against concurrent-write overload with adaptive
  shedding. A daily batch job from one writer never produces the concurrent
  burst this exists to shed — there's no overload problem domain to defend
  against here, only the added latency-probing overhead.
- `ShardRoutingEnabled` forwards keys that don't belong to the local node to
  their owner across a multi-node placement. A single node owns 100% of the
  keyspace, so there is no routing decision to make.

Flip either on only when the deployment actually grows into the shape they're
for (concurrent multi-writer load, or a real multi-node placement) — leaving
them on by default in this shape adds overhead and a probing/forwarding
surface with nothing behind it to protect or route to.

## Data Cleaning

Every write passes through a cleaning hook before it's buffered: frame and
size checks, optional timestamp monotonicity (dispatched by the write's
declared time-kind as of M1, not a heuristic over the key string), and a
contract-driven schema registry keyed by record type
(`service/ingesthook/schema`, `contracts/*.schema.json`). A rejected record
returns `dropped` and never touches the buffer. The bundled market-data
contract enforces required fields, positive prices, OHLC consistency, and a
±21% sanity bound on daily price change (21%, not 20% — ChiNext/STAR-market
limit-up is ±20% off the *previous close*, but the limit-up *price* itself
can be up to ~20.02% above it after rounding, so a 20% threshold clips
legitimate limit-up prints; see `service/ingesthook/schema/quote.go`).
Adding a new record type is one contract file plus one `Validator`
implementation — see the package docs.

## Temporal Core — Semantics

See [docs/架构与语义总览.md](docs/架构与语义总览.md) for the full write-up:
the temporal key space (logical key / version key / `:current` pointer),
the `as_of(t)` contract and its point-in-time guarantee, the fingerprint
definition, and the bitemporal roadmap (M0 unifies valid-time and write-time;
separating them is M2+ scope). The RFC this was built from —
[`docs/rfc/时态内核-M0-版本化与as-of.md`](docs/rfc/时态内核-M0-版本化与as-of.md)
— has the wiring decisions and their rationale in full.

The protocol side of this (RFC stage, zero code shipped — see
[`docs/rfc/Kair-2.md`](docs/rfc/Kair-2.md), whose header literally says
"design document, no code changes yet") sketches what production usage
actually needs: **write-heavy, read-light**. The real load is QuantScout's
daily batch export of ~5000 rows, not interactive request/response, so v2
designs three ack tiers selectable per connection — `every` (today's
behavior, one response per write), `window` (batched acknowledgment every
N writes or on `FLUSH`), and `none` (fully fire-and-forget). Dropping
per-write ack on `none` also drops the guarantee that a lost connection
tells you what got lost — so the design makes **reconciliation mandatory**
for that tier: a client on `ack=none` must be able to replay/diff what it
sent against what the server actually has, or it must not use that tier.
None of this exists in code yet; v1's `ack=every` remains the only shipped
behavior.

## Roadmap

The full four-milestone plan (M0–M4) lives in QuantBrew's
[`方案-BanDB-时态内核与AI数据平面.md`](https://github.com/ChronoBrew/QuantBrew/blob/main/docs/方案-BanDB-时态内核与AI数据平面.md).
In short: M0/M1 are shipped (see [Features](#features)); M2 (replay-as-a-
service, the audit envelope, `LIST_WRITES`), M3 (declarative Job control
plane) and M4 (the `Context`/`Proposal` AI data plane, `internal/aiplane`,
exposed via `kairosflux.Engine.SubmitProposal`/`BuildContext`) are implemented;
what remains on the roadmap is the network-visible agent plane over the wire.

## Performance

发布口径基准（2026-08-25 实测，darwin/arm64 8 核，本机 fsync 地板约 250–530 次/s；完整
矩阵与已知测量缺陷见 [`docs/bench/01.md`](docs/bench/01.md)）：

- **载入**：100w 版本化写入 16 并发载入耗时 19m8.61s（871 w/s），载入后
  `REPLAY_FINGERPRINT` 对账零不一致（逻辑键=100000，对账不一致=0）
- **写路径**（数据量=1000000，每行 50000 采样，全 0 错误）：
  - server v2 `PUT_VERSIONED`（ack=every/window/none）：**QPS 460–474，p50 约 16ms**——
    standalone 模式每次写 2 次 WAL append + fsync，吞吐被磁盘 fsync 速率钉住
  - embedded 进程内直调：**QPS 452**（与网络路径同一 fsync 地板，进程内 vs 网络无吞吐差）
  - server v1 `PUT`：**QPS 931** ≈ 2×v2（1 次 append vs 2 次，物理自洽检查通过）
- **读路径**：`GET_AS_OF` 100w 档 p50 **2.66 ms（embedded）/ 3.01 ms（server）**，10w 档
  数百微秒级；前缀扫描与 `LIST_WRITES` 完整行以 10w 档为准（100w 档扫描路径因引擎读路径
  文件句柄耗尽未能测——已知缺陷，转 M5-C）
- **耐久**：kill -9 复验恢复数 ≥ 已 ack 数（ack-after-fsync 契约在代码审查与 kill 复验中
  均未破坏）

Reproduce with the stage B harness in
[`cmd/kairosflux-bench`](cmd/kairosflux-bench)（perf / footprint / adversarial / soak100
四个子命令）。

## Robustness

`go test -fuzz` against the 4 frame-parsing entry points ran for a combined
300 seconds (5 minutes) and logged **~37.7 million executions with zero
crashes** (`kairnet.FuzzUnPack` 369,985 / `proto.FuzzDecodeScanRequest`
15,108,651 / `proto.FuzzDecodeScanResponse` 12,065,019 /
`ingesthook.FuzzParsePut` 10,203,443). Full write-up, including the
malformed-frame test matrix (truncated frames, oversized length claims,
non-UTF8 msgIDs, slow-client half-writes) it grew out of:
[`docs/iteration-2026-08-20-bannet-robustness-audit.md`](docs/iteration-2026-08-20-bannet-robustness-audit.md).

## Who's using this

QuantScout (soon ChronoScout), a Python market-data crawler, writes
full-market daily snapshots into KairosFlux as its first production
tenant — see the Python client example above. A real 5241-row full-market
export: 5222 rows accepted, 19 rejected (all explainable: 17 halted/
delisted/warning-flag stocks with no trade that day, 2 legitimate ChiNext
limit-up prints tripping the ±21% sanity bound) — cross-checked by
reading every row back with the Go client and diffing field-by-field
against the Python-side source.

## Documentation

- [docs/架构与语义总览.md](docs/架构与语义总览.md) — temporal key space,
  version semantics, the `as_of` contract, fingerprint definition, and the
  bitemporal roadmap (contributor map, Chinese).
- [docs/Kair-协议规范.md](docs/Kair-协议规范.md) — Kair wire protocol spec.
- [docs/rfc/时态内核-M0-版本化与as-of.md](docs/rfc/时态内核-M0-版本化与as-of.md) — the temporal core RFC and its wiring log.
- [docs/kair/vectors-v2.json](docs/kair/vectors-v2.json) / [docs/kair/vectors.json](docs/kair/vectors.json) — cross-language test vectors (Go + Python), v2 and v1.
- `docs/iteration-*.md` — dated engineering notes on specific changes.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in this project by you, as defined in the
Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
