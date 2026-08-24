# KairosFlux

[English](README.md) | [中文](README.zh-CN.md)

![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)
![Go Version](https://img.shields.io/badge/go-1.26%2B-00ADD8?logo=go)
![Protocol](https://img.shields.io/badge/protocol-Kair-orange)
![Performance](https://img.shields.io/badge/Kair%20vs%20gRPC-2.7x%20faster%20(GET)-brightgreen)
<!-- CI badge: add once a GitHub Actions workflow is wired up -->

KairosFlux (formerly BanDB) is the temporal data-flow engine of **ChronoBrew**,
an org built around one idea: an AI-native time-series engine where every
write is versioned, every query can be asked "as of when", and every state
can be replayed from the ledger and re-checked against its own fingerprint.
Time-series is the substance; determinism is the differentiator; AI is the
direction. QuantBrew (a deterministic A-share backtesting kernel) and
ChronoScout (a full-market recon crawler) are its first two tenants — see
[ChronoBrew's architecture overview](https://github.com/ChronoBrew/QuantBrew/blob/main/docs/架构总览-ChronoBrew.md)
for how the three pieces fit together.

Today, in production, it does something narrower and already useful:
high-frequency writes headed for a data warehouse are bursty upstream and
easy to overwhelm downstream. KairosFlux sits in between: it absorbs the
burst, validates and cleans each record, buffers it durably, and delivers it
downstream at a pace the sink can handle.

Single binary. No third-party dependencies beyond gRPC/Protobuf, which is
only used for an optional benchmarking endpoint (see below).

> This repository has completed its product/protocol rename: `BanDB` →
> `KairosFlux`, protocol brand `BANLV` → `Kair`. Module path, package name
> (`bannet` → `kairnet`), `cmd/*` binaries, and docs have all moved. Frame
> format, opcodes, and cross-language test vector bytes are unchanged
> throughout, so nothing deployed today breaks. One file is deliberately
> still named `client/python/bandb_client.py` — it's slated to be retired
> outright in an upcoming cleanup rather than renamed in place, so its old
> identifiers (`BanDBClient`, `BanDBError`, ...) are left alone for now.
> Every command below is real and runnable as written.

## Features

- *Native protocol* — Kair, a zero-dependency binary TCP TLV protocol built for ingest.
- *Clean before buffering* — pluggable per-record-type schema validation; bad rows never reach the buffer.
- *Durable buffer* — WAL-backed LSM engine, crash-safe, survives restarts.
- *Reliable delivery* — at-least-once, with circuit breakers and health-aware routing across sinks.
- *Multi-language* — Go SDK and a dependency-free Python client, no protobuf toolchain required.
- gRPC endpoint included for benchmarking only — see [Performance](#performance).

## Quick Start

Requires Go 1.26+.

Start a server:

```bash
cd cmd/kairosflux-server && go run .
```

Write and read with the Go client (another terminal):

```console
$ go run ./cmd/kairosflux-cli -addr 127.0.0.1:8080 put order:1001 '{"amount":128,"ts":1754380800}'
已写入: order:1001 = {"amount":128,"ts":1754380800}

$ go run ./cmd/kairosflux-cli -addr 127.0.0.1:8080 get order:1001
{"amount":128,"ts":1754380800}
```

Write a market-data snapshot with the Python client (`client/python/`, standard library only):

```console
$ python3 client/python/examples/write_quote.py --addr 127.0.0.1:8080 \
    --code 600000 --date 2026-08-17 \
    --open 10.0 --high 10.5 --low 9.8 --close 10.2 --volume 1000000 --prev-close 10.0
写入成功: key=quote:2026-08-17:600000
读回内容: {"code": "600000", "date": "2026-08-17", "open": 10.0, "high": 10.5, "low": 9.8, "close": 10.2, "volume": 1000000.0, "prev_close": 10.0}
```

A negative price is rejected by schema validation before it ever touches the buffer:

```console
$ python3 client/python/examples/write_quote.py --addr 127.0.0.1:8080 \
    --code 600001 --date 2026-08-17 --open -1 --high 10.5 --low 9.8 --close 10.2 --volume 1000000
写入被拒绝（清洗/schema 校验未通过）: dropped
```

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
size checks, optional timestamp monotonicity, and a schema registry keyed
by record type (`service/ingesthook/schema`). A rejected record returns
`dropped` and never touches the buffer. The bundled market-data validator
enforces required fields, positive prices, OHLC consistency, and a ±20%
sanity bound on daily price change. Adding a new record type is one
`Validator` implementation plus a registration call — see the package docs.

## Architecture

`Ingest (Kair) → Clean (schema) → Buffer (LSM) → Deliver (Sink)`

The current production sink writes to a local file; a ClickHouse sink with
health-aware failover is available behind config (`DeliverySinkType`). Two
subsystems — a Multi-Raft sharded KV and a dubbo-go-inspired delivery
governance layer — are fully implemented and tested but excluded from the
default build (`//go:build experimental`); build with `-tags experimental`
to include them. Details: [docs/Kair-协议规范.md](docs/Kair-协议规范.md).

## Temporal Core (built, not yet wired into the write path)

`internal/temporal` implements the semantics an "AI-native" time-series
engine needs: writes never overwrite (each write produces a new immutable
version), `as_of(t)` returns the latest version whose write time is ≤ t and
never a future write, and `Fingerprint(entries)` gives a deterministic
sha256 over `(LogicalKey, Seq, Payload)` so any replayed state can be
checked against its own history. This is unit-tested pure logic today —
**it is not yet connected to the router or storage engine**, so production
writes still go through the plain overwrite path described above. Wiring
it in (plus the versioned opcodes below) is the next roadmap milestone, not
a shipped capability; see
[`方案-BanDB-时态内核与AI数据平面.md`](https://github.com/ChronoBrew/QuantBrew/blob/main/docs/方案-BanDB-时态内核与AI数据平面.md)
in the QuantBrew repo for the full four-milestone plan.

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

## Performance

In our benchmarks, the Kair wire protocol is measurably faster than the
bundled gRPC endpoint on the read path (2.7x on GET at 50 concurrent
clients; writes are roughly on par since both share the same fsync-bound
persistence path). Reproduce with `bash scripts/bench.sh` and the commands
in [docs/Kair-协议规范.md](docs/Kair-协议规范.md).

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
limit-up prints tripping the generic ±20% sanity bound) — cross-checked by
reading every row back with the Go client and diffing field-by-field
against the Python-side source.

## Documentation

- [docs/Kair-协议规范.md](docs/Kair-协议规范.md) — Kair wire protocol spec.
- [docs/kair/vectors.json](docs/kair/vectors.json) — cross-language test vectors (Go + Python).
- `docs/iteration-*.md` — dated engineering notes on specific changes.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in this project by you, as defined in the
Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
