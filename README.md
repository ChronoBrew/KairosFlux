# KairosFlux

[English](README.md) | [中文](README.zh-CN.md)

![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)
![Go Version](https://img.shields.io/badge/go-1.26%2B-00ADD8?logo=go)
![Protocol](https://img.shields.io/badge/protocol-BANLV-orange)
![Performance](https://img.shields.io/badge/BANLV%20vs%20gRPC-2.7x%20faster%20(GET)-brightgreen)
<!-- CI badge: add once a GitHub Actions workflow is wired up -->

High-frequency writes headed for a data warehouse are bursty upstream and
easy to overwhelm downstream. BanDB sits in between: it absorbs the burst,
validates and cleans each record, buffers it durably, and delivers it
downstream at a pace the sink can handle.

Single binary. No third-party dependencies beyond gRPC/Protobuf, which is
only used for an optional benchmarking endpoint (see below).

## Features

- *Native protocol* — BANLV, a zero-dependency binary TCP TLV protocol built for ingest.
- *Clean before buffering* — pluggable per-record-type schema validation; bad rows never reach the buffer.
- *Durable buffer* — WAL-backed LSM engine, crash-safe, survives restarts.
- *Reliable delivery* — at-least-once, with circuit breakers and health-aware routing across sinks.
- *Multi-language* — Go SDK and a dependency-free Python client, no protobuf toolchain required.
- gRPC endpoint included for benchmarking only — see [Performance](#performance).

## Quick Start

Requires Go 1.26+.

Start a server:

```bash
cd cmd/ban-server && go run .
```

Write and read with the Go client (another terminal):

```console
$ go run ./cmd/ban-cli -addr 127.0.0.1:8080 put order:1001 '{"amount":128,"ts":1754380800}'
已写入: order:1001 = {"amount":128,"ts":1754380800}

$ go run ./cmd/ban-cli -addr 127.0.0.1:8080 get order:1001
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

`Ingest (BANLV) → Clean (schema) → Buffer (LSM) → Deliver (Sink)`

The current production sink writes to a local file; a ClickHouse sink with
health-aware failover is available behind config (`DeliverySinkType`). Two
subsystems — a Multi-Raft sharded KV and a dubbo-go-inspired delivery
governance layer — are fully implemented and tested but excluded from the
default build (`//go:build experimental`); build with `-tags experimental`
to include them. Details: [docs/BANLV-协议规范.md](docs/BANLV-协议规范.md).

## Performance

In our benchmarks, the BANLV wire protocol is measurably faster than the
bundled gRPC endpoint on the read path (2.7x on GET at 50 concurrent
clients; writes are roughly on par since both share the same fsync-bound
persistence path). Reproduce with `bash scripts/bench.sh` and the commands
in [docs/BANLV-协议规范.md](docs/BANLV-协议规范.md).

## Who's using this

QuantScout, a Python market-data crawler, writes full-market daily
snapshots into BanDB as its first production tenant — see the Python
client example above.

## Documentation

- [docs/BANLV-协议规范.md](docs/BANLV-协议规范.md) — BANLV wire protocol spec.
- [docs/banlv/vectors.json](docs/banlv/vectors.json) — cross-language test vectors (Go + Python).
- `docs/iteration-*.md` — dated engineering notes on specific changes.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in this project by you, as defined in the
Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
