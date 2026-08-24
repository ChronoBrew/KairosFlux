#!/usr/bin/env python3
"""crosslang_probe —— 跨语言联调测试的 Python 侧驱动脚本。

由 KairosFlux 仓库的 Go 测试（client/python/crosslang_test.go）以子进程方式调用：
对一个真实运行中的 kairosflux-server 执行指定场景的一次写入，把结果状态打印到标准
输出的最后一行，供 Go 测试解析比对。不是给人手工跑的工具（虽然也能手工跑），
是联调测试的固定驱动腿。

场景（--scenario）：
  valid_quote        合法行情快照，预期 status=ok
  invalid_price      open<=0，预期被 schema 校验拒绝，status=dropped
  oversized          value 超过服务端 maxValueLen，预期 status=dropped
  malformed_lengths  PUT 负载内 keyLen 与实际字节数不符（畸形负载，非畸形帧头），
                     预期被 ingesthook.Filter 拒绝，status=dropped

用法：
  python3 crosslang_probe.py --addr 127.0.0.1:PORT --scenario valid_quote --key quote:2026-08-17:600099
"""

import argparse
import struct
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bandb_client import BanDBClient, BanDBError  # noqa: E402

_VALID_QUOTE = (
    b'{"code":"600000","date":"2026-08-17","open":10.0,"high":10.5,'
    b'"low":9.8,"close":10.2,"volume":1000000,"prev_close":10.0}'
)

_INVALID_PRICE_QUOTE = (
    b'{"code":"600000","date":"2026-08-17","open":-1,"high":10.5,'
    b'"low":9.8,"close":10.2,"volume":1000000}'
)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--addr", required=True, help="host:port")
    p.add_argument(
        "--scenario",
        required=True,
        choices=["valid_quote", "invalid_price", "oversized", "malformed_lengths"],
    )
    p.add_argument("--key", required=True, help="写入用的 key（各场景各用独立 key 避免冲突）")
    args = p.parse_args()

    key = args.key.encode("utf-8")

    with BanDBClient(args.addr) as c:
        if args.scenario == "malformed_lengths":
            # keyLen 谎称 100，实际只有 2 字节数据——与 docs/kair/vectors.json 的
            # put_request_malformed_lengths 向量同构。
            payload = struct.pack("<II", 100, 0) + b"ab"
            _, status = c.raw_put(payload)
            print(f"status={status}")
            return 0

        if args.scenario == "valid_quote":
            value = _VALID_QUOTE
        elif args.scenario == "invalid_price":
            value = _INVALID_PRICE_QUOTE
        elif args.scenario == "oversized":
            value = b"x" * 4096
        else:
            print(f"error=unknown scenario {args.scenario}", file=sys.stderr)
            return 2

        try:
            c.put(key, value)
            print("status=ok")
        except BanDBError as e:
            status = e.args[0] if e.args else type(e).__name__
            print(f"status={status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
