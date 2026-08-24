#!/usr/bin/env python3
"""write_quote —— 用最小 Python 客户端把一条全市场股票日线快照写入 KairosFlux。

演示 QuantScout 这类 Python 上游如何直接写 Kair 协议（kairnet TLV），不经 gRPC、
不装 protobuf 工具链。key 布局用裁决过的 quote:<日期>:<代码>（日期在前），
理由见 docs/Kair-协议规范.md 与 service/ingesthook/schema/quote.go 的注释：
同一天的全市场快照在 key 空间连续，投递/retention 按「日」成批时不需要改动
现有机制。

用法：
    python3 write_quote.py --addr 127.0.0.1:8080 \\
        --code 600000 --date 2026-08-17 \\
        --open 10.0 --high 10.5 --low 9.8 --close 10.2 --volume 1000000 \\
        [--prev-close 10.0]

不传 --prev-close 时，服务端 schema 校验会跳过涨跌幅物理极限检查（±20%）并计数
（metrics.SchemaChecksSkipped），不视为失败——首个交易日/复牌首日没有可比昨收。
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bandb_client import BanDBClient, DroppedError, KeyNotFoundError  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--addr", required=True, help="kairosflux-server 地址，如 127.0.0.1:8080")
    p.add_argument("--code", required=True, help="标的代码，如 600000")
    p.add_argument("--date", required=True, help="交易日，YYYY-MM-DD")
    p.add_argument("--open", type=float, required=True)
    p.add_argument("--high", type=float, required=True)
    p.add_argument("--low", type=float, required=True)
    p.add_argument("--close", type=float, required=True)
    p.add_argument("--volume", type=float, required=True)
    p.add_argument("--prev-close", type=float, default=None, help="昨收，缺省则跳过涨跌幅校验")
    args = p.parse_args()

    record = {
        "code": args.code,
        "date": args.date,
        "open": args.open,
        "high": args.high,
        "low": args.low,
        "close": args.close,
        "volume": args.volume,
    }
    if args.prev_close is not None:
        record["prev_close"] = args.prev_close

    # key 布局：quote:<日期>:<代码>，日期在前。
    key = f"quote:{args.date}:{args.code}".encode("utf-8")
    value = json.dumps(record, ensure_ascii=False).encode("utf-8")

    with BanDBClient(args.addr) as c:
        try:
            c.put(key, value)
        except DroppedError as e:
            # e.reason 是服务端回传的具体拒绝原因（如非正价格/OHLC 不一致/涨跌幅
            # 超限）；老服务端未实现该字段时 e.reason 为空，仍能看到 "dropped"。
            detail = f"：{e.reason}" if e.reason else ""
            print(f"写入被拒绝（清洗/schema 校验未通过）{detail}", file=sys.stderr)
            return 1

        # 读回校验：证明写入的字节经服务端持久化后可原样取回。
        try:
            got = c.get(key)
        except KeyNotFoundError:
            print("写入报告成功但读回未命中，异常情况", file=sys.stderr)
            return 1

        print(f"写入成功: key={key.decode()}")
        print(f"读回内容: {got.decode()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
