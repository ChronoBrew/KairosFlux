#!/usr/bin/env python3
"""v2_window_probe —— Kair v2 ack=window/none 交互模式（RFC
docs/rfc/Kair-2.md §11）的跨语言联调驱动脚本。

由 KairosFlux 仓库的 Go 测试（client/python/crosslang_test.go 的
TestCrosslang_V2WindowBatchAndReconcile/TestCrosslang_V2NoneStatReconcile）
以子进程方式调用：用 Python 的 BanDBClientV2 连接一个真实的、由 Go 侧起的
kairnet.Server + service.RouterV2，批量写入、FLUSH/STAT/BYE，把结果打印成
"key=value"形式的行，供 Go 测试解析断言——验证的不是"两侧对同一份协议的
理解方式恰好一致地错"，而是"Go 服务端真的按这份协议跟一个独立的 Python
客户端实现说得通"。

用法：
    python3 v2_window_probe.py window     --addr host:port --count N [--corrid C]
    python3 v2_window_probe.py none       --addr host:port --count N [--bad-count B]
    python3 v2_window_probe.py pagination --addr host:port --prefix P --page-size N
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bandb_client import ACK_EVERY, ACK_NONE, ACK_WINDOW, BanDBClientV2, ReconciliationError  # noqa: E402


def _print_ack(prefix: str, fields) -> None:
    # first_err_reason 故意在打印前去掉空白折叠成一个无空格 token——Go 侧
    # 联调测试（crosslang_test.go 的 runV2WindowProbe）用简单的空白分词
    # 解析这一行的 "key=value" 序列，人读原因（如 "quote: non-positive
    # price: open=-1"）本身带空格会破坏这个解析；Go 测试也只断言数字
    # first_err_code，不断言 reason 文本内容，折叠空白不影响任何断言。
    reason_token = "".join(fields.first_err_reason.split()) or "-"
    print(
        f"{prefix}: corr_id={fields.corr_id} received={fields.received} "
        f"accepted={fields.accepted} rejected={fields.rejected} "
        f"first_err_code={fields.first_err_code} first_err_reason={reason_token}"
    )


def cmd_window(args: argparse.Namespace) -> int:
    c = BanDBClientV2(args.addr)
    try:
        ack = c.connect(ACK_WINDOW)
        print(f"negotiated_ack={ack}")

        for i in range(args.count):
            c.put_window(args.corrid, f"pywk{i}".encode(), b"v", 0)

        flush_ack = c.flush()
        _print_ack("flush", flush_ack)

        window_ack, stat_ack = c.bye()
        if window_ack is not None:
            _print_ack("bye_window", window_ack)
        _print_ack("bye_stat", stat_ack)
    finally:
        c.close()
    return 0


def cmd_none(args: argparse.Namespace) -> int:
    c = BanDBClientV2(args.addr)
    try:
        ack = c.connect(ACK_NONE)
        print(f"negotiated_ack={ack}")

        good = args.count - args.bad_count
        for i in range(good):
            c.put_none(f"pynk{i}".encode(), b"v")
        for i in range(args.bad_count):
            # 人为注入 schema 拒绝：quote 类型的非法价格。
            c.put_none(
                f"quote:2026-08-20:py{i}".encode(),
                b'{"code":"600000","date":"2026-08-20","open":-1,"high":1,"low":1,"close":1,"volume":1}',
                type_=1,
            )

        try:
            fields = c.reconcile()
            print("reconcile_status=matched")
            _print_ack("reconcile", fields)
        except ReconciliationError as e:
            print(f"reconcile_status=mismatch local_sent={e.local_sent} server_received={e.server_received}")
            return 0

        _, stat_ack = c.bye()
        _print_ack("bye_stat", stat_ack)
    finally:
        c.close()
    return 0


def cmd_pagination(args: argparse.Namespace) -> int:
    """LIST_WRITES 分页跨语言实测：客户端游标循环 + list_writes_all 遍历
    助手把结果分页取全（服务端把 MaxPackageSize 压小后，单帧装不下全部
    命中——分页是取全的唯一路径，见 Go 侧
    TestCrosslang_V2ListWritesPaginationOverFrameLimit）。"""
    c = BanDBClientV2(args.addr)
    try:
        ack = c.connect(ACK_EVERY)
        print(f"negotiated_ack={ack}")

        # 手写游标循环（与 list_writes_all 内部同构），顺带数页数——联调
        # 测试要断言"真的分了页、页与页之间用 next_cursor 衔接"，单页返回
        # 的遍历助手把分页细节藏起来了。
        pages = 0
        cursor = b""
        entries = []
        while True:
            page, _, next_cursor = c.list_writes(args.prefix, 0, 0, "", cursor, args.page_size)
            entries.extend(page)
            pages += 1
            if not next_cursor:
                break
            cursor = next_cursor
        print(f"pages={pages} total={len(entries)}")

        # 全量遍历助手应得到同一份结果（seq 序列一致才算一致）。
        helper_entries = c.list_writes_all(args.prefix, page_size=args.page_size)
        helper_seqs = [e.seq for e in helper_entries]
        loop_seqs = [e.seq for e in entries]
        print(f"helper_total={len(helper_entries)}")
        print(f"helper_match={1 if helper_seqs == loop_seqs else 0}")

        # 总序核对（游标=(logical_key, seq)，跨页不重不漏、按逻辑键字典序
        # 升序；seq 是每个逻辑键内部版本号，各键独立从 1 起）。不打印键本身
        # ——键含冒号，会破坏 Go 侧 "key=value" 行解析器（见 crosslang_test.go
        # runV2WindowProbe 的分词约定），改为打印核对结论。
        keys = [e.logical_key for e in entries]
        ordered = 1 if keys == sorted(keys) and len(set(keys)) == len(keys) else 0
        print(f"ordered={ordered} distinct={len(set(keys))}")
        print("seqs=" + ",".join(str(e.seq) for e in entries))
    finally:
        c.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_window = sub.add_parser("window")
    p_window.add_argument("--addr", required=True)
    p_window.add_argument("--count", type=int, required=True)
    p_window.add_argument("--corrid", type=int, default=1)
    p_window.set_defaults(func=cmd_window)

    p_none = sub.add_parser("none")
    p_none.add_argument("--addr", required=True)
    p_none.add_argument("--count", type=int, required=True)
    p_none.add_argument("--bad-count", type=int, default=0)
    p_none.set_defaults(func=cmd_none)

    p_pagination = sub.add_parser("pagination")
    p_pagination.add_argument("--addr", required=True)
    p_pagination.add_argument("--prefix", required=True)
    p_pagination.add_argument("--page-size", type=int, required=True)
    p_pagination.set_defaults(func=cmd_pagination)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
