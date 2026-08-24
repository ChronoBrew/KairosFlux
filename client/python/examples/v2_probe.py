#!/usr/bin/env python3
"""v2_probe —— Kair v2 帧编解码的跨语言联调驱动脚本。

由 KairosFlux 仓库的 Go 测试（client/python/crosslang_test.go 的
TestCrosslang_V2FrameCrossLanguage）以子进程方式调用，两个方向各验证一次：

  encode: 按参数构造一个 v2 帧，打印其十六进制到标准输出最后一行——
          Go 侧用 kairnet/codec.DataPackV2.UnPack 解析这段十六进制，断言
          解出的字段与参数一致（验证"Python 编码的 v2 帧 Go 能解"）。
  decode: 读入一个十六进制 v2 帧（Go 侧用 codec.DataPackV2.Pack 生成），
          解析后把字段打印成 "opcode=.. type=.. corr_id=.. data_hex=.."
          一行，供 Go 侧断言与自己构造时使用的参数一致（验证"Go 编码的
          v2 帧 Python 能解"）。

不是给人手工跑的工具（虽然也能手工跑），是联调测试的固定驱动腿，与
crosslang_probe.py（v1 场景）是同一模式在 v2 帧编解码这个更底层问题上的
对应实现。
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bandb_client import decode_header_v2, encode_frame_v2, HEADER_V2_LEN  # noqa: E402


def cmd_encode(args: argparse.Namespace) -> int:
    payload = bytes.fromhex(args.payload_hex)
    frame = encode_frame_v2(args.flags, args.opcode, args.type, args.corr_id, payload)
    print(f"frame_hex={frame.hex()}")
    return 0


def cmd_decode(args: argparse.Namespace) -> int:
    frame = bytes.fromhex(args.frame_hex)
    header = decode_header_v2(frame[:HEADER_V2_LEN])
    payload = frame[HEADER_V2_LEN:]
    print(
        f"flags={header.flags} opcode={header.opcode} type={header.type} "
        f"corr_id={header.corr_id} data_hex={payload.hex()}"
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_encode = sub.add_parser("encode")
    p_encode.add_argument("--flags", type=int, default=0)
    p_encode.add_argument("--opcode", type=int, required=True)
    p_encode.add_argument("--type", type=int, required=True)
    p_encode.add_argument("--corrid", dest="corr_id", type=int, required=True)
    p_encode.add_argument("--payload-hex", default="")
    p_encode.set_defaults(func=cmd_encode)

    p_decode = sub.add_parser("decode")
    p_decode.add_argument("--frame-hex", required=True)
    p_decode.set_defaults(func=cmd_decode)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
