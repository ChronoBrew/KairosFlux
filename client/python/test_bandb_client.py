"""BANLV 协议向量测试 —— 加载 docs/banlv/vectors.json（由 bannet 权威 Go 实现生成，
见 docs/BANLV-协议规范.md 附录），验证本 Python 客户端的编解码与 Go 侧逐字节一致。

这是防止 Go/Python 两个实现悄悄分叉的锚点：Go 侧对应测试见
bannet/vectors_test.go，两者读同一份 vectors.json。任何一侧改了编解码逻辑，
只要向量文件不变，测试就会在改错的那一侧失败。

运行（须在 client/python/ 目录下，保证 bandb_client 可被直接 import）：
    cd client/python && python3 -m unittest test_bandb_client -v
（纯标准库 unittest，无需安装 pytest 等第三方测试框架。）
"""

import json
import os
import unittest

import struct

from bandb_client import (
    MSG_RESP_OK,
    DroppedError,
    decode_get_value,
    encode_frame,
    encode_key_only_payload,
    encode_put_payload,
    parse_drop_reason,
    parse_status,
)

_VECTORS_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "docs", "banlv", "vectors.json"
)


def _load_vectors():
    with open(_VECTORS_PATH, "rb") as f:
        return json.load(f)


class VectorTests(unittest.TestCase):
    """按名字取向量、逐条验证 —— 而不是无差别遍历，这样每个用例的失败信息
    能清楚指出具体是哪种语义（PUT 请求 / GET 响应 / dropped 等）不匹配。
    """

    @classmethod
    def setUpClass(cls):
        cls.vectors = {v["name"]: v for v in _load_vectors()}

    def _vec(self, name):
        self.assertIn(name, self.vectors, f"vector {name!r} not found in {_VECTORS_PATH}")
        return self.vectors[name]

    def test_put_request_basic(self):
        v = self._vec("put_request_basic")
        data = encode_put_payload(b"k1", b"v1")
        self.assertEqual(data.hex(), v["data_hex"])
        self.assertEqual(encode_frame(v["msg_id"], data).hex(), v["frame_hex"])

    def test_get_request_basic(self):
        v = self._vec("get_request_basic")
        data = encode_key_only_payload(b"k1")
        self.assertEqual(data.hex(), v["data_hex"])
        self.assertEqual(encode_frame(v["msg_id"], data).hex(), v["frame_hex"])

    def test_del_request_basic(self):
        v = self._vec("del_request_basic")
        data = encode_key_only_payload(b"k1")
        self.assertEqual(data.hex(), v["data_hex"])
        self.assertEqual(encode_frame(v["msg_id"], data).hex(), v["frame_hex"])

    def test_resp_ok_put(self):
        v = self._vec("resp_ok_put")
        data = bytes.fromhex(v["data_hex"])
        status, rest = parse_status(data)
        self.assertEqual(status, "ok")
        self.assertEqual(rest, b"")
        self.assertEqual(v["msg_id"], MSG_RESP_OK)

    def test_resp_ok_get_with_value(self):
        v = self._vec("resp_ok_get_with_value")
        data = bytes.fromhex(v["data_hex"])
        status, rest = parse_status(data)
        self.assertEqual(status, "ok")
        self.assertEqual(decode_get_value(rest), b"v1")

    def test_resp_err_notfound(self):
        v = self._vec("resp_err_notfound")
        status, rest = parse_status(bytes.fromhex(v["data_hex"]))
        self.assertEqual(status, "notfound")
        self.assertEqual(rest, b"")

    def test_resp_err_dropped(self):
        v = self._vec("resp_err_dropped")
        status, _ = parse_status(bytes.fromhex(v["data_hex"]))
        self.assertEqual(status, "dropped")

    def test_resp_err_dropped_with_reason(self):
        """BANLV v1.1 扩展向量：dropped 之后追加 reasonLen+reason（见
        docs/BANLV-协议规范.md 3.4 节）。验证 parse_status 与 parse_drop_reason
        联用能从这份 Go 权威生成的向量里还原出完整 reason 字符串。
        """
        v = self._vec("resp_err_dropped_with_reason")
        status, rest = parse_status(bytes.fromhex(v["data_hex"]))
        self.assertEqual(status, "dropped")
        self.assertEqual(parse_drop_reason(rest), "quote: non-positive price: open=-1")

    def test_resp_err_overloaded(self):
        v = self._vec("resp_err_overloaded")
        status, _ = parse_status(bytes.fromhex(v["data_hex"]))
        self.assertEqual(status, "overloaded")

    def test_resp_err_server(self):
        v = self._vec("resp_err_server")
        status, _ = parse_status(bytes.fromhex(v["data_hex"]))
        self.assertEqual(status, "error")

    def test_put_request_quote(self):
        v = self._vec("put_request_quote")
        key = b"quote:2026-08-17:600000"
        value = (
            b'{"code":"600000","date":"2026-08-17","open":10.0,"high":10.5,'
            b'"low":9.8,"close":10.2,"volume":1000000,"prev_close":10.0}'
        )
        data = encode_put_payload(key, value)
        self.assertEqual(data.hex(), v["data_hex"])
        self.assertEqual(encode_frame(v["msg_id"], data).hex(), v["frame_hex"])

    def test_put_request_malformed_lengths(self):
        """畸形 PUT 负载向量：keyLen 声明 100、实际只有 2 字节数据。本测试只验证
        字节布局与向量一致——它是否被服务端拒绝，由跨语言联调测试
        (crosslang 目录) 对真实服务端验证，这里不起网络。
        """
        v = self._vec("put_request_malformed_lengths")
        data = struct.pack("<II", 100, 0) + b"ab"
        self.assertEqual(data.hex(), v["data_hex"])
        self.assertEqual(encode_frame(v["msg_id"], data).hex(), v["frame_hex"])

    def test_parse_drop_reason_roundtrip(self):
        """dropped 响应在 status 之后追加 [reasonLen u16 LE][reason]（见
        service/router.go 的 droppedPayload、docs/BANLV-协议规范.md）。这里不依赖
        vectors.json（该向量文件目前只覆盖不带 reason 的旧 dropped 响应），直接
        验证 parse_drop_reason 对新格式的解析。
        """
        reason = "quote: non-positive price: open=-1"
        rest = struct.pack("<H", len(reason)) + reason.encode("utf-8")
        self.assertEqual(parse_drop_reason(rest), reason)

    def test_parse_drop_reason_missing_field_returns_empty(self):
        """老服务端未实现 reason 字段时，rest 为空——应返回空字符串而非报错，
        这是可选协议扩展的向后兼容要求。
        """
        self.assertEqual(parse_drop_reason(b""), "")

    def test_dropped_error_exposes_reason_without_changing_message(self):
        """DroppedError.reason 携带具体原因，但 str(e)/e.args[0] 仍恒为 status——
        crosslang_probe.py 等按 status 精确比对的调用方不受影响。
        """
        err = DroppedError("dropped", "quote: missing required field \"code\"")
        self.assertEqual(str(err), "dropped")
        self.assertEqual(err.args[0], "dropped")
        self.assertEqual(err.reason, 'quote: missing required field "code"')

        err_no_reason = DroppedError("dropped")
        self.assertEqual(err_no_reason.reason, "")


if __name__ == "__main__":
    unittest.main()
