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

from bandb_client import (
    MSG_RESP_OK,
    decode_get_value,
    encode_frame,
    encode_key_only_payload,
    encode_put_payload,
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
        import struct

        data = struct.pack("<II", 100, 0) + b"ab"
        self.assertEqual(data.hex(), v["data_hex"])
        self.assertEqual(encode_frame(v["msg_id"], data).hex(), v["frame_hex"])


if __name__ == "__main__":
    unittest.main()
