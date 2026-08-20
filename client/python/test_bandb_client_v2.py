"""BANLV v2 协议向量测试 —— 加载 docs/banlv/vectors-v2.json（手工推导生成，
不是本仓库任何一侧的 v2 编解码实现自己生成的存档，见该文件顶部 _comment 的
方法论说明），验证本 Python 客户端的 v2 编解码与手工黄金字节逐字节一致。

与 test_bandb_client.py（v1 向量）并存不合并，对应 Go 侧
bannet/vectors_v2_test.go（同样与 vectors_test.go 并存），见
docs/rfc/BANLV-2.md §8 迁移方案第 4 条。

运行（须在 client/python/ 目录下，保证 bandb_client 可被直接 import）：
    cd client/python && python3 -m unittest test_bandb_client_v2 -v
"""

import json
import os
import socket
import unittest

from bandb_client import (
    ACK_EVERY,
    HEADER_V2_LEN,
    MAGIC_V2,
    MSG_HELLO,
    OPCODE_OK,
    SNIFF_V1,
    SNIFF_V2,
    SNIFF_UNSUPPORTED_VERSION,
    VERSION_V2,
    ProtocolError,
    UnsupportedV2VersionError,
    build_hello_response_v2,
    decode_frame_header,
    decode_header_v2,
    encode_frame,
    encode_frame_v2,
    encode_hello_probe_v2,
    negotiate_client,
    sniff_version,
)

_VECTORS_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "docs", "banlv", "vectors-v2.json"
)


def _load_vectors():
    with open(_VECTORS_PATH, "rb") as f:
        return json.load(f)


class FrameVectorTests(unittest.TestCase):
    """frames 分区：覆盖各 opcode/type 分派、corr_id 边界、magic 字节序。"""

    @classmethod
    def setUpClass(cls):
        doc = _load_vectors()
        cls.frames = {v["name"]: v for v in doc["frames"]}
        cls.header_only = {v["name"]: v for v in doc["header_only"]}
        cls.negotiation = {v["name"]: v for v in doc["negotiation"]}

    def _frame(self, name):
        self.assertIn(name, self.frames, f"vector {name!r} not found in frames")
        return self.frames[name]

    def _assert_frame_matches(self, name):
        v = self._frame(name)
        data = bytes.fromhex(v["data_hex"])
        got = encode_frame_v2(v["flags"], v["opcode"], v["type"], v["corr_id"], data)
        self.assertEqual(got.hex(), v["frame_hex"], f"{name}: 帧字节不一致")

        # 交叉核验解码方向：UnPack 应还原出同样的字段。
        frame = bytes.fromhex(v["frame_hex"])
        header = decode_header_v2(frame[:HEADER_V2_LEN])
        self.assertEqual(header.flags, v["flags"])
        self.assertEqual(header.opcode, v["opcode"])
        self.assertEqual(header.type, v["type"])
        self.assertEqual(header.corr_id, v["corr_id"])
        self.assertEqual(header.data_len, len(data))
        self.assertEqual(frame[HEADER_V2_LEN:].hex(), v["data_hex"])

    def test_put_request_quote(self):
        self._assert_frame_matches("v2_put_request_quote")

    def test_get_request_unspecified_type(self):
        self._assert_frame_matches("v2_get_request_unspecified_type")

    def test_del_request(self):
        self._assert_frame_matches("v2_del_request")

    def test_scan_request_quote(self):
        self._assert_frame_matches("v2_scan_request_quote")

    def test_resp_ok(self):
        self._assert_frame_matches("v2_resp_ok")

    def test_resp_err(self):
        self._assert_frame_matches("v2_resp_err")

    def test_opcode_hello_native_v2_framing(self):
        self._assert_frame_matches("v2_opcode_hello_native_v2_framing")

    def test_empty_payload(self):
        self._assert_frame_matches("v2_empty_payload")

    def test_corr_id_zero(self):
        self._assert_frame_matches("v2_corr_id_zero")

    def test_corr_id_max(self):
        v = self._frame("v2_corr_id_max")
        self.assertEqual(v["corr_id"], 0xFFFFFFFF)
        self._assert_frame_matches("v2_corr_id_max")

    def test_magic_byte_order_lock(self):
        """锁定 wire 上第 0 字节是 version、第 1 字节才是 magic——不通过
        Pack/UnPack 往返自洽，直接断言绝对字节位置，防止"两侧实现一致地
        搞反字节序"这种系统性错误被自洽的向量掩盖。
        """
        v = self._frame("v2_magic_byte_order_lock")
        frame = bytes.fromhex(v["frame_hex"])
        self.assertEqual(frame[0], 0x02, "frame[0] 应为 version(0x02)")
        self.assertEqual(frame[1], MAGIC_V2, "frame[1] 应为 magic(0xBA)")

        data = bytes.fromhex(v["data_hex"])
        got = encode_frame_v2(v["flags"], v["opcode"], v["type"], v["corr_id"], data)
        self.assertEqual(got[0], 0x02)
        self.assertEqual(got[1], MAGIC_V2)

    def test_header_len_is_fourteen_bytes(self):
        self.assertEqual(HEADER_V2_LEN, 14)

    def test_max_declared_datalen_header_only(self):
        v = self.header_only["v2_max_declared_datalen_header"]
        head = bytes.fromhex(v["header_hex"])
        self.assertEqual(len(head), HEADER_V2_LEN)
        header = decode_header_v2(head)
        self.assertEqual(header.opcode, v["opcode"])
        self.assertEqual(header.type, v["type"])
        self.assertEqual(header.corr_id, v["corr_id"])
        self.assertEqual(header.data_len, v["data_len"])

    def test_sniff_version_dispatch(self):
        v2_frame = bytes.fromhex(self._frame("v2_put_request_quote")["frame_hex"])
        self.assertEqual(sniff_version(v2_frame[:2]), SNIFF_V2)

        # 借用 vectors.json 的 v1 向量首 2 字节（put_request_basic）。
        v1_frame = bytes.fromhex("0c000000030050555402000000020000006b317631")
        self.assertEqual(sniff_version(v1_frame[:2]), SNIFF_V1)

        unsupported = bytes([0x03, MAGIC_V2])
        self.assertEqual(sniff_version(unsupported), SNIFF_UNSUPPORTED_VERSION)

    def test_decode_header_v2_rejects_non_v2_magic(self):
        v1_frame = bytes.fromhex("0c000000030050555402000000020000006b317631")
        with self.assertRaises(ProtocolError):
            decode_header_v2(v1_frame[:HEADER_V2_LEN])

    def test_decode_header_v2_rejects_unsupported_version(self):
        bad_head = bytes([0x03, MAGIC_V2]) + bytes(HEADER_V2_LEN - 2)
        with self.assertRaises(UnsupportedV2VersionError):
            decode_header_v2(bad_head)


class NegotiationVectorTests(unittest.TestCase):
    """negotiation 分区：§5 协商探测/响应帧——注意探测帧是 v1 格式、响应帧
    是 v2 格式，两者不是同一种"HELLO"（详见向量 description）。
    """

    @classmethod
    def setUpClass(cls):
        doc = _load_vectors()
        cls.negotiation = {v["name"]: v for v in doc["negotiation"]}

    def test_client_probe_is_v1_format(self):
        v = self.negotiation["v2_negotiation_client_probe_v1_format"]
        payload = bytes.fromhex(v["payload_hex"])
        self.assertEqual(payload, bytes([VERSION_V2, ACK_EVERY]))

        got = encode_frame(v["msg_id"], payload)
        self.assertEqual(got.hex(), v["frame_hex"])
        self.assertEqual(v["msg_id"], MSG_HELLO)

        # 探测帧首 2 字节不应携带 v2 magic（它必须让真正的 v1 服务端读起来
        # 像一条普通消息）。
        self.assertEqual(sniff_version(got[:2]), SNIFF_V1)

        # 与 encode_hello_probe_v2() 的产出完全一致。
        self.assertEqual(encode_hello_probe_v2().hex(), v["frame_hex"])

    def test_server_response_is_v2_format(self):
        v = self.negotiation["v2_negotiation_server_response_v2_format"]
        data = bytes.fromhex(v["data_hex"])
        got = encode_frame_v2(v["flags"], v["opcode"], v["type"], v["corr_id"], data)
        self.assertEqual(got.hex(), v["frame_hex"])
        self.assertEqual(v["opcode"], OPCODE_OK)
        self.assertEqual(v["corr_id"], 0)

        self.assertEqual(sniff_version(got[:2]), SNIFF_V2)

        # 与 build_hello_response_v2() 的产出完全一致。
        self.assertEqual(build_hello_response_v2().hex(), v["frame_hex"])

    def test_v1_server_silence_is_documented_not_a_byte_vector(self):
        v = self.negotiation["v2_negotiation_v1_server_silence"]
        self.assertTrue(v.get("expect_no_response"))
        self.assertNotIn("frame_hex", v)


class NegotiateClientTests(unittest.TestCase):
    """negotiate_client() 的行为测试——对应 Go 侧
    bannet/negotiate/negotiate_test.go 的同名场景。用 socket.socketpair()
    构造一对已连接的本地 socket，不用起真实 TCP 服务器/线程：两端各自有
    内核缓冲区，"服务端"一侧只需 send()/不 send() 即可模拟各种响应模式，
    "客户端"一侧的 negotiate_client() 在真实的 socket 超时语义下运行。

    覆盖三态判断（不能塌缩成"读失败就当 v1"）：完全静默→v1、完整 v2 响应
    →v2、半读（partial）→报错且不得判成 v1、magic 匹配但版本不兼容→报错
    且不得判成 v1。
    """

    def setUp(self):
        self.client_sock, self.server_sock = socket.socketpair()
        self.addCleanup(self.client_sock.close)
        self.addCleanup(self.server_sock.close)

    def test_downgrades_to_v1_on_full_silence(self):
        # 服务端一侧什么都不做（真正的 v1 服务端不会响应 HELLO 探测帧）。
        version, ack = negotiate_client(self.client_sock, timeout=0.2)
        self.assertEqual(version, SNIFF_V1)
        self.assertEqual(ack, ACK_EVERY)

        # 探测帧确实已经发出，服务端一侧应该能收到完整的 v1 格式探测帧
        # （证明"降级"不是因为探测帧根本没发出去）。
        probe = self.server_sock.recv(1024)
        self.assertEqual(probe, encode_hello_probe_v2())

    def test_succeeds_with_v2_on_valid_response(self):
        # 两端 socket 的收发缓冲区互相独立，谁先谁后不影响最终能不能读到——
        # 提前把响应字节塞进 server_sock 的发送方向（即 client_sock 的接收
        # 缓冲区），negotiate_client() 内部先发探测帧、再读响应时，响应早已
        # 在缓冲区里等着，不需要另开线程模拟"服务端同时在运行"。若在这里
        # 先 recv() 探测帧再 send()，会在单线程测试里死锁（探测帧要等
        # negotiate_client() 内部才发出）。
        self.server_sock.sendall(build_hello_response_v2())

        version, ack = negotiate_client(self.client_sock, timeout=2.0)
        self.assertEqual(version, SNIFF_V2)
        self.assertEqual(ack, ACK_EVERY)

        # 之后再验证探测帧确实被完整发出（此时早已在 server_sock 的接收
        # 缓冲区里，recv 不会阻塞）。
        probe = self.server_sock.recv(1024)
        _, msg_id_len = decode_frame_header(probe[:6])
        self.assertEqual(probe[6 : 6 + msg_id_len].decode("ascii"), MSG_HELLO)

    def test_partial_response_then_silence_raises_protocol_error(self):
        # 只发 magic+ver 字段的第 1 个字节，然后不再发送任何字节——半读，
        # 连接已经不可信，必须报错，不能被误判为"降级成功"。提前发送，
        # 理由同上（避免单线程死锁）。
        self.server_sock.sendall(bytes([0x02]))

        with self.assertRaises(ProtocolError):
            negotiate_client(self.client_sock, timeout=0.2)

    def test_magic_matches_unsupported_version_raises_protocol_error(self):
        # magic+ver 数值=magic<<8|version=0xBA03，LE 存储：[0]=0x03(version)，
        # [1]=0xBA(magic)——版本不受支持，必须报错，不能塌缩成"当作 v1"。
        self.server_sock.sendall(bytes([0x03, MAGIC_V2]))

        with self.assertRaises(ProtocolError):
            negotiate_client(self.client_sock, timeout=0.5)


if __name__ == "__main__":
    unittest.main()
