"""bandb_client —— BanDB 的 BANLV(自研 TLV) 协议最小 Python 客户端。

定位：生产摄入的权威入口是 bannet（BANLV 协议），不是 gRPC ——见
internal/kvgrpc 包注释与 docs/BANLV-协议规范.md 的说明。本客户端是
BANLV 协议在 Python 侧的最小可用实现，供 QuantScout 这类 Python 上游
直接写入行情快照，不依赖 protobuf 工具链。

权威实现：BanDB 仓库的 bannet/（帧编解码）与 client/（Go SDK，请求/响应负载
格式）。本文件逐字段对照它们实现，每个函数的文档字符串标注对应的 Go 源码
位置；协议本身的正式规范见 docs/BANLV-协议规范.md（含跨语言测试向量）。

刻意的最小范围（不做的事，与 Go SDK 的差异）：
  - 无连接池：BANLV 是严格请求-响应协议，一条 socket 一次只能有一个在途请求；
    需要并发时由调用方开多个 BanDBClient 实例。
  - 无自动重试：Go SDK 对 overloaded 等可重试错误做指数退避重试（见
    client/client.go 的 do()），本实现只把状态映射为异常，重试策略留给调用方。
  - 无 SCAN：谓词下推的编解码更复杂，当前量化写入场景只需要 PUT（GET 用于
    读回验证），SCAN 留待后续需要时再对照 proto/scan.go 实现。

纯标准库：只用 socket + struct，不引入任何第三方依赖，与 BanDB「零依赖」
的定位一致。
"""

from __future__ import annotations

import socket
import struct
from typing import Iterable, Tuple

# ---------------------------------------------------------------------------
# 协议常量。对应 proto/codes.go。
# ---------------------------------------------------------------------------

MSG_PUT = "PUT"
MSG_GET = "GET"
MSG_DELETE = "DEL"
MSG_SCAN = "SCAN"
MSG_RESP_OK = "OK"
MSG_RESP_ERR = "ERR"

STATUS_OK = "ok"
STATUS_ERROR = "error"
STATUS_DROPPED = "dropped"
STATUS_OVERLOADED = "overloaded"
STATUS_NOTFOUND = "notfound"

# 帧头固定 6 字节：dataLen(u32 LE) + idLen(u16 LE)。见 bannet/datapack.go
# 的 DataPack.HeadLen/Pack/UnPack，以及 client/conn.go 的 frameHeadLen。
_FRAME_HEAD_LEN = 6
_HEAD_STRUCT = struct.Struct("<IH")  # dataLen u32 LE, idLen u16 LE


# ---------------------------------------------------------------------------
# 异常。对应 client/errors.go 的哨兵错误——Go 用 errors.Is 判别，
# Python 侧用异常类型判别，语义一一对应。
# ---------------------------------------------------------------------------


class BanDBError(Exception):
    """所有 BanDB 客户端异常的基类。"""


class KeyNotFoundError(BanDBError):
    """key 不存在，或其最新版本是删除墓碑——正常查询结果，非故障。对应 ErrKeyNotFound。"""


class OverloadedError(BanDBError):
    """服务端网关自适应准入 shed（过载拒绝）；可重试。对应 ErrOverloaded。"""


class DroppedError(BanDBError):
    """请求被落盘前钩子（ingesthook.Filter）按策略丢弃：畸形负载/超限/schema
    校验不通过/时间戳非单调。确定性拒绝，重试无意义。对应 ErrDropped。

    reason 是服务端回传的具体丢弃原因（如 "quote: non-positive price: open=-1"），
    老服务端未实现该字段时为空字符串——此前这里只有 "dropped" 三个字没有任何
    上下文，调用方只能靠在本地重新实现一遍校验规则去猜为什么，这正是本字段
    要解决的问题（QuantScout 全量实测反馈，见
    docs/iteration-2026-08-20-quantscout-realdata-fixes.md 的 D2 记录）。
    """

    def __init__(self, status: str, reason: str = ""):
        # 只传 status 给基类：e.args[0]/str(e) 保持恒为 "dropped"，不因新增 reason
        # 而变化——crosslang_probe.py 等按 status 精确比对的调用方不应受影响。
        # reason 通过独立属性暴露，需要细节的调用方显式读 e.reason。
        super().__init__(status)
        self.reason = reason


class ServerError(BanDBError):
    """服务端返回内部错误，可重试。对应 ErrServer。"""


class ProtocolError(BanDBError):
    """收到无法解析的响应，通常意味着协议不一致或连接串话。对应 ErrProtocol。"""


def _status_to_exception(status: str, rest: bytes = b"") -> "BanDBError | None":
    """把响应状态映射为异常；status=ok 返回 None。对应 client/conn.go 的 statusError()。
    rest 是状态字段之后的剩余字节，仅 dropped 状态用它取丢弃原因。
    """
    if status == STATUS_OK:
        return None
    if status == STATUS_NOTFOUND:
        return KeyNotFoundError(status)
    if status == STATUS_OVERLOADED:
        return OverloadedError(status)
    if status == STATUS_DROPPED:
        return DroppedError(status, parse_drop_reason(rest))
    if status == STATUS_ERROR:
        return ServerError(status)
    return ServerError(f"unknown status {status!r}")


def parse_drop_reason(rest: bytes) -> str:
    """从 dropped 响应的剩余字节里解出丢弃原因：[reasonLen u16 LE][reason bytes]
    （见 service/router.go 的 droppedPayload、docs/BANLV-协议规范.md 的响应负载
    一节）。老服务端未实现该字段、或字节格式不符时返回空字符串，不报错——是否
    携带 reason 是可选的协议扩展。对应 Go: client/conn.go 的 parseDropReason()。
    """
    if len(rest) < 2:
        return ""
    (n,) = struct.unpack_from("<H", rest, 0)
    if len(rest) < 2 + n:
        return ""
    return rest[2 : 2 + n].decode("utf-8", errors="replace")


# ---------------------------------------------------------------------------
# 帧编解码。对应 bannet/datapack.go（Pack/UnPack）与 client/conn.go（encodeFrame/
# roundTrip 的头部解析）。
# ---------------------------------------------------------------------------


def encode_frame(msg_id: str, data: bytes) -> bytes:
    """按 BANLV 线格式编码一帧：[dataLen u32 LE][idLen u16 LE][id][data]。

    对应 Go: client/conn.go 的 encodeFrame()，语义与 bannet/datapack.go 的
    DataPack.Pack() 一致（服务端用后者编码响应）。
    """
    id_bytes = msg_id.encode("ascii")
    head = _HEAD_STRUCT.pack(len(data), len(id_bytes))
    return head + id_bytes + data


def decode_frame_header(head: bytes) -> Tuple[int, int]:
    """解析 6 字节定长帧头，返回 (dataLen, idLen)。

    对应 Go: bannet/datapack.go 的 DataPack.UnPack()（服务端侧）与
    client/conn.go 的 roundTrip() 里对响应头的解析（客户端侧）。
    """
    if len(head) != _FRAME_HEAD_LEN:
        raise ProtocolError(f"frame header must be {_FRAME_HEAD_LEN} bytes, got {len(head)}")
    data_len, id_len = _HEAD_STRUCT.unpack(head)
    return data_len, id_len


# ---------------------------------------------------------------------------
# 请求负载编码。对应 client/client.go 的 Put()/Get()/Delete()。
# ---------------------------------------------------------------------------


def encode_put_payload(key: bytes, value: bytes) -> bytes:
    """PUT 请求负载：[keyLen u32 LE][valueLen u32 LE][key][value]。对应 client/client.go Put()。"""
    return struct.pack("<II", len(key), len(value)) + key + value


def encode_key_only_payload(key: bytes) -> bytes:
    """GET/DEL 请求负载：[keyLen u32 LE][key]。对应 client/client.go Get()/Delete()（同形状）。"""
    return struct.pack("<I", len(key)) + key


# ---------------------------------------------------------------------------
# 响应负载解码。对应 client/conn.go 的 parseStatus() 与 client/client.go Get()
# 里 status 之后 value 段的解析。
# ---------------------------------------------------------------------------


def parse_status(payload: bytes) -> Tuple[str, bytes]:
    """解析响应负载头部 [statusLen u8][status bytes]，返回 (status, rest)。

    对应 Go: client/conn.go 的 parseStatus()。
    """
    if len(payload) < 1:
        raise ProtocolError("response payload is empty")
    n = payload[0]
    if len(payload) < 1 + n:
        raise ProtocolError(f"status field length {n} exceeds payload {len(payload)} bytes")
    return payload[1 : 1 + n].decode("ascii"), payload[1 + n :]


def decode_get_value(rest: bytes) -> bytes:
    """从 GET 成功响应剥离 status 后的剩余字节里解出 value：[valueLen u32 LE][value]。

    对应 Go: client/client.go 的 Get()。
    """
    if len(rest) < 4:
        raise ProtocolError("GET response missing value length")
    (n,) = struct.unpack_from("<I", rest, 0)
    if len(rest) < 4 + n:
        raise ProtocolError(f"GET response value length {n} exceeds payload")
    return rest[4 : 4 + n]


# ---------------------------------------------------------------------------
# 客户端。单连接，严格请求-响应——不可并发复用同一实例发请求（与 Go SDK的
# conn 类型同一约束，见 client/conn.go 的注释），并发请求请各开一个实例。
# ---------------------------------------------------------------------------


class BanDBClient:
    def __init__(self, addr: str, timeout: float = 5.0):
        """addr 形如 "host:port"。timeout 同时作为连接与每次请求的超时。"""
        self._addr = addr
        self._timeout = timeout
        self._sock: "socket.socket | None" = None

    def connect(self) -> None:
        if self._sock is not None:
            return
        host, port_str = self._addr.rsplit(":", 1)
        self._sock = socket.create_connection((host, int(port_str)), timeout=self._timeout)

    def close(self) -> None:
        if self._sock is not None:
            self._sock.close()
            self._sock = None

    def __enter__(self) -> "BanDBClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _recv_exact(self, n: int) -> bytes:
        assert self._sock is not None
        chunks = []
        remaining = n
        while remaining > 0:
            chunk = self._sock.recv(remaining)
            if not chunk:
                raise ProtocolError("connection closed while reading response")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def _roundtrip(self, msg_id: str, data: bytes) -> Tuple[str, bytes]:
        """发一帧、收一帧，返回 (resp_msg_id, resp_data)。不对状态做任何解释——
        由调用方决定如何处理。对应 Go: client/conn.go 的 conn.roundTrip()。
        """
        self.connect()
        assert self._sock is not None
        self._sock.sendall(encode_frame(msg_id, data))

        head = self._recv_exact(_FRAME_HEAD_LEN)
        data_len, id_len = decode_frame_header(head)
        rest = self._recv_exact(id_len + data_len)
        resp_msg_id = rest[:id_len].decode("ascii")
        resp_data = rest[id_len:]
        return resp_msg_id, resp_data

    def raw_put(self, payload: bytes) -> Tuple[str, str]:
        """发送一个已编码好的 PUT 负载（可以是刻意构造的畸形负载），不做客户端侧
        校验、不抛状态异常，只返回 (resp_msg_id, status)——供协议一致性测试使用
        （如验证畸形负载在两端客户端下都被服务端判定为 dropped）。
        """
        resp_msg_id, resp_data = self._roundtrip(MSG_PUT, payload)
        status, _ = parse_status(resp_data)
        return resp_msg_id, status

    def put(self, key: bytes, value: bytes) -> None:
        """写入一条键值。返回时数据已在服务端 fsync 落盘（standalone）或已提交
        (raft)。对应 Go: client/client.go 的 Put()。
        """
        _, resp_data = self._roundtrip(MSG_PUT, encode_put_payload(key, value))
        status, rest = parse_status(resp_data)
        err = _status_to_exception(status, rest)
        if err is not None:
            raise err

    def put_many(self, items: Iterable[Tuple[bytes, bytes]]) -> None:
        """批量写入：逐条循环调用 put()。BANLV 是严格请求-响应协议，单连接无法
        流水线，批量在协议层面就是循环——这与「批量」的直觉不同，但如实反映
        协议约束（Go SDK 用连接池做并发批量，本最小实现不做池化，见模块文档）。
        """
        for key, value in items:
            self.put(key, value)

    def get(self, key: bytes) -> bytes:
        """读取键值；key 不存在时抛 KeyNotFoundError（非故障，是正常查询结果）。
        对应 Go: client/client.go 的 Get()。
        """
        _, resp_data = self._roundtrip(MSG_GET, encode_key_only_payload(key))
        status, rest = parse_status(resp_data)
        err = _status_to_exception(status, rest)
        if err is not None:
            raise err
        return decode_get_value(rest)

    def delete(self, key: bytes) -> None:
        """删除键。幂等盲写：删除不存在的 key 同样成功。对应 Go: client/client.go 的 Delete()。"""
        _, resp_data = self._roundtrip(MSG_DELETE, encode_key_only_payload(key))
        status, rest = parse_status(resp_data)
        err = _status_to_exception(status, rest)
        if err is not None:
            raise err
