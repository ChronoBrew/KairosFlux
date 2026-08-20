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


# ---------------------------------------------------------------------------
# BANLV v2（docs/rfc/BANLV-2.md）。以下是纯新增区块，不改动上面 v1 的任何一行
# ——v1 是且仍是生产协议，v2 是本阶段追加的能力，两者在这个文件里完全独立。
#
# 权威实现对照：bannet/codec/v2.go（帧编解码）、bannet/negotiate/negotiate.go
# （§5/§5.1 HELLO 协商）。跨语言测试向量见 docs/banlv/vectors-v2.json，Python
# 侧测试见 test_bandb_client_v2.py（对应 Go 侧 bannet/vectors_v2_test.go）。
# ---------------------------------------------------------------------------

# --- 帧格式常量 (RFC §2/§3) ---------------------------------------------------

MAGIC_V2 = 0xBA
VERSION_V2 = 0x02
HEADER_V2_LEN = 14  # magic+ver(2) + flags(1) + opcode(1) + type(2) + corr_id(4) + dataLen(4)

# struct 格式字符串必须带 "<" 前缀（显式小端、无原生对齐填充）——不带前缀时
# "HBBHII" 会按原生对齐规则在字段间插入填充字节，令这个 14 字节头部在本机
# 变成 16 字节，本地测试看起来"通过"（因为编解码用的是同一个 pack/unpack 调
# 用），但产出的字节与 Go 侧、与协议本身规定的 14 字节定长头部不一致——这正是
# 本文件其它地方（_HEAD_STRUCT）已经在用的同一条纪律，这里只是对 v2 头部重申。
_HEADER_V2_STRUCT = struct.Struct("<HBBHII")

# opcode（RFC §3.1）。0x00-0x7F 请求，0x80-0xFF 响应。
OPCODE_PUT = 0x01
OPCODE_GET = 0x02
OPCODE_DEL = 0x03
OPCODE_SCAN = 0x04
OPCODE_HELLO = 0x05  # v2 起第一次被赋予行为（协商，见下）
OPCODE_BYE = 0x06  # 语义待定（RFC §3.1/§7），本阶段只保留常量

# OPCODE_FLUSH/OPCODE_STAT: RFC §11 交互模式（ack=window/none）新增 opcode，
# 第二阶段实现——本阶段只保留常量定义，不实现其行为。
OPCODE_FLUSH = 0x07
OPCODE_STAT = 0x08

OPCODE_OK = 0x80
OPCODE_ERR = 0x81

# OPCODE_WINDOW_ACK/OPCODE_STAT_ACK: 同上，RFC §11 交互模式，第二阶段实现。
OPCODE_WINDOW_ACK = 0x82
OPCODE_STAT_ACK = 0x83

# type（RFC §3.2），与 service/ingesthook/schema 的注册表对齐。
TYPE_UNSPECIFIED = 0  # 未声明类型，向后兼容默认值，退化为 v1 行为（RFC §6）
TYPE_QUOTE = 1  # 对应 "quote:" 前缀校验器


class UnsupportedV2VersionError(BanDBError):
    """magic 匹配但版本号不是本实现认识的 VERSION_V2——这是协议不兼容错误，
    不应被当作"这是 v1 帧"静默处理（对应 Go: codec.ErrUnsupportedV2Version）。
    """


def encode_magic_ver(magic: int, version: int) -> int:
    """组装 magic+ver 字段的 u16 数值：数值 = magic<<8 | version（高字节
    magic、低字节 version，RFC §2 原文措辞）。对应 Go: codec.EncodeMagicVer。
    """
    return ((magic & 0xFF) << 8) | (version & 0xFF)


def decode_magic_ver(v: int) -> Tuple[int, int]:
    """从 u16 数值里拆出 (magic, version)。对应 Go: codec.DecodeMagicVer。"""
    return (v >> 8) & 0xFF, v & 0xFF


# sniff_version 的三种返回值（对应 Go: codec.SniffResult 的三个常量）。
SNIFF_V1 = "v1"
SNIFF_V2 = "v2"
SNIFF_UNSUPPORTED_VERSION = "unsupported_version"


def sniff_version(first2_bytes: bytes) -> str:
    """RFC §6 双栈判据：给定帧最前 2 字节，判断应该按 v1 头部回退解释、还是
    按 v2 头部继续解析、还是 magic 匹配但版本不受支持（这第三种不应该被
    误判为"这是 v1 帧"，见 Go 侧 codec.SniffVersion 的同款文档）。

    对应 Go: codec.SniffVersion。
    """
    if len(first2_bytes) != 2:
        raise ProtocolError(f"sniff_version 需要恰好 2 字节，收到 {len(first2_bytes)}")
    (magic_ver,) = struct.unpack("<H", first2_bytes)
    magic, version = decode_magic_ver(magic_ver)
    if magic != MAGIC_V2:
        return SNIFF_V1
    if version != VERSION_V2:
        return SNIFF_UNSUPPORTED_VERSION
    return SNIFF_V2


def encode_frame_v2(flags: int, opcode: int, type_: int, corr_id: int, payload: bytes) -> bytes:
    """按 v2 线格式编码一帧：14 字节定长头 + 负载。

    对应 Go: codec.DataPackV2.Pack。
    """
    head = _HEADER_V2_STRUCT.pack(
        encode_magic_ver(MAGIC_V2, VERSION_V2), flags, opcode, type_, corr_id, len(payload)
    )
    return head + payload


class HeaderV2:
    """v2 定长头部的内存表示（不含负载），对应 Go: codec.HeaderV2。"""

    __slots__ = ("flags", "opcode", "type", "corr_id", "data_len")

    def __init__(self, flags: int, opcode: int, type_: int, corr_id: int, data_len: int):
        self.flags = flags
        self.opcode = opcode
        self.type = type_
        self.corr_id = corr_id
        self.data_len = data_len


def decode_header_v2(head: bytes) -> HeaderV2:
    """解析 14 字节定长头部，校验 magic/version。对应 Go: codec.DataPackV2.UnPack。"""
    if len(head) != HEADER_V2_LEN:
        raise ProtocolError(f"v2 头部必须是 {HEADER_V2_LEN} 字节，收到 {len(head)}")
    magic_ver, flags, opcode, type_, corr_id, data_len = _HEADER_V2_STRUCT.unpack(head)
    magic, version = decode_magic_ver(magic_ver)
    if magic != MAGIC_V2:
        raise ProtocolError(f"不携带 v2 magic: got {magic:#x}")
    if version != VERSION_V2:
        raise UnsupportedV2VersionError(f"不受支持的 v2 版本号: got {version:#x}")
    return HeaderV2(flags, opcode, type_, corr_id, data_len)


# --- HELLO 协商 (RFC §5/§5.1) ------------------------------------------------

# ack 三档 (RFC §11.2)；本阶段客户端恒发送 ACK_EVERY、服务端恒回复
# ACK_EVERY——ACK_WINDOW/ACK_NONE 的编号已保留，行为留给后续阶段实现。
ACK_EVERY = 0
ACK_WINDOW = 1  # §11 交互模式，第二阶段实现——本阶段不发送/不生效
ACK_NONE = 2  # 同上


def encode_hello_probe_v2() -> bytes:
    """按 §5.1 编码 v2 客户端的探测帧：v1 帧格式（msgID="HELLO"），负载
    [version u8][ackPref u8]。对应 Go: negotiate.encodeHelloProbe（未导出，
    这里是 Python 侧的对应实现）。
    """
    payload = bytes([VERSION_V2, ACK_EVERY])
    return encode_frame(MSG_HELLO, payload)


def build_hello_response_v2() -> bytes:
    """按 §5.1 构造 v2 服务端对探测帧的响应：v2 帧格式，opcode=OK、type=0、
    corr_id=0，负载 [version u8][ackPref u8]。对应 Go:
    negotiate.BuildHelloResponseV2。
    """
    payload = bytes([VERSION_V2, ACK_EVERY])
    return encode_frame_v2(0, OPCODE_OK, TYPE_UNSPECIFIED, 0, payload)


# MSG_HELLO 是 v1 帧格式里 HELLO 探测帧使用的 msgID 字符串——注意这与
# OPCODE_HELLO(0x05) 是两回事：后者是 v2 原生帧格式下的 opcode 分派项，
# 协商阶段客户端还不知道对端是否支持 v2，只能、也应该用 v1 格式探测
# （见 encode_hello_probe_v2 与 docs/rfc/BANLV-2.md §5.1 的说明）。
MSG_HELLO = "HELLO"


def negotiate_client(sock: "socket.socket", timeout: float) -> Tuple[str, int]:
    """v2 客户端一侧的协商入口：写入 §5.1 探测帧，在 timeout 内等待响应，
    返回 (version, ack)，version 是 "v1"/"v2"。对应 Go: negotiate.ClientNegotiate。

    三种读取结果分别处理（不能塌缩成"读失败就当 v1"，见 Go 侧同名函数的
    文档——这里是同一套判断逻辑的 Python 实现，必须保持一致）：

      1. 一个字节都没读到就超时：判定为 v1，不是错误。
      2. 超时或提前断开，但已经读到了部分字节：协议错误，连接已不可信。
      3. 收到完整 2 字节 magic+ver：按 sniff_version 分派，非 v2 视为协议
         错误（真正的 v1 服务端根本不会发送任何字节）。
    """
    sock.sendall(encode_hello_probe_v2())

    original_timeout = sock.gettimeout()
    sock.settimeout(timeout)
    try:
        magic_ver = _recv_exact_or_none(sock, 2)
        if magic_ver is None:
            # 情形 1：一个字节都没读到就超时——正常降级路径。
            return SNIFF_V1, ACK_EVERY

        sniff = sniff_version(magic_ver)
        if sniff == SNIFF_V2:
            rest = _recv_exact_or_none(sock, HEADER_V2_LEN - 2)
            if rest is None:
                raise ProtocolError("negotiate: 读响应剩余头部时超时（半读，连接不可信）")
            header = decode_header_v2(magic_ver + rest)
            if header.opcode != OPCODE_OK:
                raise ProtocolError(f"negotiate: 响应 opcode={header.opcode:#x}，期望 OK(0x80)")
            payload = b""
            if header.data_len > 0:
                payload = _recv_exact_or_none(sock, header.data_len)
                if payload is None:
                    raise ProtocolError("negotiate: 读响应负载时超时（半读，连接不可信）")
            if len(payload) < 2:
                raise ProtocolError(f"negotiate: 响应负载长度={len(payload)}，期望至少 2 字节")
            server_version = payload[0]
            if server_version != VERSION_V2:
                raise ProtocolError(f"negotiate: 服务端选定版本={server_version:#x}，本实现只认识 {VERSION_V2:#x}")
            # payload[1] 是 ackPref；本阶段服务端恒回 ACK_EVERY，忽略取值。
            return SNIFF_V2, ACK_EVERY
        elif sniff == SNIFF_UNSUPPORTED_VERSION:
            raise ProtocolError("negotiate: 对端 magic 匹配但版本号不受支持")
        else:
            # 情形 2 的另一半：收到了字节但不是 v2 magic——真正的 v1 服务端
            # 不该发送任何字节，收到非预期字节是协议错误，不是"降级为 v1"。
            raise ProtocolError("negotiate: 收到非预期的响应字节（既非超时也非 v2 magic）")
    finally:
        sock.settimeout(original_timeout)


def _recv_exact_or_none(sock: "socket.socket", n: int) -> "bytes | None":
    """在 sock 当前设置的超时内尝试读满 n 字节。

    - 一个字节都没读到就超时：返回 None（供调用方判定为"降级"这一正常
      路径，不是错误）。
    - 读到部分字节后超时/对端提前关闭：抛 ProtocolError（半读，连接已经
      不可信，不能静默当作"没读到"处理——这是 negotiate_client 三态判断
      里最容易被错误合并的一种情形，单独判断以避免误判）。
    - 正常读满：返回完整的 n 字节。
    """
    chunks = []
    remaining = n
    try:
        while remaining > 0:
            chunk = sock.recv(remaining)
            if not chunk:
                if remaining == n:
                    return None
                raise ProtocolError("negotiate: 连接在读取过程中被对端关闭（半读）")
            chunks.append(chunk)
            remaining -= len(chunk)
    except socket.timeout:
        if remaining == n:
            return None
        raise ProtocolError("negotiate: 读取过程中超时（半读，连接不可信）")
    return b"".join(chunks)
