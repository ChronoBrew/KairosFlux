#!/usr/bin/env bash
#
# bench.sh — 清理旧数据后, 全新启动一个 BanDB 服务端并跑 benchmark, 结束后关停。
#
# 解决的问题: 直接对一个长期运行/复用数据目录的服务端压测, 会读到上一轮残留的
# SSTable/WAL/Raft 状态(脏数据), 影响结果。本脚本每次都把服务端的数据落到一个
# 受控且可被完全清空的目录, 保证每次压测都是干净状态。
#
# 用法:
#   bash scripts/bench.sh [ban-bench 参数...]
#   PORT=8081 bash scripts/bench.sh -d 10s -c 16 -n 10000 -mode mixed
#
# 透传给 ban-bench 的常用参数: -d 时长  -c 并发  -n key数  -mode put|get|delete|mixed
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT="${PORT:-8080}"
RUNDIR="$REPO/.bench/srv"          # 服务端工作目录(运行时 CWD), 全部数据收敛于此及 <repo>/log
SRVLOG="$REPO/.bench/server.log"

# 1. 清理上一轮数据(SSTable/WAL 落在 <repo>/log, Raft 落在 RUNDIR/raft_data)
echo "[bench] cleaning previous data ..."
rm -rf "$REPO/log" "$REPO/.bench"
mkdir -p "$RUNDIR/config"

# 2. 生成临时配置: 复制仓库配置并覆盖监听地址与单节点 Raft 拓扑, 使三者一致
#    (Host/Port 即监听地址, Peers[Me] 必须等于它, 否则单节点 Raft 会去连别的
#     地址——例如你正在运行的另一个服务端——导致选不出 Leader、写入全部失败)。
#    数据路径保持相对值, 随 CWD(RUNDIR) 落入受控、可清空的目录。
#    注意 [0-9][0-9]* 而非 [0-9]\+: BSD sed(macOS) 的 BRE 不认 \+, 会静默不替换,
#    导致服务端仍监听配置里的原端口, PORT= 覆盖失效。
sed -e "s/\"Port\": [0-9][0-9]*/\"Port\": $PORT/" \
    -e "s/\"Host\": \"[^\"]*\"/\"Host\": \"127.0.0.1\"/" \
    -e "1s/^{/{ \"Peers\": [\"127.0.0.1:$PORT\"], \"Me\": 0,/" \
    "$REPO/config/config.json" > "$RUNDIR/config/config.json"

# 3. 构建
echo "[bench] building server & benchmark ..."
go build -o "$REPO/bin/ban-server" ./cmd/ban-server
go build -o "$REPO/bin/ban-bench" ./cmd/ban-bench

# 4. 启动服务端(CWD=RUNDIR: 优先读 RUNDIR/config/config.json, 数据落入受控位置)
echo "[bench] starting fresh server on :$PORT ..."
( cd "$RUNDIR" && exec "$REPO/bin/ban-server" ) > "$SRVLOG" 2>&1 &
SRV_PID=$!
cleanup() { kill "$SRV_PID" 2>/dev/null || true; }
trap cleanup EXIT

# 5. 等服务端就绪(最多 30s): 端口可连 **且**(raft 模式下)Raft 已选出 Leader。
#    raft 模式只等端口会有竞态——端口先于 Leader 选举打开, 紧接着的 pre-populate 写入
#    会打到尚未成为 Leader 的节点, AppendEntry 失败导致整批写入报错。
#    但 standalone 模式不选主, "becoming leader" 永不出现: 无条件要求它会让就绪探测
#    必然超时, 整个压测一条也跑不起来。故按模式决定是否要求该日志。
if grep -q '"Mode"[[:space:]]*:[[:space:]]*"raft"' "$RUNDIR/config/config.json"; then
  NEED_LEADER=1
else
  NEED_LEADER=0
fi
for _ in $(seq 1 60); do
  if ! kill -0 "$SRV_PID" 2>/dev/null; then
    echo "[bench] server exited before becoming ready; log:" >&2
    cat "$SRVLOG" >&2
    exit 1
  fi
  if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null &&
     { [ "$NEED_LEADER" = 0 ] || grep -q "becoming leader" "$SRVLOG"; }; then
    exec 3>&- 3<&-
    ready=1
    break
  fi
  sleep 0.5
done
if [ "${ready:-0}" != "1" ]; then
  echo "[bench] timed out waiting for server to become ready (port + raft leader); log:" >&2
  cat "$SRVLOG" >&2
  exit 1
fi

# 6. 跑 benchmark(透传参数); 退出码即压测结果, 便于 CI 据此判定
echo "[bench] running benchmark ..."
set +e
"$REPO/bin/ban-bench" -addr "127.0.0.1:$PORT" "$@"
rc=$?
set -e
exit "$rc"
