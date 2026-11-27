#!/usr/bin/env bash
# kafka-console-rs 停止脚本（Linux）
#
# 用法:
#   ./stop.sh              # 先 SIGTERM，10 秒没退再 SIGKILL
#   ./stop.sh 30           # 自定义等待秒数
#
# 只按 kafka-console.pid 精确杀本目录的实例，不会误杀其它进程。

set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$BASE_DIR/kafka-console.pid"
BIN="$BASE_DIR/kafka-console-rs"
WAIT_SECS="${1:-10}"

log_info() { printf '%s\n' "$*"; }

if [ ! -f "$PID_FILE" ]; then
  # pid 文件丢了，但进程可能还在：按可执行文件绝对路径兜底找一次
  if command -v pgrep >/dev/null 2>&1; then
    stray="$(pgrep -f "^${BIN}$" 2>/dev/null | head -n 1 || true)"
    if [ -n "$stray" ]; then
      log_info "pid 文件不存在，但发现了同目录实例 (PID $stray)，按它处理"
      printf '%s\n' "$stray" > "$PID_FILE"
    else
      log_info "没有运行中的实例（也不存在 $PID_FILE）"
      exit 0
    fi
  else
    log_info "没有运行中的实例（也不存在 $PID_FILE）"
    exit 0
  fi
fi

pid="$(cat "$PID_FILE" 2>/dev/null || true)"

if [ -z "$pid" ]; then
  log_info "pid 文件为空，直接清理: $PID_FILE"
  rm -f "$PID_FILE"
  exit 0
fi

if ! kill -0 "$pid" 2>/dev/null; then
  log_info "进程 $pid 已不存在，清理 pid 文件"
  rm -f "$PID_FILE"
  exit 0
fi

log_info "停止 PID $pid ..."
kill -TERM "$pid" 2>/dev/null || true

for _ in $(seq 1 "$WAIT_SECS"); do
  if ! kill -0 "$pid" 2>/dev/null; then
    rm -f "$PID_FILE"
    log_info "已停止"
    exit 0
  fi
  sleep 1
done

log_info "SIGTERM 后 ${WAIT_SECS}s 未退出，强制 SIGKILL"
kill -KILL "$pid" 2>/dev/null || true
sleep 0.5
if kill -0 "$pid" 2>/dev/null; then
  log_info "ERROR: 进程 $pid 仍在运行，请手工检查"
  exit 1
fi

rm -f "$PID_FILE"
log_info "已强制停止"
