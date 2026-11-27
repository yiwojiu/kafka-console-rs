#!/usr/bin/env bash
# kafka-console-rs 启动脚本（Linux）
#
# 用法:
#   ./start.sh                    # 默认 0.0.0.0:7755
#   KAFKA_CONSOLE_LISTEN=0.0.0.0:8080 ./start.sh
#
# 约定:
#   - 必须和可执行文件、static/ 放在同一目录（程序从 cwd 找 static/）
#   - PID 写到 kafka-console.pid，日志追加到 logs/kafka-console.log
#   - 启动后自动做一次 /api/health 探活，失败会把日志尾部打出来

set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$BASE_DIR/kafka-console-rs"
PID_FILE="$BASE_DIR/kafka-console.pid"
LOG_DIR="$BASE_DIR/logs"
LOG_FILE="$LOG_DIR/kafka-console.log"

LISTEN="${KAFKA_CONSOLE_LISTEN:-0.0.0.0:7755}"
PORT="${LISTEN##*:}"
# 探活优先走回环地址，监听在 0.0.0.0 / :: 时也能连上
PROBE_HOST="127.0.0.1"

log_info() { printf '%s\n' "$*"; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

is_running() {
  local pid="${1:-}"
  [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

# ---- 已有实例检查 -----------------------------------------------------------
if [ -f "$PID_FILE" ]; then
  old_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if is_running "$old_pid"; then
    log_info "已经在运行 (PID $old_pid)。要重启请先执行 ./stop.sh"
    exit 1
  fi
  log_info "清理残留的 pid 文件 (PID $old_pid 已不存在)"
  rm -f "$PID_FILE"
fi

# ---- 运行环境检查 -----------------------------------------------------------
[ -x "$BIN" ] || die "找不到可执行文件或没有执行权限: $BIN（试试 chmod +x kafka-console-rs）"
[ -f "$BASE_DIR/static/index.html" ] || die "缺少 static/index.html —— 前端页面必须和可执行文件放在同一目录"

mkdir -p "$LOG_DIR"

# 端口占用先提示一下，免得日志里看不懂
if command -v ss >/dev/null 2>&1; then
  if ss -ltn 2>/dev/null | awk '{print $4}' | grep -qE "[:.]${PORT}$"; then
    log_info "提示: 端口 $PORT 已被占用，启动可能失败"
  fi
fi

# ---- 启动 -------------------------------------------------------------------
export KAFKA_CONSOLE_LISTEN="$LISTEN"
cd "$BASE_DIR"

printf '\n===== %s start =====\n' "$(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
nohup "$BIN" >> "$LOG_FILE" 2>&1 &
new_pid=$!
echo "$new_pid" > "$PID_FILE"

# ---- 探活（最多 10 秒） -----------------------------------------------------
for _ in $(seq 1 50); do
  if ! is_running "$new_pid"; then
    printf 'ERROR: 进程启动后立即退出，日志尾部：\n' >&2
    tail -n 20 "$LOG_FILE" >&2
    rm -f "$PID_FILE"
    exit 1
  fi
  if command -v curl >/dev/null 2>&1; then
    code="$(curl -s -o /dev/null -m 1 -w '%{http_code}' "http://$PROBE_HOST:$PORT/api/health" 2>/dev/null || true)"
    if [ "$code" = "200" ]; then
      log_info "已启动: PID $new_pid"
      log_info "访问:   http://$LISTEN"
      log_info "日志:   $LOG_FILE"
      exit 0
    fi
  fi
  sleep 0.2
done

if ! command -v curl >/dev/null 2>&1; then
  log_info "已启动(未装 curl，跳过探活): PID $new_pid，访问 http://$LISTEN，日志 $LOG_FILE"
  exit 0
fi

printf 'WARN: 进程在运行(PID %s)，但 10 秒内 /api/health 未返回 200，请看日志: %s\n' "$new_pid" "$LOG_FILE" >&2
exit 1
