#!/usr/bin/env bash
set -euo pipefail

# Requirements: bash, jq, awk, sed, mktemp, timeout
# Usage: ./cli_test.sh [/path/to/spqr-router]
BIN="${1:-./spqr-router}"

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required. Please install jq." >&2
  exit 1
fi

PASS=0
FAIL=0
TMPDIR="$(mktemp -d)"
LOG="$TMPDIR/router.log"
PID_FILE="$TMPDIR/router.pid"

# Graceful cleanup on script exit (success or failure)
cleanup() {
  if [[ -n "${TAIL_PID:-}" ]] && kill -0 "$TAIL_PID" 2>/dev/null; then
    kill "$TAIL_PID" 2>/dev/null || true
  fi

  if [[ -f "$PID_FILE" ]]; then
    PID="$(cat "$PID_FILE" || true)"
    if [[ -n "${PID:-}" ]] && kill -0 "$PID" 2>/dev/null; then
      kill "$PID" 2>/dev/null || true
      sleep 0.5
      kill -9 "$PID" 2>/dev/null || true
    fi
  fi
  rm -rf "$TMPDIR"
}
trap cleanup EXIT

# Prepare an empty log file and start tailing it live to the console
: > "$LOG"
tail -n0 -F "$LOG" &
TAIL_PID=$!

echo "[i] Using config: $CFG"
echo "[i] Logs:          $LOG"

# --- Helpers ---

# Count how many times the log contains the "Running config:" marker
count_configs() {
  if [[ -f "$LOG" ]]; then
    grep -c 'Running config:' "$LOG" || echo 0
  else
    echo 0
  fi
}

# Wait until the number of "Running config:" occurrences reaches target_count
# Arguments:
#   $1 target_count
#   $2 timeout_s (optional, default 10s)
wait_for_new_config() {
  local target_count="$1"
  local timeout_s="${2:-10}"
  local start_ts
  start_ts="$(date +%s)"
  while (( $(date +%s) - start_ts < timeout_s )); do
    local c
    c="$(count_configs | tr -d '[:space:]')"
    if [[ -z "$c" ]]; then
      c=0
    fi
    if (( c >= target_count )); then
      return 0
    fi
    sleep 0.2
  done
  echo "[!] Timeout waiting for Running config (#$target_count)" >&2
  cat $LOG
  return 1
}

# Extract the JSON blob printed after the latest "Running config:" entry.
extract_last_config_json() {
    awk '
    /Running config:/ {
    # стартуем с первой "{" в этой строке
    p = index($0, "{")
    if (p) {
        out = 1; depth = 0
        s = substr($0, p)
        print s
        depth += gsub(/\{/, "&", s)
        depth -= gsub(/\}/, "&", s)
        next
    }
    }
    out {
    print
    depth += gsub(/\{/, "&")
    depth -= gsub(/\}/, "&")
    if (depth == 0) exit
    }
    ' "$LOG"
}

assert_eq() {
  local name="$1" expected="$2" actual="$3"
  if [[ "$expected" == "$actual" ]]; then
    echo "[PASS] $name = $actual"
    PASS=$((PASS+1))
  else
    echo "[FAIL] $name expected='$expected' actual='$actual'"
    FAIL=$((FAIL+1))
  fi
}

assert_true() {
  local name="$1" actual="$2"
  if [[ "$actual" == "true" ]]; then
    echo "[PASS] $name is true"
    PASS=$((PASS+1))
  else
    echo "[FAIL] $name expected=true actual='$actual'"
    FAIL=$((FAIL+1))
  fi
}

# Validate key fields of the effective config JSON
check_effective_config() {
  local json="$1"
  local want_log_level="${2:-info}"
  local want_router_port="${3:-6545}"
  local want_drb="${4:-BLOCK}"
  local want_show_notice="${5:-true}"

  local LOG_LEVEL ROUTER_PORT DRB SHOW_NOTICES
  LOG_LEVEL="$(echo "$json" | jq -r '.log_level')"
  ROUTER_PORT="$(echo "$json" | jq -r '.router_port')"
  DRB="$(echo "$json" | jq -r '.query_routing.default_route_behaviour')"
  SHOW_NOTICES="$(echo "$json" | jq -r '.show_notice_messages')"

  assert_eq "log_level" "$want_log_level" "$LOG_LEVEL"
  assert_eq "router_port" "$want_router_port" "$ROUTER_PORT"
  assert_eq "query_routing.default_route_behaviour" "$want_drb" "$DRB"
  assert_true "show_notice_messages" "$SHOW_NOTICES"
}

OPTS=( run -c "$CFG" --log-level=info --router-port=6545 --default-route-behaviour=block --show-notice-messages --pretty-log )


# Start the binary unbuffered (line-buffer stdout/stderr) and redirect to $LOG
set +e
stdbuf -oL -eL "$BIN" "${OPTS[@]}" >>"$LOG" 2>&1 &
PID=$!
set -e
echo "$PID" >"$PID_FILE"

# 1) Wait for the first "Running config" occurrence (startup)
target=1
echo "[i] Waiting for Running config #$target..."
wait_for_new_config "$target" 15

# Parse and assert startup effective config
JSON="$(extract_last_config_json)"
if [[ -z "$JSON" ]]; then
  echo "[!] Failed to extract startup effective config JSON"
  exit 1
fi
echo "[i] Asserting startup effective config..."
check_effective_config "$JSON"

# 2) Send SIGHUP and expect another "Running config" (reload)
echo "[i] Sending SIGHUP..."
kill -HUP "$PID" 2>/dev/null || true

target=$((target+1))
wait_for_new_config "$target" 10

# Parse and assert post-SIGHUP effective config (overrides should persist)
JSON2="$(extract_last_config_json)"
if [[ -z "$JSON2" ]]; then
  echo "[!] Failed to extract post-SIGHUP effective config JSON"
  exit 1
fi
echo "[i] Asserting effective config after SIGHUP (should keep CLI overrides)..."
check_effective_config "$JSON2"

# 4) Stop the process gracefully; fall back to SIGKILL if needed
echo "[i] Stopping process..."
if [[ -f "$PID_FILE" ]]; then
  PID="$(cat "$PID_FILE" || true)"
  if [[ -n "${PID:-}" ]] && kill -0 "$PID" 2>/dev/null; then
    kill -USR2 "$PID" 2>/dev/null || kill "$PID" 2>/dev/null || true
    sleep 1
    if kill -0 "$PID" 2>/dev/null; then
      echo "[!] Process $PID still alive, killing -9"
      kill -9 "$PID" 2>/dev/null || true
    fi
  fi
fi

echo "===================="
echo "PASS: $PASS"
echo "FAIL: $FAIL"
echo "===================="
if [[ $FAIL -ne 0 ]]; then
  echo "Some checks failed. See log: $LOG"
  exit 1
fi