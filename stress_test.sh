#!/usr/bin/env bash
# stress_test.sh — stress test cho io-multiplexing HTTP server.
#
# Hỗ trợ Linux (epoll), macOS (kqueue), Windows (WSAPoll) qua Git Bash/WSL/MSYS.
# Tự build server theo OS hiện tại, khởi chạy nền, bắn N request với C kết nối
# song song, in tỉ lệ thành công và throughput. Nếu có `wrk`, `ab` hoặc `hey`,
# script ưu tiên dùng tool đó để có số liệu chính xác hơn.
#
# Cách dùng:
#   ./stress_test.sh                       # mặc định: 5000 req, 100 concurrent
#   ./stress_test.sh -n 20000 -c 200       # tuỳ chỉnh
#   ./stress_test.sh -p 9090 -d 30s        # đổi port, dùng wrk với duration
#   ./stress_test.sh --no-build            # không build, dùng binary có sẵn
#   ./stress_test.sh --keep-server         # giữ server sau khi test (debug)

set -u

REQUESTS=5000
CONCURRENCY=100
PORT=8080
DURATION="10s"
DO_BUILD=1
KEEP_SERVER=0
PATH_TARGET="/hello"

usage() {
    cat <<EOF
Usage: $0 [options]
  -n, --requests N      tổng số request (curl/ab/hey)            [default: $REQUESTS]
  -c, --concurrency C   số kết nối song song                      [default: $CONCURRENCY]
  -p, --port P          port server lắng nghe                     [default: $PORT]
  -d, --duration D      thời lượng test khi dùng wrk (vd 30s)     [default: $DURATION]
      --no-build        không build, dùng binary đã có
      --keep-server     không kill server sau khi test
  -h, --help            in trợ giúp
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        -n|--requests)    REQUESTS="$2"; shift 2 ;;
        -c|--concurrency) CONCURRENCY="$2"; shift 2 ;;
        -p|--port)        PORT="$2"; shift 2 ;;
        -d|--duration)    DURATION="$2"; shift 2 ;;
        --no-build)       DO_BUILD=0; shift ;;
        --keep-server)    KEEP_SERVER=1; shift ;;
        -h|--help)        usage; exit 0 ;;
        *) echo "unknown option: $1" >&2; usage; exit 2 ;;
    esac
done

# Phát hiện OS để chọn tên binary + lệnh đo thời gian phù hợp.
detect_os() {
    case "$(uname -s 2>/dev/null)" in
        Linux*)                       echo "linux" ;;
        Darwin*)                      echo "darwin" ;;
        CYGWIN*|MINGW*|MSYS*|Windows*) echo "windows" ;;
        *) echo "unknown" ;;
    esac
}

OS="$(detect_os)"
BIN="./io-mux-server"
[ "$OS" = "windows" ] && BIN="./io-mux-server.exe"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "==> OS detected: $OS"
echo "==> Working dir: $SCRIPT_DIR"

if [ "$DO_BUILD" -eq 1 ]; then
    command -v go >/dev/null 2>&1 || { echo "go not found in PATH"; exit 1; }
    echo "==> Building server -> $BIN"
    go build -o "$BIN" . || { echo "build failed"; exit 1; }
fi

[ -x "$BIN" ] || { echo "binary $BIN not found or not executable"; exit 1; }

# Khởi chạy server nền, log ra file để debug nếu test fail.
SERVER_LOG="$(mktemp -t io-mux-server.XXXXXX.log 2>/dev/null || echo "/tmp/io-mux-server.$$.log")"
echo "==> Starting server on :$PORT (log: $SERVER_LOG)"
"$BIN" ":$PORT" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

cleanup() {
    if [ "$KEEP_SERVER" -eq 0 ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "==> Stopping server (pid $SERVER_PID)"
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

# Đợi server mở port (tối đa ~5s). Dùng /dev/tcp nếu bash hỗ trợ, fallback curl.
wait_for_port() {
    local i
    for i in $(seq 1 50); do
        if (exec 3<>/dev/tcp/127.0.0.1/"$PORT") 2>/dev/null; then
            exec 3<&- 3>&- 2>/dev/null
            return 0
        fi
        if command -v curl >/dev/null 2>&1 && \
           curl -fsS -o /dev/null --max-time 1 "http://127.0.0.1:$PORT$PATH_TARGET" 2>/dev/null; then
            return 0
        fi
        sleep 0.1
    done
    return 1
}

if ! wait_for_port; then
    echo "server did not start on port $PORT. Server log:"
    cat "$SERVER_LOG" || true
    exit 1
fi
echo "==> Server is up"

URL="http://127.0.0.1:$PORT$PATH_TARGET"

# Ưu tiên wrk > hey > ab > curl-parallel: wrk/hey cho throughput chuẩn nhất.
run_with_wrk() {
    echo "==> Using wrk: -t$CONCURRENCY -c$CONCURRENCY -d$DURATION $URL"
    wrk -t"$CONCURRENCY" -c"$CONCURRENCY" -d"$DURATION" "$URL"
}

run_with_hey() {
    echo "==> Using hey: -n $REQUESTS -c $CONCURRENCY $URL"
    hey -n "$REQUESTS" -c "$CONCURRENCY" "$URL"
}

run_with_ab() {
    echo "==> Using ab: -n $REQUESTS -c $CONCURRENCY $URL"
    ab -n "$REQUESTS" -c "$CONCURRENCY" "$URL"
}

# Fallback: curl chạy song song qua xargs -P. Đo wall-clock bằng SECONDS để
# tránh phụ thuộc `date +%s.%N` (macOS BSD date không hỗ trợ %N).
run_with_curl() {
    command -v curl >/dev/null 2>&1 || { echo "no curl/ab/hey/wrk available"; return 1; }
    echo "==> Using curl in parallel ($REQUESTS requests, $CONCURRENCY concurrent)"
    local tmp_ok tmp_fail
    tmp_ok="$(mktemp)"; tmp_fail="$(mktemp)"
    local start_s=$SECONDS
    # In status code per request; phân loại 200 vs còn lại.
    seq "$REQUESTS" | \
        xargs -n1 -P"$CONCURRENCY" -I{} \
            curl -s -o /dev/null -w "%{http_code}\n" --max-time 5 "$URL" \
        | awk -v ok="$tmp_ok" -v bad="$tmp_fail" '
            { if ($1 == "200") print >> ok; else print >> bad }
          '
    local elapsed=$(( SECONDS - start_s ))
    [ "$elapsed" -lt 1 ] && elapsed=1
    local ok_n bad_n
    ok_n=$(wc -l < "$tmp_ok" | tr -d ' ')
    bad_n=$(wc -l < "$tmp_fail" | tr -d ' ')
    rm -f "$tmp_ok" "$tmp_fail"
    local rps=$(( ok_n / elapsed ))
    echo "----"
    echo "Total:       $REQUESTS"
    echo "OK (200):    $ok_n"
    echo "Failed:      $bad_n"
    echo "Elapsed:     ${elapsed}s"
    echo "Throughput:  ~${rps} req/s"
    [ "$bad_n" -eq 0 ]
}

RESULT=0
if   command -v wrk >/dev/null 2>&1;  then run_with_wrk  || RESULT=$?
elif command -v hey >/dev/null 2>&1;  then run_with_hey  || RESULT=$?
elif command -v ab  >/dev/null 2>&1;  then run_with_ab   || RESULT=$?
else                                       run_with_curl || RESULT=$?
fi

if [ "$RESULT" -ne 0 ]; then
    echo "==> Stress test reported failures. Server log tail:"
    tail -n 40 "$SERVER_LOG" || true
fi

exit "$RESULT"
