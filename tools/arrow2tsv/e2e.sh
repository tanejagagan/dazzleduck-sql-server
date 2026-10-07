#!/usr/bin/env bash
# End-to-end check of an arrow2tsv binary against a DazzleDuck server: each query is fetched as
# Arrow through arrow2tsv and compared with the server's own TSV for the same query.
#
#   ./e2e.sh path/to/arrow2tsv                         # build and start the server from this checkout
#   ./e2e.sh path/to/arrow2tsv http://localhost:8081   # use a server that is already running
#
# Starting the server needs JDK 25 (JAVA_HOME, or java on the PATH). It is built with the Maven
# wrapper, runs HTTP only on $E2E_PORT (default 18081) with a temporary warehouse in the timezone
# $E2E_SERVER_TZ (default Asia/Kolkata), and is stopped
# when the script exits; its log is printed if it fails to come up.
#
# The queries stick to types both sides render the same way. Where the server's TSV is known to be
# wrong (temporals inside lists/structs, UTINYINT, blobs, values containing tabs) arrow2tsv is
# checked against a literal expectation instead.
set -euo pipefail

bin=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
failures=0

work=""
start_server() {
  local repo port java
  repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
  port=${E2E_PORT:-18081}
  java=${JAVA_HOME:+$JAVA_HOME/bin/}java
  work=$(mktemp -d)
  base=http://localhost:$port

  echo "building the server from $repo"
  export MAVEN_OPTS="${MAVEN_OPTS:---enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow}"
  (cd "$repo" && ./mvnw -B -q -ntp install -DskipTests -Dmaven.javadoc.skip=true -Dmaven.source.skip=true \
      -pl dazzleduck-sql-runtime -am &&
    ./mvnw -B -q -ntp dependency:build-classpath -pl dazzleduck-sql-runtime -Dmdep.outputFile="$work/cp.txt")

  echo "starting the server on $base"
  # The Arrow flags are the root pom's arrow.jvm.flags. The server runs in a non-UTC timezone, which
  # DuckDB puts on TIMESTAMPTZ columns, so a client that renders in the column's zone shows up here.
  TZ=${E2E_SERVER_TZ:-Asia/Kolkata} "$java" --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED \
    --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --enable-native-access=ALL-UNNAMED \
    --sun-misc-unsafe-memory-access=allow \
    -cp "$repo/dazzleduck-sql-runtime/target/classes:$(cat "$work/cp.txt")" \
    io.dazzleduck.sql.runtime.Main \
    --conf "dazzleduck_server.warehouse=\"$work/warehouse\"" \
    --conf "dazzleduck_server.http.port=$port" \
    --conf 'dazzleduck_server.networking_modes=[http]' \
    > "$work/server.log" 2>&1 &
  server_pid=$!
  mkdir -p "$work/warehouse"
  trap 'kill "$server_pid" 2> /dev/null || true; wait "$server_pid" 2> /dev/null || true; rm -rf "$work"' EXIT

  for _ in $(seq 1 120); do
    curl -sf "$base/health" > /dev/null && return 0
    kill -0 "$server_pid" 2> /dev/null || break
    sleep 1
  done
  echo "the server did not come up; its log:" >&2
  cat "$work/server.log" >&2
  exit 1
}

if [ $# -ge 2 ]; then
  base=$2
else
  start_server
fi

token=$(curl -sf -X POST "$base/v1/login" -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin"}' | sed -E 's/.*"accessToken":"([^"]+)".*/\1/')
[ -n "$token" ] || { echo "login failed" >&2; exit 1; }

server_tsv() {
  curl -sf -G -H "Authorization: Bearer $token" -H 'Accept: text/tab-separated-values' \
    --data-urlencode "q=$1" "$base/v1/query"
}

check() { # name, expected, actual
  if [ "$2" == "$3" ]; then
    echo "ok    $1"
  else
    echo "FAIL  $1"
    diff <(printf '%s\n' "$2") <(printf '%s\n' "$3") || true
    failures=$((failures + 1))
  fi
}

same_as_server() { # name, query
  check "$1" "$(server_tsv "$2")" "$("$bin" "$base/v1/query" -q "$2" -t "$token")"
}

same_as_server scalars "select 1 as i, -2::bigint b, 'text' s, NULL::int n, true t, 3.14::decimal(5,2) dc,
  1.5::double f, DATE '2024-01-01' d, TIMESTAMP '2024-01-01 00:00:01.5' ts,
  TIMESTAMPTZ '2024-01-01 00:00:00+00' tz, TIMESTAMPTZ '2024-01-01 00:00:01.5+00' tzf,
  TIME '12:00:00' tm, TIME '12:00:00.25' tmf, 'é漢' u"
same_as_server nested "select [1, NULL, 3] l, ['a', NULL, '', 'q\"t'] sl, []::int[] e, NULL::int[] nl,
  {'x': 1, 'y': 'z', 'n': {'i': [1.5::double, 'nan'::double, 'inf'::double]}} st,
  MAP {'k': [1, 2]} m, [{'a': true}] ls"
same_as_server multi_row "select i, i::varchar s, [i, i + 1] l from range(5) t(i)"
same_as_server empty "select 1 as a where false"

check escaping $'s\na\\tb\\\\c' "$("$bin" "$base/v1/query" -q "select 'a'||chr(9)||'b\\c' s" -t "$token")"
check nested_temporal $'d\n["2024-01-01",null]' \
  "$("$bin" "$base/v1/query" -q "select [DATE '2024-01-01', NULL] d" -t "$token")"
check utinyint $'u\n255' "$("$bin" "$base/v1/query" -q "select 255::utinyint u" -t "$token")"

# A large result streams through stdin too, and stops cleanly when the reader goes away.
check stdin_large 1000001 "$(curl -sf -H "Authorization: Bearer $token" \
  "$base/v1/query?q=select%20*%20from%20range(1000000)" | "$bin" | wc -l | tr -d ' ')"
check closed_pipe $'range\n0' "$("$bin" "$base/v1/query" -q "select * from range(10000000)" -t "$token" | head -2)"

# Errors are reported with the server's message and a non-zero exit.
if err=$("$bin" "$base/v1/query" -q "select bogus" -t "$token" 2>&1); then
  check http_error "non-zero exit" "exit 0"
else
  check http_error yes "$(grep -q 'Referenced column "bogus" not found\|bogus' <<<"$err" && echo yes || echo "$err")"
fi

[ "$failures" -eq 0 ] || { echo "$failures check(s) failed"; exit 1; }
echo "all checks passed"
