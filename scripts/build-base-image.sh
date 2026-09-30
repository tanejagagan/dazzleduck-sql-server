#!/usr/bin/env bash
# Builds and pushes the private base image dazzleduck/base-jre that every Jib image starts from:
#   dazzleduck/base-jre:<java>-noble-duckdb-<duckdb>   (multi-arch: linux/amd64 + linux/arm64)
#
# The Java and DuckDB versions come from the parent pom (java.runtime.version, duckdb.version), so
# the tag always describes what the image actually contains. The DuckDB JDBC jar it ships lands in
# /app/libs, next to the one Jib adds; with matching versions they are the same file.
#
# Usage: scripts/build-base-image.sh [--load]   (--load: build for this host's arch into the local
#        Docker daemon instead of pushing both architectures)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

prop() { sed -n "s:.*<$1>\(.*\)</$1>.*:\1:p" "$ROOT/pom.xml" | head -1; }
JAVA_VERSION="$(prop java.runtime.version)"
DUCKDB_VERSION="$(prop duckdb.version)"
[[ -n "$JAVA_VERSION" && -n "$DUCKDB_VERSION" ]] || { echo "could not read versions from pom.xml" >&2; exit 1; }
TAG="dazzleduck/base-jre:${JAVA_VERSION}-noble-duckdb-${DUCKDB_VERSION}"

args=(--file "$ROOT/dazzleduck-sql-runtime/docker/Dockerfile.base"
      --build-arg "JAVA_VERSION=$JAVA_VERSION" --build-arg "DUCKDB_VERSION=$DUCKDB_VERSION"
      --tag "$TAG")
if [[ "${1:-}" == "--load" ]]; then
  docker buildx build "${args[@]}" --load "$ROOT"
else
  docker buildx build "${args[@]}" --platform linux/amd64,linux/arm64 --push "$ROOT"
fi
echo "built $TAG"
