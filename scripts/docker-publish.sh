#!/usr/bin/env bash
# Build and publish all Docker images with multi-arch manifests.
#
# Usage:
#   ./scripts/docker-publish.sh                        # build + push all arches + manifests
#   ./scripts/docker-publish.sh --local                # build to local Docker daemon only (amd64)
#   ./scripts/docker-publish.sh --arch arm64           # push one arch only (skip manifests)
#   ./scripts/docker-publish.sh --module compactor     # build + push one module only
#   ./scripts/docker-publish.sh --skip-build           # skip jib, only create/push manifests
#
# Module aliases: runtime, compactor, otel-collector, scrapper
#
# Override version:
#   VERSION=0.2.9 ./scripts/docker-publish.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."

# ── Options ───────────────────────────────────────────────────────────────────
LOCAL=false
SKIP_BUILD=false
SINGLE_ARCH=""
MODULE_FILTER=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --local)       LOCAL=true; shift ;;
    --skip-build)  SKIP_BUILD=true; shift ;;
    --arch)        SINGLE_ARCH="$2"; shift 2 ;;
    --module)      MODULE_FILTER="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# ── Version ───────────────────────────────────────────────────────────────────
VERSION="${VERSION:-$(cd "$ROOT" && ./mvnw help:evaluate -Dexpression=project.version -q -DforceStdout 2>/dev/null)}"
echo "Version: $VERSION"

# ── Java env ──────────────────────────────────────────────────────────────────
export JAVA_HOME="${JAVA_HOME:-/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home}"
export MAVEN_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"

MVN="$ROOT/mvnw"

# ── Module registry ───────────────────────────────────────────────────────────
# Format: "alias|maven-module|docker-image|arch-support"  (arch-support: multi|single)
MODULES=(
  "runtime|dazzleduck-sql-runtime|dazzleduck/dazzleduck|multi"
  "compactor|dazzleduck-sql-ducklake-compactor|dazzleduck/ducklake-compactor|multi"
  "otel-collector|dazzleduck-sql-otel-collector|dazzleduck/dazzleduck-sql-otel-collector|multi"
  "scrapper|dazzleduck-sql-scrapper|dazzleduck/dazzleduck-sql-scrapper|single"
)

# Filter to a single module if requested
if [[ -n "$MODULE_FILTER" ]]; then
  filtered=()
  for entry in "${MODULES[@]}"; do
    alias="${entry%%|*}"
    if [[ "$alias" == "$MODULE_FILTER" ]]; then
      filtered+=("$entry")
    fi
  done
  if [[ ${#filtered[@]} -eq 0 ]]; then
    echo "Unknown module: $MODULE_FILTER. Valid: runtime, compactor, otel-collector, scrapper"
    exit 1
  fi
  MODULES=("${filtered[@]}")
fi

# ── Install all JARs to local Maven repo ──────────────────────────────────────
if [[ "$SKIP_BUILD" == false ]]; then
  echo ""
  echo "▶ Installing all modules..."
  "$MVN" install -DskipTests -f "$ROOT/pom.xml"
fi

# ── Build helpers ─────────────────────────────────────────────────────────────
jib_goal() {
  local goal="$1"
  [[ "$LOCAL" == true ]] && goal="${goal/jib:build/jib:dockerBuild}" || true
  echo "$goal"
}

build_multi_arch() {
  local maven_module="$1"
  local arch="$2"
  local goal

  # runtime uses named executions with arch hardcoded; others use -Djib.architecture
  if [[ "$maven_module" == "dazzleduck-sql-runtime" ]]; then
    goal=$(jib_goal "jib:build@docker-${arch}")
    echo ""
    echo "▶ $maven_module ($arch)"
    "$MVN" "$goal" -pl "$maven_module" -DskipTests -f "$ROOT/pom.xml"
  else
    goal=$(jib_goal "jib:build")
    echo ""
    echo "▶ $maven_module ($arch)"
    "$MVN" "$goal" -pl "$maven_module" -Djib.architecture="$arch" -DskipTests -f "$ROOT/pom.xml"
  fi
}

build_single_arch() {
  local maven_module="$1"
  local goal
  goal=$(jib_goal "jib:build")
  echo ""
  echo "▶ $maven_module"
  "$MVN" "$goal" -pl "$maven_module" -DskipTests -f "$ROOT/pom.xml"
}

push_manifests() {
  local image="$1"
  echo ""
  echo "▶ manifest: $image"
  for tag in "$VERSION" "latest"; do
    # Remove stale local manifest cache so --amend picks up freshly pushed arch digests
    docker manifest rm "${image}:${tag}" 2>/dev/null || true
    docker manifest create "${image}:${tag}" \
      "${image}:${tag}-amd64" \
      "${image}:${tag}-arm64"
    docker manifest push "${image}:${tag}"
    echo "  pushed ${image}:${tag}"
  done
}

# ── Build and publish ─────────────────────────────────────────────────────────
ARCHES=("amd64" "arm64")
[[ -n "$SINGLE_ARCH" ]] && ARCHES=("$SINGLE_ARCH")

for entry in "${MODULES[@]}"; do
  IFS='|' read -r alias maven_module image arch_support <<< "$entry"

  if [[ "$SKIP_BUILD" == false ]]; then
    if [[ "$arch_support" == "multi" ]]; then
      for arch in "${ARCHES[@]}"; do
        build_multi_arch "$maven_module" "$arch"
      done
    else
      build_single_arch "$maven_module"
    fi
  fi

  if [[ "$LOCAL" == false && "$arch_support" == "multi" && -z "$SINGLE_ARCH" ]]; then
    push_manifests "$image"
  fi
done

echo ""
echo "✓ Done."
