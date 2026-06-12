#!/usr/bin/env bash
# Build and publish all Docker images with multi-arch manifests.
#
# Usage:
#   ./scripts/docker-publish.sh                  # build + push both arches + manifests
#   ./scripts/docker-publish.sh --local          # build to local Docker daemon only (amd64)
#   ./scripts/docker-publish.sh --arch arm64     # push one arch only (skip manifests)
#   ./scripts/docker-publish.sh --skip-build     # skip jib, only create/push manifests
#
# Override version:
#   VERSION=0.2.9 ./scripts/docker-publish.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."

# ── Options ──────────────────────────────────────────────────────────────────
LOCAL=false
SKIP_BUILD=false
SINGLE_ARCH=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --local)       LOCAL=true; shift ;;
    --skip-build)  SKIP_BUILD=true; shift ;;
    --arch)        SINGLE_ARCH="$2"; shift 2 ;;
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

# ── Multi-arch images (amd64 + arm64 + manifest) ─────────────────────────────
MULTI_ARCH_MODULES=(
  "dazzleduck-sql-runtime:jib:build@docker-amd64:jib:build@docker-arm64"
  "dazzleduck-sql-ducklake-compactor:jib:build:jib:build"
  "dazzleduck-sql-otel-collector:jib:build:jib:build"
)

MULTI_ARCH_IMAGES=(
  "dazzleduck/dazzleduck"
  "dazzleduck/ducklake-compactor"
  "dazzleduck/dazzleduck-sql-otel-collector"
)

# ── Single-arch images ────────────────────────────────────────────────────────
SINGLE_ARCH_MODULES=(
  "dazzleduck-sql-scrapper"
)

SINGLE_ARCH_IMAGES=(
  "dazzleduck/dazzleduck-sql-scrapper"
)

# ── Install all JARs to local Maven repo ──────────────────────────────────────
if [[ "$SKIP_BUILD" == false ]]; then
  echo ""
  echo "▶ Installing all modules..."
  "$MVN" install -DskipTests -f "$ROOT/pom.xml"
fi

# ── Build function ────────────────────────────────────────────────────────────
build_image() {
  local module="$1"
  local goal="$2"
  local arch="$3"

  if [[ "$LOCAL" == true ]]; then
    # Replace jib:build with jib:dockerBuild for local daemon
    goal="${goal/jib:build/jib:dockerBuild}"
    goal="${goal/jib:build@/jib:dockerBuild@}"
  fi

  echo ""
  echo "▶ $module ($arch) — $goal"
  if [[ "$module" == "dazzleduck-sql-ducklake-compactor" || "$module" == "dazzleduck-sql-otel-collector" ]]; then
    "$MVN" "$goal" -pl "$module" -Djib.architecture="$arch" -DskipTests -f "$ROOT/pom.xml"
  else
    "$MVN" "$goal" -pl "$module" -DskipTests -f "$ROOT/pom.xml"
  fi
}

# ── Build multi-arch images ───────────────────────────────────────────────────
ARCHES=("amd64" "arm64")
[[ -n "$SINGLE_ARCH" ]] && ARCHES=("$SINGLE_ARCH")

for i in "${!MULTI_ARCH_MODULES[@]}"; do
  IFS=: read -r module goal_amd64 _ goal_arm64 <<< "${MULTI_ARCH_MODULES[$i]}"

  if [[ "$SKIP_BUILD" == false ]]; then
    for arch in "${ARCHES[@]}"; do
      if [[ "$arch" == "amd64" ]]; then
        build_image "$module" "$goal_amd64" "amd64"
      else
        build_image "$module" "$goal_arm64" "arm64"
      fi
    done
  fi
done

# ── Build single-arch images ──────────────────────────────────────────────────
if [[ "$SKIP_BUILD" == false ]]; then
  for module in "${SINGLE_ARCH_MODULES[@]}"; do
    echo ""
    echo "▶ $module"
    "$MVN" jib:build -pl "$module" -DskipTests -f "$ROOT/pom.xml"
  done
fi

# ── Create and push multi-arch manifests ──────────────────────────────────────
if [[ "$LOCAL" == false && -z "$SINGLE_ARCH" ]]; then
  echo ""
  echo "▶ Creating multi-arch manifests..."
  for image in "${MULTI_ARCH_IMAGES[@]}"; do
    for tag in "$VERSION" "latest"; do
      echo "  manifest: $image:$tag"
      docker manifest create --amend "${image}:${tag}" \
        "${image}:${tag}-amd64" \
        "${image}:${tag}-arm64"
      docker manifest push "${image}:${tag}"
    done
  done
  echo ""
  echo "✓ All images built and published."
else
  echo ""
  echo "✓ Build complete (manifest step skipped)."
fi
