# Publish Docker Image

Builds and publishes multi-arch Docker images for both `dazzleduck/dazzleduck` and `dazzleduck/dazzleduck-otel-collector` to Docker Hub.

## Steps

1. Verify working tree is clean (warn if uncommitted changes)
2. Read current version from `pom.xml`
3. Run `mvn clean install -DskipTests`
4. Build and push `arm64` + `amd64` images for both targets (4 builds, run in parallel)
5. Create and push multi-arch manifests for `<version>` and `latest` for both images
6. Verify the published manifests with `docker manifest inspect`
7. Report the final digests and all tags

## Instructions

Run the following, stopping immediately on any failure and reporting the error:

### Step 1 — Pre-flight checks

Run these in parallel:
- `git status --short` — if any tracked files are modified or staged, print a warning ("Uncommitted changes detected — image may not match HEAD") but continue
- Extract the version: `grep -m1 '<version>' pom.xml | sed 's/.*<version>\(.*\)<\/version>.*/\1/'`

### Step 2 — Maven install

```bash
export MAVEN_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
./mvnw clean install -DskipTests
```

Fail fast if this returns non-zero.

### Step 3 — Build and push platform images (run all 4 in parallel)

**dazzleduck/dazzleduck arm64:**
```bash
export MAVEN_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED"
./mvnw package -DskipTests jib:build -pl dazzleduck-sql-runtime \
  -Djib.architecture=arm64 \
  -Djib.to.image=docker.io/dazzleduck/dazzleduck:<version>-arm64
```

**dazzleduck/dazzleduck amd64:**
```bash
export MAVEN_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED"
./mvnw package -DskipTests jib:build -pl dazzleduck-sql-runtime \
  -Djib.architecture=amd64 \
  -Djib.to.image=docker.io/dazzleduck/dazzleduck:<version>-amd64
```

**dazzleduck/dazzleduck-otel-collector arm64:**
```bash
export MAVEN_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED"
./mvnw package -DskipTests jib:build -pl dazzleduck-sql-otel-collector \
  -Djib.architecture=arm64 \
  -Djib.to.image=docker.io/dazzleduck/dazzleduck-otel-collector:<version>-arm64
```

**dazzleduck/dazzleduck-otel-collector amd64:**
```bash
export MAVEN_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED"
./mvnw package -DskipTests jib:build -pl dazzleduck-sql-otel-collector \
  -Djib.architecture=amd64 \
  -Djib.to.image=docker.io/dazzleduck/dazzleduck-otel-collector:<version>-amd64
```

### Step 4 — Create and push multi-arch manifests

```bash
# dazzleduck/dazzleduck
docker manifest rm docker.io/dazzleduck/dazzleduck:<version> 2>/dev/null
docker manifest create docker.io/dazzleduck/dazzleduck:<version> \
  docker.io/dazzleduck/dazzleduck:<version>-amd64 \
  docker.io/dazzleduck/dazzleduck:<version>-arm64
docker manifest push docker.io/dazzleduck/dazzleduck:<version>

docker manifest rm docker.io/dazzleduck/dazzleduck:latest 2>/dev/null
docker manifest create docker.io/dazzleduck/dazzleduck:latest \
  docker.io/dazzleduck/dazzleduck:<version>-amd64 \
  docker.io/dazzleduck/dazzleduck:<version>-arm64
docker manifest push docker.io/dazzleduck/dazzleduck:latest

# dazzleduck/dazzleduck-otel-collector
docker manifest rm docker.io/dazzleduck/dazzleduck-otel-collector:<version> 2>/dev/null
docker manifest create docker.io/dazzleduck/dazzleduck-otel-collector:<version> \
  docker.io/dazzleduck/dazzleduck-otel-collector:<version>-amd64 \
  docker.io/dazzleduck/dazzleduck-otel-collector:<version>-arm64
docker manifest push docker.io/dazzleduck/dazzleduck-otel-collector:<version>

docker manifest rm docker.io/dazzleduck/dazzleduck-otel-collector:latest 2>/dev/null
docker manifest create docker.io/dazzleduck/dazzleduck-otel-collector:latest \
  docker.io/dazzleduck/dazzleduck-otel-collector:<version>-amd64 \
  docker.io/dazzleduck/dazzleduck-otel-collector:<version>-arm64
docker manifest push docker.io/dazzleduck/dazzleduck-otel-collector:latest
```

### Step 5 — Verify

```bash
docker manifest inspect docker.io/dazzleduck/dazzleduck:<version>
docker manifest inspect docker.io/dazzleduck/dazzleduck-otel-collector:<version>
```

Confirm both `amd64` and `arm64` platforms appear in each manifest.

### Step 6 — Summary

Print a table:

| Image | Tag | Platforms | Digest |
|-------|-----|-----------|--------|
| `dazzleduck/dazzleduck` | `<version>` | linux/amd64, linux/arm64 | `sha256:...` |
| `dazzleduck/dazzleduck` | `latest` | linux/amd64, linux/arm64 | `sha256:...` |
| `dazzleduck/dazzleduck-otel-collector` | `<version>` | linux/amd64, linux/arm64 | `sha256:...` |
| `dazzleduck/dazzleduck-otel-collector` | `latest` | linux/amd64, linux/arm64 | `sha256:...` |

## Notes

- Jib pushes directly to the registry — Docker daemon is not required for the build step
- The `<version>-arm64` / `<version>-amd64` intermediate tags are pushed by Jib; the canonical multi-arch entry points are `<version>` and `latest`
- If Docker Hub credentials are missing, run: `docker login docker.io`
- sqlite-jdbc uses the multi-platform fat JAR (13.6 MB); platform-specific JARs are not published to Maven Central.
- `dazzleduck-sql-runtime` bundles Flight SQL + HTTP + OTel Collector (all-in-one)
- `dazzleduck-sql-otel-collector` is a lightweight standalone OTel collector image
