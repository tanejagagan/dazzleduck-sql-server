# Publish Docker Image

Builds and publishes a multi-arch Docker image for `dazzleduck/dazzleduck` to Docker Hub.

## Steps

1. Verify working tree is clean (warn if uncommitted changes)
2. Read current version from `pom.xml`
3. Run `mvn clean install -DskipTests`
4. Build and push `arm64` image tagged `<version>-arm64` and `latest-arm64`
5. Build and push `amd64` image tagged `<version>-amd64` and `latest-amd64`
6. Create and push multi-arch manifest for `<version>` (amd64 + arm64)
7. Create and push multi-arch manifest for `latest` (amd64 + arm64)
8. Verify the published manifest with `docker manifest inspect`
9. Report the final digest and all tags

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

### Step 3 — Build and push platform images (run in parallel)

**arm64:**
```bash
export MAVEN_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED"
./mvnw package -DskipTests jib:build -pl dazzleduck-sql-runtime \
  -Djib.architecture=arm64 \
  -Djib.to.image=docker.io/dazzleduck/dazzleduck:<version>-arm64
```

**amd64:**
```bash
export MAVEN_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED"
./mvnw package -DskipTests jib:build -pl dazzleduck-sql-runtime \
  -Djib.architecture=amd64 \
  -Djib.to.image=docker.io/dazzleduck/dazzleduck:<version>-amd64
```

### Step 4 — Create and push multi-arch manifests

```bash
# versioned tag
docker manifest rm docker.io/dazzleduck/dazzleduck:<version> 2>/dev/null
docker manifest create docker.io/dazzleduck/dazzleduck:<version> \
  docker.io/dazzleduck/dazzleduck:<version>-amd64 \
  docker.io/dazzleduck/dazzleduck:<version>-arm64
docker manifest push docker.io/dazzleduck/dazzleduck:<version>

# latest tag
docker manifest rm docker.io/dazzleduck/dazzleduck:latest 2>/dev/null
docker manifest create docker.io/dazzleduck/dazzleduck:latest \
  docker.io/dazzleduck/dazzleduck:<version>-amd64 \
  docker.io/dazzleduck/dazzleduck:<version>-arm64
docker manifest push docker.io/dazzleduck/dazzleduck:latest
```

### Step 5 — Verify

```bash
docker manifest inspect docker.io/dazzleduck/dazzleduck:<version>
```

Confirm both `amd64` and `arm64` platforms appear in the manifest.

### Step 6 — Summary

Print a table:

| Tag | Platforms | Digest |
|-----|-----------|--------|
| `dazzleduck/dazzleduck:<version>` | linux/amd64, linux/arm64 | `sha256:...` |
| `dazzleduck/dazzleduck:latest` | linux/amd64, linux/arm64 | `sha256:...` |

## Notes

- Jib pushes directly to the registry — Docker daemon is not required for the build step
- The `latest-arm64` / `latest-amd64` tags are side-effects of the Jib config in `pom.xml`; they are intermediate tags and not the canonical multi-arch entry points
- If Docker Hub credentials are missing, run: `docker login docker.io`
- The `dazzleduck-sql-runtime` module is the Jib target; it transitively bundles Flight SQL, HTTP, and OTel Collector
