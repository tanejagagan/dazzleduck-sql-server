# Patched DuckLake Extension

This module's Docker images (both the GraalVM native image and the Jib image) bake in a
custom-built DuckLake extension instead of the stock one DuckDB would otherwise fetch over the
network. This document explains what the patch is, how it's built and published, how the images
consume it, and the exact steps to update it when `duckdb.version` changes.

## What the patch fixes

DuckLake's compaction-conflict check operates at table granularity: two transactions compacting
the same table always conflict, even when they retire completely disjoint sets of files. That
makes it impossible to run this module's tiered compaction (`minor`/`major`/etc.) concurrently
against one table — exactly what the N-tier design (see the module README) depends on.

The fix is [`duckdb/ducklake#1453`](https://github.com/duckdb/ducklake/pull/1453) (not merged
upstream at time of writing), which escalates the conflict check to file granularity. Two
follow-ups are carried alongside it (also unmerged upstream):
[`duckdb/ducklake#1482`](https://github.com/duckdb/ducklake/pull/1482) detects two concurrent
deletes that each replace a file's existing delete file (previously both committed and corrupted
the table), and [`duckdb/ducklake#1484`](https://github.com/duckdb/ducklake/pull/1484) narrows
compaction-vs-delete conflicts to file granularity too, so a compaction no longer fails whenever
the table is updated during it. All three are backported onto `d8a1881e` — the exact DuckLake commit DuckDB `v1.5.5` pins via its own
`.github/config/extensions/ducklake.cmake` — so it's ABI-compatible with the DuckDB build this
repo's `duckdb.version` (`1.5.5.1`) actually ships. That match was verified directly, not just
inferred from the pin file:

```sql
-- against duckdb_jdbc:1.5.5.1
select * from pragma_version();
-- library_version = v1.5.5, source_id = d8cdaa33fd  (matches the duckdb/duckdb v1.5.5 tag exactly)

install ducklake; load ducklake;
select extension_version from duckdb_extensions() where extension_name = 'ducklake';
-- d8a1881e  (the exact backport base)
```

## Where things live

| Component | Location |
|---|---|
| Patched source | [`dazzleduck-web/ducklake`](https://github.com/dazzleduck-web/ducklake), branch `backport/1453-file-level-compaction-conflict` |
| Build + publish workflow | same repo, `.github/workflows/build-and-publish-extension.yml` (manually triggered) |
| Published binaries | GitHub Releases on that repo, e.g. [`v1.5.5-dazzleduck.2`](https://github.com/dazzleduck-web/ducklake/releases/tag/v1.5.5-dazzleduck.2), assets `ducklake-linux_amd64.duckdb_extension` / `ducklake-linux_arm64.duckdb_extension` |
| Version pins in this repo | `dazzleduck-sql-ducklake-compactor/pom.xml`: `ducklake.patch.release`, `ducklake.patch.built.for.duckdb.version`, `ducklake.extension.cache.version` |
| Drift guard | `DuckLakePatchVersionTest` (fails the build if `duckdb.version` diverges from `ducklake.patch.built.for.duckdb.version`, or if `ducklake.extension.cache.version` no longer matches `duckdb.version`'s first three components) |
| Runtime enablement | Two separate connections need it, both set as a JDBC connection **property** (not a `SET` statement — DuckDB rejects changing it once the instance is running): `RawConnections.java` for the compaction/housekeeping connections, and `src/main/resources/duckdb.properties` for the shared commons singleton `Main.java` runs the startup script against first (`ConnectionPool.executeOnSingleton` has no other hook for connection properties) |

The build itself runs via DuckDB's own official `duckdb/extension-ci-tools` reusable workflow —
the same manylinux-style environment official extensions use — so the binaries link against an
old-enough glibc for both `debian:12-slim` (native image runtime) and the Ubuntu Noble Jib base
image.

## How the images consume it

Neither the native Dockerfile nor the Jib packaging talks to the ducklake fork's git repo — both
just download the two published release assets and place them at the exact local path DuckDB
resolves `LOAD ducklake` from: `~/.duckdb/extensions/<ducklake.extension.cache.version>/linux_<arch>/ducklake.duckdb_extension`.

- **`Dockerfile.native`** reads `ducklake.patch.release` and `ducklake.extension.cache.version`
  from the pom, `curl`s the matching-arch asset into that path, then a small Java program opens its
  verification connection with `allow_unsigned_extensions=true` as a **connection property** (not a
  `SET` statement — see "Runtime enablement" above) and calls `LOAD ducklake` to confirm it loads
  before baking it into the runtime stage.
- **Jib packaging** (`pom.xml`) runs an `exec-maven-plugin` execution bound to `generate-resources`
  that `curl`s the asset for whichever `jib.architecture` is being built into
  `target/ducklake-extension/linux_<arch>/`, then Jib's `extraDirectories` copies that arch's
  subdirectory into the image at the same cache path. This download is **skipped by default**
  (`ducklake.extension.download.skip=true`) since `generate-resources` also runs for plain
  `test`/`install`/`verify` and the native profile's own `package`, none of which need this ~36MB
  asset — callers that actually build the jib image pass
  `-Dducklake.extension.download.skip=false` (see `release.yml`, `scripts/docker-publish.sh`, this
  module's own README, `.claude/commands/publish-docker.md`).

Both paths therefore need re-running (a normal Docker/Jib build) whenever the release tag changes
— nothing else to wire up.

## Updating the patch (e.g. a DuckDB version bump)

Whenever `duckdb.version` moves to a new DuckDB release, `DuckLakePatchVersionTest` will fail the
build until the patch is rebuilt for that version. Steps:

1. **Check whether the fix is still needed.** If `duckdb/ducklake#1453` (or an equivalent) has
   since merged upstream and shipped in the new DuckDB's pinned DuckLake commit, the patch may no
   longer be necessary at all — see "Retiring the patch" below instead.

2. **Find the new backport base.** Look up the DuckLake commit the new DuckDB version pins:

   ```bash
   gh api "repos/duckdb/duckdb/contents/.github/config/extensions/ducklake.cmake?ref=vX.Y.Z" \
     --jq '.content' | base64 -d
   ```

   Cross-check it against the actual JDBC driver you're pinning to (not just the DuckDB tag),
   the same way it was verified above, via `pragma_version()` + `duckdb_extensions()`.

3. **Rebase the patch onto the new base**, in a checkout of `dazzleduck-web/ducklake`:

   ```bash
   git fetch origin
   git checkout -b backport/1453-<new-version> <new-base-commit>
   git cherry-pick a881b262 776d5a0d   # the two duckdb/ducklake#1453 commits
   printf 'vX.Y.Z' > .github/duckdb-version   # the DuckDB tag from step 2
   git add .github/duckdb-version
   git commit -m "Pin duckdb-version to vX.Y.Z"
   git push origin backport/1453-<new-version>
   ```

4. **Build and publish** a new release:

   ```bash
   gh workflow run build-and-publish-extension.yml \
     --repo dazzleduck-web/ducklake \
     --ref backport/1453-<new-version> \
     -f duckdb_version=vX.Y.Z \
     -f release_tag=vX.Y.Z-dazzleduck.1
   ```

   Wait for it to complete, then confirm both assets are attached:

   ```bash
   gh release view vX.Y.Z-dazzleduck.1 --repo dazzleduck-web/ducklake
   ```

5. **Update this repo's pins** in `dazzleduck-sql-ducklake-compactor/pom.xml`:
   - `ducklake.patch.release` → the new release tag
   - `ducklake.patch.built.for.duckdb.version` → the new `duckdb.version` (must match exactly —
     this is what `DuckLakePatchVersionTest` checks)
   - `ducklake.extension.cache.version` → the new DuckDB `library_version` string (from
     `pragma_version()`, e.g. `v2.0.0`), used for the extension cache directory name

6. **Verify**: `./mvnw test -pl dazzleduck-sql-ducklake-compactor` should pass, and a Docker/Jib
   build should complete without a 404 on the extension download.

## Retiring the patch

If `duckdb/ducklake#1453` merges upstream and a future DuckDB release's pinned DuckLake commit
already contains the fix, this whole mechanism can be removed:

- `Dockerfile.native`: replace the `curl`-and-place block with a plain `INSTALL ducklake` again.
- `pom.xml`: remove the `download-patched-ducklake-extension` execution, the `extraDirectories`
  block, and the `ducklake.patch.*`/`ducklake.extension.cache.version`/
  `ducklake.extension.download.skip` properties.
- `RawConnections.java`: remove the `allow_unsigned_extensions` connection property.
- Delete `src/main/resources/duckdb.properties`.
- Delete `DuckLakePatchVersionTest` and `src/test/resources-filtered/`.
- Revert the `-Dducklake.extension.download.skip=false` flag added to `release.yml`,
  `scripts/docker-publish.sh`, this module's README, and `.claude/commands/publish-docker.md`.
