# GraalVM native image for the OTel collector — feasibility & plan

## Goal

Ship `dazzleduck-sql-otel-collector` as a GraalVM **native image** as an alternative to the
current JVM/jib image, for **fast startup and low memory footprint** (a collector/sidecar
workload). The JVM image stays the default; native is an additional artifact.

## Status — native build works end-to-end (linux/arm64)

A native `collector` binary of the real `Main` builds, parses args, and **starts the OTLP gRPC
server on 4317 in ~2 s**, both directly and inside a container.

- Build: `./mvnw -Pnative -pl dazzleduck-sql-otel-collector -am -DskipTests package` (a GraalVM
  JDK 21 with `native-image` on PATH), or `docker build -f dazzleduck-sql-otel-collector/Dockerfile.native .`
- Binary ~116 MB (embeds the DuckDB `.so`); runtime image on `debian:12-slim` ~358 MB.
- Reachability metadata is committed under `src/main/resources/META-INF/native-image/` (captured
  by the tracing agent against this project's exact dependency versions). The shipped GraalVM
  metadata repo is **disabled** in the profile — its entries are for older versions and one fails
  to link (`ScopedMemoryAccess.closeScope0`).
- The metadata covers the **full ingest path**: captured by running `OtelCollectorDuckLakeTest`
  (OTLP logs/traces/metrics → Arrow → DuckLake) under the agent, not just startup (reflect 234 →
  379 entries).
- The image **pre-installs the DuckDB `arrow` + `ducklake` extensions** for the pinned engine
  version so the collector loads them without network. Extensions are keyed to engine version and
  platform, so the build must run on the target arch.
- **linux/arm64 only** so far (bundled DuckDB `.so` + extensions are arm64); an amd64 image needs
  its own agent capture and extension install.

Still open: a true black-box OTLP smoke (external client sends an export with a JWT carrying the
`x-dd-ingestion-queue` claim → assert Parquet/DuckLake rows); `-H:+StripDebugInfo` + tailored base
for size; amd64; JFR/Arrow-reflection cleanup; CI. See the plan below.

## Feasibility — proven by a spike

A minimal native binary exercising only the two risky dependencies — **DuckDB (JNI)** and
**Apache Arrow (off-heap/Unsafe)** — was built and run on Linux `arm64`:

- Native binary ran: DuckDB `SELECT` returned correctly and an Arrow off-heap vector allocated and
  read back. **Both work under native image.**
- Native binary size: **84.5 MB**, unstripped, with debug info. It embeds the DuckDB Linux `.so`
  (about 54 MB) as a resource — extracted and `System.load`-ed at runtime.
- Runtime image measured (binary copied onto a base):
  - `debian:12-slim` base: **256 MB**, runs correctly.
  - `distroless/cc-debian12`: 168 MB, but **fails** — missing `libz.so.1`.

### Size levers (path to roughly 90–110 MB)

- **Strip debug info** (`-H:+StripDebugInfo`, or `strip` in a build stage) — the current binary is
  unstripped; a large chunk is debug info.
- **Tailored distroless base** carrying exactly `glibc`, `libstdc++`, `libgcc_s`, and `libz`
  (DuckDB is C++ and pulls zlib). `distroless/cc` alone is missing `libz`.

## Known issues to clear (all non-fatal in the spike)

- **Logback/SLF4J build-time init** — native build aborts unless
  `--initialize-at-build-time=org.slf4j,ch.qos.logback` is set. (Confirmed fix.) Alternatively
  swap Logback for `slf4j-simple` in the native profile.
- **Arrow `InaccessibleObjectException`** on `java.nio.Buffer.address` / `DirectByteBuffer` — there
  is no runtime `--add-opens` in a native image, so Arrow's fast-path reflection is blocked and it
  falls back (allocation still worked). Set Arrow's memory-access option to remove the fallback and
  the stack traces.
- **JFR `UnsatisfiedLinkError`** — JFR is not wired in the community image; harmless unless Java
  Flight Recorder is used. Disable/ignore JFR.

## Plan

1. **Build profile.** Add a `native` Maven profile to the module using
   `org.graalvm.buildtools:native-maven-plugin`, main `io.dazzleduck.sql.otel.collector.Main`. Keep
   the jib JVM image as the default build.
2. **Reachability metadata.** Run the GraalVM tracing agent against the **real** `Main` (gRPC on
   4317) under realistic traffic — send OTLP logs/traces/metrics and trigger a DuckLake ingest — to
   capture reflect/JNI/resource/proxy/serialization config for DuckDB, Arrow, gRPC, Netty,
   protobuf, Jackson, jjwt, jcommander, and HOCON. Commit the result under
   `src/main/resources/META-INF/native-image/`. Pull shipped metadata for Netty/gRPC/protobuf from
   the GraalVM reachability-metadata repository.
3. **native-image flags** (in the profile):
   - `--no-fallback`
   - `--initialize-at-build-time=org.slf4j,ch.qos.logback`
   - `--initialize-at-run-time=io.netty` (plus Netty specifics as metadata dictates)
   - Arrow memory-access option so `java.nio` access succeeds without the fallback
   - `-H:IncludeResources` for `reference.conf` / `application.conf` / `logback.xml` and the DuckDB
     `.so`
   - `-H:+StripDebugInfo` for the release variant
4. **Runtime image.** Multi-stage Dockerfile: build stage
   `ghcr.io/graalvm/native-image-community:21`; runtime stage a tailored distroless
   (`glibc` + `libstdc++` + `libgcc_s` + `libz`) or `debian:12-slim`. Expose 4317. Ensure a
   writable temp dir for the DuckDB `.so` extraction (writable `java.io.tmpdir`, or pre-extract the
   `.so` into the image and point the loader at it).
5. **CI.** Native builds are heavy (multi-GB RAM, minutes) — use a dedicated runner. Build per-arch
   (`amd64` + `arm64`) and publish a manifest, matching the jib image's `${jib.architecture}`
   convention.
6. **Testing.** A container smoke test: start the native binary, send an OTLP export
   (grpcurl or an SDK), assert Parquet/DuckLake rows are written, and check `/health`.

## Tradeoffs

Native buys **startup speed and low RSS**; it costs a **larger-than-typical binary** (the DuckDB
`.so` dominates) and **lower JIT peak throughput**. DuckDB already does the heavy lifting in native
C++, so the JVM overhead is mostly startup and memory, not query speed. Keep the jib JVM image as
the throughput-oriented default; offer the native image for edge, sidecar, and fast-scale
scenarios.

## Effort and risk

- Metadata generation, Arrow/JFR/logback flags, Dockerfile and CI: about **2–4 days**.
- Main residual risk: the full `Main` path (gRPC server plus ingest) may surface more reflection or
  JNI entries than the spike did — the tracing agent against real traffic covers most of these.

## Open decisions

- **Base image:** tailored distroless (smaller, more setup) vs `debian:12-slim` (larger, simpler).
- **Strip debug info:** strip for release (smaller, harder to debug native crashes); optionally keep
  an unstripped debug variant.
- **Architectures:** ship both `amd64` and `arm64`, or `arm64` first.
