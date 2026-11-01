# Sidecar Views — Design Specification

Status: **proposed.** Replaces the producer-side `SidecarRows` mechanism specified in
`doc/ATTRIBUTE_TYPE_REGISTRY_SPEC.md` with sidecars defined as SQL views over the rows being
ingested. Measured with the harness in `tools/sidecar-bench`; the numbers below come from it.

## Summary

A queue may declare any number of **sidecars**. Each one is a table beside the queue's target table
and a SQL transformation over the batch's base rows, exposed as `__base`. For every batch:

1. The base rows are written as Parquet to a **local staging directory**, and their upload to the
   table's data path starts at once.
2. Each sidecar is derived **from that staged Parquet**, written to staging, and uploaded as soon as
   it is written.
3. When every upload has finished, one DuckLake transaction registers the base files and every
   sidecar file with `ducklake_add_data_files`, so they commit or roll back together.

No uploader is configured by default. Then the same outputs are written by parallel DuckDB `COPY`s
straight to the destination, as the base is written today; see *Without an uploader*.

## Why change the design

`SidecarRows` carries rows a producer built in Java, rendered as SQL literals, on each `Batch`. That
made every sidecar a code change in a producer, limited sidecars to producers running inside the
server, and needed a typed projection because `ducklake_add_data_files` does not cast.

The case it was built for no longer needs it. The attribute type registry recorded which OTLP value
types an attribute arrived with, which the write path could not see. Since #506 the typed attribute
maps (`attributes_int`, `attributes_double`, `attributes_bool`, `attributes_bytes`, …) are columns
of the base rows, so the registry is a query over them.

A sidecar defined as SQL over the base rows is configuration, not code. It works for any producer,
`/v1/ingest` included; its column types are whatever the query produces, so a file always matches
a table created from the same query; and several sidecars per queue cost nothing extra to support.

## Configuration

Sidecars sit on the queue's entry in `ingestion_queue_table_mapping`, beside its `transformation`:

```hocon
ingestion_queue_table_mapping = [{
    ingestion_queue = "log"
    catalog = "loglake", schema = "main", table = "log"
    transformation = "SELECT *, date_trunc('minute', ts) AS minute FROM __this"

    sidecars = [
        { table = "log_minute_metrics"
          transformation = "SELECT minute, service, severity, count(*) AS n FROM __base GROUP BY ALL" }
        { table = "log_attribute_types"
          transformation_view = "loglake.main.v_log_attribute_types" }
    ]
}]
```

- `__base` is the base table's rows for this batch as written: after the queue's transformation and
  its partition projections.
- `transformation_view` names a view whose SQL is read on the same refresh tick as a view-based
  queue transformation, so a sidecar can change without a restart.
- The queue's ingestion variables apply, so a sidecar can read `getvariable('name')`.
- A sidecar table lives in the queue's catalog and schema.

**Tables are created and checked at startup.** For each sidecar, startup runs its transformation
over an empty `__base` of the base table's columns. A missing table is created from that result's
schema; an existing table whose columns differ fails startup, naming the table and the column. A
batch therefore never meets a missing or mismatched table, and no DDL runs inside the ingestion
transaction.

## Deriving from the staged Parquet, not the raw input

**Decided: from the staged base Parquet.** Measured on 1M rows, deriving three sidecars from the local
base Parquet took 214 to 238 ms against 256 to 377 ms from the raw Arrow input, depending on the
DuckDB version (strategies F and E below); at 100k rows the two were within noise. Parquet lets each
sidecar read only its columns, and the base transformation runs once rather than once per sidecar.

It also makes a sidecar a pure function of the registered base files. A sidecar can be re-derived
later from the files in the table's data path, to backfill a new sidecar or repair one, and gets the
answer the batch got.

**Partitioned queues.** `PARTITION_BY` leaves the partition columns out of the files and in the
directory names. The staged base is read with `hive_partitioning = true` so `__base` keeps every
column the transformation produced.

**Reading the base back from S3 is ruled out.** It was the slowest strategy in every run, roughly
twice the alternatives once latency is realistic.

## Staging locally, then uploading

**Decided: stage locally and upload, overlapping the base's upload with sidecar derivation**
(strategy G below). It was the fastest strategy for large batches on both DuckDB versions and within
about 50 ms of the fastest for small ones.

Two properties come with it besides speed: nothing reaches object storage unless every output was
derived, and a failed upload can be retried without recomputing anything.

**Failure handling.**
- A sidecar that fails to derive fails the batch. Files already uploaded for it are deleted on a
  best-effort basis; any left behind are unregistered, and DuckLake's orphan-file cleanup removes
  them, as it does for a batch's own data file today.
- Staged files are deleted once the transaction commits or the batch fails. On startup the staging
  directory is cleared, since nothing in it belongs to a live batch.

**Disk.** The staging directory holds each in-flight batch's base and sidecar files, so it needs room
for the bucket size times the number of concurrent writes, plus the sidecars. It sits next to the
Arrow temp files the queue already stages.

## The uploader

Nothing in the server uploads a file today: every byte reaches object storage through a DuckDB
`COPY`, and the build carries no cloud SDK. DuckDB cannot upload an existing file as it is, only
re-encode it, so the staged path needs an uploader.

```java
public interface FileUploader {
    /** Uploads {@code local} to {@code destination}, a URI in this uploader's scheme. */
    CompletableFuture<Void> upload(Path local, URI destination);
}
```

One uploader per scheme, chosen by the destination: `s3://` first, a rename for `file://`, and Azure
and GCS later. A destination whose scheme has no uploader uses the direct path.

### Library

| Option | Jars | Size |
|---|---|---|
| AWS SDK v2 `s3` + `s3-transfer-manager` (async, Netty) | 47 | 13.7 MB |
| **AWS SDK v2 `s3` + `sts` + `url-connection-client`, no Netty** | 32 | **8.2 MB** |
| MinIO Java SDK 8.5 | 26 | 22.9 MB |
| Hand-written client: JDK `HttpClient` and SigV4 signing | 0 | 0 MB |

**Recommended: the AWS SDK's `s3` client on its JDK-based `url-connection-client`, without Netty**,
with the uploader running multipart parts and files concurrently on its own executor. It leaves out
the transfer manager and its Netty, which would sit beside the Netty that gRPC and Arrow Flight
already bring, with a version of its own. `sts` is what lets the default credential chain use web
identity (IRSA).

A hand-written client costs nothing in jars, and the S3 calls themselves are small. The credential
chain is not: web identity through STS, EKS Pod Identity and ECS container credentials, and IMDSv2
tokens are each a security-sensitive protocol to implement and keep current. Against an image whose
DuckDB jar alone is 85 MB, 8 MB is not worth that.

The MinIO SDK is not lighter, and talks to AWS S3 no better.

### Authentication

The uploader cannot reuse DuckDB's secrets. DuckDB refuses to show a secret unredacted
(`duckdb_secrets(redact := false)` fails with "Displaying unredacted secrets is disabled"), and the
setting that allows it would show secrets to any query a user is allowed to run.

So both read **one identity from the environment** instead of sharing a secret:

- DuckDB's secret uses the credential chain:

  ```sql
  CREATE SECRET s3_data (TYPE s3, PROVIDER credential_chain, REGION 'us-east-1');
  ```

- The uploader uses the AWS SDK's default chain: environment variables, system properties, web
  identity (IRSA), the shared profile, EKS Pod Identity or ECS container credentials, then IMDSv2.

On Kubernetes that is the pod's IAM role, with no keys anywhere. Static keys, as for MinIO, go in the
environment (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), where both chains read them.

### Uploader configuration

```hocon
ingestion.upload {
    mode = auto                  # auto | staged | direct
    staging_dir = ${java.io.tmpdir}"/dazzleduck-staging"
    max_concurrent_uploads = 16

    s3 {
        region = "us-east-1"
        endpoint = ""            # set for MinIO, R2 and other S3-compatible stores
        path_style = false
        credentials = default    # default (AWS chain) | static
        access_key_id = ${?DD_S3_ACCESS_KEY_ID}           # static only
        secret_access_key = ${?DD_S3_SECRET_ACCESS_KEY}   # static only
        multipart_threshold = 16MiB
        part_size = 8MiB
    }
}
```

`auto`, the default, stages when an uploader is configured for the destination's scheme and writes
directly otherwise. `staged` with no uploader for a destination's scheme fails startup, naming the
queue and the scheme. `direct` never stages.

### Without an uploader

The default. No `ingestion.upload.s3` block means no uploader, and every queue writing to object
storage takes the **direct path**:

- **The base** is written by the same `COPY` straight to its destination as today.
- **Each sidecar** is its own `COPY` straight to its table's data path, over the same source relation
  the base is written from: the batch's Arrow files with the queue's transformation applied, as
  `__base`. The sidecar `COPY`s run in parallel with the base's.
- **Registration** is unchanged: one DuckLake transaction registers every file.

The sidecars are derived from the source relation here rather than from the written base, because
the base is already in object storage and reading it back was the slowest strategy measured (D). They
return the same rows as long as the transformation is deterministic. What is lost is only that the
sidecar files are not computed from the registered files themselves.

This is strategy B below: the fastest for small batches on DuckDB 1.5.6, and about 10 to 30 percent
slower than staging for large ones. It needs nothing beyond the DuckDB secret already configured, and
a queue that declares no sidecars behaves exactly as it does today.

**A local destination** (a filesystem path) never needs an uploader. The base is written in place and
each sidecar is derived from it there, since reading a local file back is cheap; this is strategy F
without the upload.

**Why the uploader is not enabled implicitly.** It is the one component that would talk to object
storage outside DuckDB, with credentials of its own. Turning it on should be a deliberate line of
configuration, not a side effect of credentials happening to be in the environment.

## Benchmark

One batch of OTel-like log rows as uncompressed Arrow IPC (4 files), written as a base table plus the
three sidecars in `tools/sidecar-bench/README.md`, to MinIO behind a proxy adding about 40 ms per
round trip. Median total in ms; DuckDB 1.5.6 and 2.0.0 measured back to back.

| Strategy | 100k rows, 1.5.6 | 100k rows, 2.0.0 | 1M rows, 1.5.6 | 1M rows, 2.0.0 |
|---|---|---|---|---|
| A: raw input, `COPY` to S3 per output, in turn | 458 | 953 | 1,541 | 1,509 |
| B: raw input, `COPY` to S3 per output, in parallel | **253** | 432 | 1,186 | 916 |
| C: transformed base in a temp table, `COPY`s to S3 | 492 | 995 | 1,529 | 1,576 |
| D: base to S3, sidecars read it back from S3 | 961 | 1,213 | 2,236 | 2,142 |
| E: raw input to local Parquet, then parallel upload | 334 | 320 | 1,093 | 894 |
| F: base to local Parquet, sidecars from it, then upload | 341 | 343 | 915 | 841 |
| G: as F, base upload overlapped with sidecars | 307 | **318** | **895** | **809** |

Every strategy wrote the same rows per output, and each output was read back from S3 and counted.

**DuckDB 2.0 cut the direct path's large-batch time** (B: 1,186 to 916 ms), likely from its
asynchronous Parquet writes, **and doubled its small-batch time** (B: 253 to 432 ms). Its HTTP log
shows two `HEAD` requests per file before writing that 1.5.6 does not make, and with real latency
each one is a round trip. The staged strategies do not go through DuckDB's S3 writer and did not
change. Choosing G does not depend on the version, but the direct fallback should be measured again
on each upgrade.

**Caveats.** One machine, synthetic rows, three sidecars, no bandwidth limit, and MinIO rather than
S3. Treat the numbers as directional until they are repeated against a real bucket.

## Effect on other work

- **`SidecarRows` is not needed.** It, `Batch.sidecarRows()` and their plumbing through
  `IngestionResult` go away. The attribute type registry becomes a sidecar view over the
  typed attribute maps, which `doc/ATTRIBUTE_TYPE_REGISTRY_SPEC.md` should be revised to say.
- **The watermark.** `doc/DUCKLAKE_NO_INLINING_SPEC.md` converts the watermark's `INSERT` to a
  write-then-register through `SidecarRows`. Its rows are a `GROUP BY` over the base rows, so it can
  be a sidecar view instead, except for the snapshot id, which is known only at commit. Its
  correction rows stay as #514 made them.
- **Inlining.** Unaffected: every sidecar file is registered, and registration is never inlined.

## Open questions

1. **Dynamic queues.** How the SQLite queue registry stores a queue's sidecars: a JSON column, or a
   child table keyed by queue.
2. **The watermark as a sidecar.** Whether the snapshot id can be supplied at commit so the
   watermark needs no code of its own.
3. **Azure and GCS.** Their SDKs are heavier than the S3 one. The alternative is a small DuckDB
   extension that copies a local file through DuckDB's own file system, which would reuse DuckDB's
   secrets for every cloud and need no Java SDK, at the cost of DuckDB's S3 writer and its 2.0
   `HEAD` requests.
4. **Small files.** Whether a single `PUT` below the multipart threshold closes the 100k-row gap
   between G and B.
5. **Real S3.** The benchmark against a real bucket, before staging is recommended as the production
   setting.
