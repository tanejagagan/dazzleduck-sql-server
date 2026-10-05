# Tuning a Postgres-backed DuckLake Catalog

DuckLake keeps its metadata in a SQL database. When that database is Postgres, the catalog becomes
an OLTP workload sitting underneath an analytical one, and it arrives with **no indexes and no
foreign keys at all** — only five primary keys across twenty-nine tables. This note records which
indexes are worth adding, why partitioning is the wrong tool here, and what changes if you use a
DuckDB file as the catalog instead.

Everything below was measured rather than reasoned about. See [Method](#method) for the setup and
its limits before trusting a specific number.

## What to do

```sql
CREATE INDEX CONCURRENTLY ON ducklake_data_file          (table_id, begin_snapshot);
CREATE INDEX CONCURRENTLY ON ducklake_delete_file        (table_id, begin_snapshot);
CREATE INDEX CONCURRENTLY ON ducklake_file_column_stats  (table_id, column_id);
CREATE INDEX CONCURRENTLY ON ducklake_file_column_stats  USING brin (data_file_id);
```

All four are zero-downtime, need no schema migration, and leave every primary key untouched. Do not
partition these tables — see [Why not partitioning](#why-not-partitioning).

If you run the compaction service, also summarise the BRIN index at the start of each housekeeping
pass, before `ducklake_merge_adjacent_files`:

```sql
SELECT brin_summarize_new_values('<brin index name>');
```

## What the catalog actually looks like

A freshly created Postgres-backed catalog:

```
PRIMARY KEYS:  ducklake_data_file(data_file_id), ducklake_delete_file(delete_file_id),
               ducklake_schema(schema_id), ducklake_snapshot(snapshot_id),
               ducklake_snapshot_changes(snapshot_id)
FOREIGN KEYS:  none
OTHER INDEXES: none
```

Seventeen of the twenty-nine tables carry `table_id`, and none of them has an index on it. Every
per-table metadata lookup is therefore a sequential scan until you add one.

## How DuckLake queries the catalog

Captured with `log_statement = all`. DuckLake reads through the Postgres scanner, so every read
arrives as `COPY (SELECT ...) TO STDOUT (FORMAT binary)` against a single table — it never pushes a
join into Postgres.

Only some reads carry a `table_id` predicate:

| Table | Predicate | Prunable by `table_id` |
|---|---|---|
| `ducklake_data_file` | `table_id = ? AND begin_snapshot <= ?` | yes |
| `ducklake_data_file` | `begin_snapshot <= ?` (attach-time), and bare, no `WHERE` (housekeeping) | no |
| `ducklake_delete_file` | `table_id = ? AND begin_snapshot <= ?` | yes |
| `ducklake_table_stats`, `ducklake_table_column_stats`, `ducklake_schema_versions` | `table_id = ?` | yes |
| `ducklake_file_column_stats` | `column_id = ? AND table_id = ?` | yes |
| `ducklake_file_column_stats` | `data_file_id IN (...)` (DELETE, from compaction) | **no** |
| `ducklake_column`, `ducklake_column_tag`, `ducklake_partition_info`, `ducklake_partition_column`, `ducklake_sort_info`, `ducklake_sort_expression`, `ducklake_table` | none | no |

Two consequences worth internalising:

- `ducklake_column` is read **whole**, with no `table_id` filter, to build the schema cache on
  attach. Anything that makes a full read of it more expensive makes every attach slower.
- Stats are fetched **one statement per referenced column**. Measured on a 60-column table:

  ```
  filter on  1 column(s):  48 statements to postgres,  1 touching file_column_stats
  filter on 10 column(s):  57 statements to postgres, 10 touching file_column_stats
  filter on 30 column(s):  77 statements to postgres, 30 touching file_column_stats
  ```

  A fixed base of roughly 47 statements plus one per filtered column, each a round trip. On wide
  tables, **narrowing projections and filters reduces catalog traffic linearly** — the cheapest
  lever available, and it costs nothing to apply.

## `ducklake_file_column_stats` — the index that matters most

This is the largest and write-hottest table in the catalog: one row per file **per column**, so a
50-column table writes 50+ catalog rows per Parquet file. Benchmarked at 3.6M rows (60k files x 60
columns, 375 MB heap, 20 tables):

| Index | Read query | Plan | Buffers |
|---|---|---|---|
| none | 38.40 ms | Parallel Seq Scan | 47,998 |
| `(table_id)` | 8.71 ms | Index Scan | 2,559 |
| `(column_id)` | 33.02 ms | Bitmap Heap Scan | 48,054 |
| **`(table_id, column_id)`** | **1.05 ms** | Index Scan | 2,408 |
| `(table_id, column_id) INCLUDE (payload)` | 0.21 ms | Index Only Scan | 36 |

`(table_id, column_id)` is 8.3x faster than `(table_id)` alone, because `column_id` supplies most of
the selectivity once a table has many columns:

```
table_id=5 only             -> 180,000 rows
column_id=7 only            ->  60,000 rows
table_id=5 AND column_id=7  ->   3,000 rows
```

With 60 columns, `column_id` contributes a 60x reduction — and the fewer tables you have, the more
of the work it is doing. `column_id` **alone** is nearly worthless (33 ms, barely better than a scan).

The covering `INCLUDE` variant is another 5x but costs 264 MB against 25 MB, and index-only scans
depend on a current visibility map, so on a table this write-heavy they decay toward heap fetches
between vacuums. Start without it.

### Why BRIN for `data_file_id`

Compaction deletes stats by file, with no `table_id`:

```sql
DELETE FROM ducklake_file_column_stats WHERE data_file_id IN (...);
```

A composite index cannot serve this — Postgres 17 has no skip scan, so a non-leading `data_file_id`
is unusable. You need a second, `data_file_id`-leading index whatever you choose:

| Indexes present | DELETE 6,000 rows | Plan |
|---|---|---|
| `(table_id, column_id)` only | 139.99 ms | Seq Scan |
| `(table_id, column_id, data_file_id)` only | 138.96 ms | **Seq Scan** |
| `(data_file_id)` btree | 1.20 ms | Index Scan |
| `(data_file_id)` BRIN | 4.10 ms | Bitmap Heap Scan |

BRIN is viable because `data_file_id` comes from the monotonic `next_file_id` counter and all rows
for a file are written together, so the heap is already in `data_file_id` order:

```
correlation of data_file_id = 0.9999642
```

BRIN is preferable because of the write asymmetry. A BRIN insert creates no index tuple at all — it
only widens the current range summary, which for an ascending column usually means extending the
max. No per-row entry, no page splits:

| Insert 60k stats rows | Time | WAL | Index size |
|---|---|---|---|
| no indexes | 57 ms | 7.8 MB | — |
| `btree(data_file_id)` only | 85 ms | 11.9 MB | 29 MB |
| `btree(table_id, column_id)` only | 72 ms | 12.0 MB | 33 MB |
| both btree | 116 ms | 16.1 MB | 54 MB |
| **BRIN + `btree(table_id, column_id)`** | **86 ms** | **12.0 MB** | **33 MB + 32 kB** |

Note that `btree(data_file_id)` is the *more* expensive of the two on insert despite being on an
ascending column — it has one entry per row and no duplicates to deduplicate. Swapping it for BRIN
is a 900x smaller index for 2.9 ms on a batch delete.

### BRIN summarisation, and why the compactor cares

BRIN cannot prune ranges it has not summarised, and compaction targets *recently written* files —
exactly the unsummarised tail:

```
DELETE on just-inserted ids, BRIN default        9.68 ms  [7107 buffers]
DELETE on just-inserted ids, autosummarize = on  8.63 ms  [7139 buffers]
after brin_summarize_new_values()                  —      [1107 buffers]
```

`autosummarize = on` does not help promptly, because it piggybacks on autovacuum. Since the bundled
minor-compaction tier runs every minute against freshly written files, the unsummarised case is the
*normal* case here — which is still only ~10 ms per pass, so it does not change the recommendation,
but an explicit `brin_summarize_new_values()` at the top of the housekeeping pass makes it ~1 ms.

### Leave `deduplicate_items` alone

It is on by default and is why `(table_id, column_id)` is affordable at all — with thousands of
files per table there are thousands of duplicate keys per pair, collapsed into posting lists:

```
dedup ON  (default)   83 ms insert,  33 MB index
dedup OFF             98 ms insert, 147 MB index
```

Two measured non-levers: `fillfactor = 100` made inserts *worse* (94 ms vs 83 ms), and
`wal_compression = lz4` left WAL volume unchanged at exactly 12.0 MB, because this workload's WAL is
new-tuple records rather than full-page images.

## `ducklake_data_file` — the file listing

`ducklake_data_file` holds the per-table file listing (one row per Parquet file); `ducklake_delete_file`
is its delete-file companion. `ducklake_list_files('<catalog>','<table>')` reads exactly these two.

It is roughly 1/60th the size of the stats table — one row per file, not per file per column — so an
index is sufficient and partitioning is not warranted. At 300k rows (100 tables x 3000 files):

| Configuration | File listing (3000 rows) |
|---|---|
| flat, no index | 9.06 ms |
| hash-partitioned(8), no index | 1.70 ms |
| **flat + `btree(table_id, begin_snapshot)`** | **0.25 ms** |
| partitioned(8) + local btree | 0.26 ms |

The index is worth 36x. Partitioning *on top of* the index is worth nothing.

## Why not partitioning

Partitioning works — a hash-partitioned catalog was verified end to end, with reads, writes, schema
evolution, time travel, `ducklake_merge_adjacent_files`, `ducklake_expire_snapshots` and
`ducklake_cleanup_old_files` all passing against it, data intact. It is simply the wrong tool.

**It gets worse as the catalog grows.** At 3M rows across 1000 tables, interleaved:

| | File listing | Attach-time scan (no `table_id`) |
|---|---|---|
| flat + index | **3.29 ms** | **208 ms** |
| hash-partitioned(8) + index | 4.63 ms | 429 ms |

The locality argument collapses: with 8 partitions and 1000 tables each partition still holds ~125
interleaved tables, so clustering is no better than flat, but you have paid for an 8-way `Append`.

**Partition-only, with no index, needs an impractical partition count.** Execution time is linear in
rows-per-partition:

```
n=8   -> 375,000 rows/partition -> 11.48 ms
n=64  ->  46,875 rows/partition ->  3.86 ms
n=256 ->  11,718 rows/partition ->  2.28 ms
```

At n=256 it finally matches `flat + index` (2.86 ms vs 2.85 ms) — but the attach-time scan becomes 2x
worse (434 ms vs 222 ms), planning time grows from 0.15 ms to 5.4 ms, and the write saving that
motivated it shrinks from 31% at n=8 to **7% at n=256**. To hold that read latency as the catalog
grows you must keep re-partitioning, and hash partition counts are the thing that is painful to change.

**And an index-free partitioned table is not actually available.** `ducklake_data_file` already has
`PRIMARY KEY (data_file_id)`, and Postgres refuses a unique constraint that omits the partition key:

```
ERROR: unique constraint on partitioned table must include all partitioning columns
```

So you would widen it to `(data_file_id, table_id)` and carry a local PK index on every partition.
That weakening is safe in itself — `data_file_id` uniqueness is maintained by the `next_file_id`
allocator, not the constraint — but it means the write saving above is an optimistic bound.

Finally, two of `ducklake_data_file`'s three access shapes have no `table_id` at all, so partitioning
cannot help them, and one is a bare full scan from housekeeping.

## Pruning and parallelism

Both work, and they are different things. Pruning is what matters.

Because DuckLake inlines literals (`table_id = '42'`), you get **plan-time** pruning — the best case,
with no `Append` node at all. Run-time pruning also works if the driver ever switches to prepared
statements (`Parallel Append` with `Subplans Removed: 7`).

Parallel query applies even to `COPY (SELECT ...) TO STDOUT` — it is not treated like a cursor — but
it barely helps this workload:

| Query shape | Serial | Parallel | Gain |
|---|---|---|---|
| `count(*)` across partitions | 201 ms | 84 ms (4 workers) | 2.4x |
| `SELECT *` all rows (what DuckLake does) | 234 ms | 191 ms (forced) | 1.2x |

Full row sets are exactly the shape the planner declines to parallelise; forcing it required zeroing
`parallel_tuple_cost` and `parallel_setup_cost` for a 1.2x return. Since each worker is a backend
process and `max_parallel_workers` defaults to 8 cluster-wide, a catalog serving many concurrent
clients is usually better off with `max_parallel_workers_per_gather = 0` or `1`, so metadata lookups
stay cheap and predictable. `enable_partitionwise_join` is off by default and irrelevant here, since
DuckLake never joins in Postgres.

## DuckDB as the catalog instead

A DuckDB file is faster on every axis, and disqualified for shared use.

Same DuckLake content both ways — 60-column table, 1001 files, 60,060 `file_column_stats` rows, 1002
snapshots:

| Operation | Postgres | DuckDB | Ratio |
|---|---|---|---|
| `ATTACH` (cold) | 21 ms | **2 ms** | 10x |
| `ATTACH` + `count(*)` over 1001 files | 47 ms | **13 ms** | 3.6x |
| filter on 1 column | 90 ms | **44 ms** | 2.0x |
| filter on 30 columns | 270 ms | **159 ms** | 1.7x |
| filter on 55 columns | 475 ms | **315 ms** | 1.5x |
| 1001 commits (write path) | 8.27 s | **5.73 s** | 1.4x |
| catalog size | 14 MB | **4.0 MB** | 3.5x |

Read timings have a 42 ms process-startup baseline subtracted. The advantage narrows as filter width
grows because only ~2.1 ms per column is catalog access; the rest is Parquet decode, which both pay.

The disqualifier is locking. DuckDB takes an exclusive OS lock on the catalog file for the process
lifetime, so while one process holds it, a second writer **and a `READ_ONLY` attach** both fail:

```
IO Error: Could not set lock on file "meta.ducklake":
Conflicting lock is held in .../duckdb (PID ...) by user ...
```

Two concurrent writers against the Postgres catalog both completed fine. So a DuckDB catalog suits
single-process, embedded, CI and dev use; anything with concurrent readers or writers needs Postgres.

Also note that none of the index tuning above transfers. DuckDB supports **ART only** — `BTREE`,
`HASH`, `BRIN`, `GIN`, `GIST`, `HNSW` and `ZONEMAP` are all rejected with `Unknown index type` — there
is no `PARTITION BY` at all, and on the catalog query an ART index made no difference (8 ms with and
without) while costing 0.4 s to build and 37 MB of file growth. Pruning there comes from zonemaps,
which are automatic.

## Write amplification, and the one structural lever

The indexes above cost roughly 3x on inserts into `ducklake_file_column_stats` (57 ms -> 86 ms per
60k rows with BRIN, versus 116 ms with two btrees). That is worth 8x on reads and 115x on the
compaction delete, but it has to be budgeted.

The structural fix is to write fewer stats rows, and since the table is `files x columns`, file count
is the only dial. There is no DuckLake setting to skip stats for uninteresting columns — the
available knobs are:

```
ducklake_default_data_inlining_row_limit | 10
ducklake_max_retry_count                 | 10
ducklake_retry_backoff / retry_wait_ms
ducklake_target_file_size                | NULL
ducklake_write_deletion_vectors          | false
```

So raising `ducklake_target_file_size` — or letting compaction keep up — halves the insert load on
every one of these tables for every halving of file count. Note the setting floors at the Parquet
row-group size, so very small values do not produce proportionally more files.

## Method

Postgres 17.5 and DuckDB 1.5.5 with the `ducklake` extension, in Docker on a single macOS machine,
`shared_buffers` 512 MB–1 GB.

Access patterns are real: captured from DuckLake 1.5.5 with `log_statement = all` against live
catalogs. A different DuckLake version may emit different SQL, so re-capture before relying on the
predicate table.

Index benchmarks are **synthetic**: catalog tables generated to match DuckLake's real DDL, column
types and insertion order, then populated to the stated scale. They are best-of-N statement
execution times with a warm cache and a single client, excluding commit fsync. Treat them as
relative comparisons between index choices, not as absolute latency predictions. In particular:

- No concurrency was tested. If many clients hit the catalog at once, fsync and lock contention may
  dominate, and the parallelism advice above becomes more important than the index choice.
- Heap correlation drives several conclusions. The BRIN recommendation depends on
  `data_file_id` correlation staying near 1.0; if the stats table is ever rewritten or re-clustered
  such that it breaks, BRIN degrades toward a full scan and a btree becomes the right choice again.
- The `ducklake_data_file` partitioning conclusion was tested at 300k and 3M rows, with both
  contiguous and interleaved physical layouts. Partitioning did win in one case — 100 tables with a
  scattered layout, 2.25 ms vs 1.42 ms — and lost once table count grew to 1000.
