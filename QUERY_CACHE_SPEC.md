# Query Result Cache — Design Specification

## Overview

File-based Arrow IPC result cache for aggregation and DISTINCT queries.
Disabled by default; enabled via configuration flag with a configurable TTL.

---

## When to Cache (Per Access Mode)

Caching eligibility and TTL resolution differ by access mode:

| Access Mode | Cache Trigger | TTL Source |
|---|---|---|
| **COMPLETE** | Global config: `cache.enabled = true` | `cache.ttl` |
| **READ_ONLY** | Global config: `cache.enabled = true` | `cache.ttl` |
| **RESTRICTED** (`RestrictedDatasourceOnlyAuthorizer`) | Global config: `cache.enabled = true` | `cache.ttl` |
| **RESTRICT_READ_ONLY** (`RestrictedReadOnlyAuthorizer`) | Explicit client header `cache_ttl` | Header value (seconds); default `3600s` if header present but empty |

**RESTRICTED mode** — caching is a server-side administrative decision.
The client has no control. If the admin enables the cache globally, all
eligible queries in this mode are cached.

**RESTRICT_READ_ONLY mode** — caching is opt-in per request by the client.
The client signals intent by sending the `cache_ttl` header:

```
cache_ttl: 3600       # cache this result for 1 hour
cache_ttl:            # cache with server default (3600s)
                      # header absent → never cache
```

The server-side `cache.ttl` config acts as a cap:
if `cache_ttl` from the header exceeds `cache.ttl`, the configured value wins.
This prevents clients from pinning stale data indefinitely.

`cache.enabled = false` disables caching for all modes regardless of the
client header — `NOOP` cache is always used.

---

## Cacheable Queries

A query is eligible for caching if its top-level SELECT node satisfies any of:

- `group_expressions` is non-empty (GROUP BY present)
- `select_list` contains an aggregate function:
  `count`, `sum`, `avg`, `min`, `max`, `count_star`,
  `approx_count_distinct`, `stddev`, `variance`
- `distinct: true` on the SELECT_NODE

Detection via a single `Transformations.isCacheable(JsonNode statement)` method.
Non-SELECT statements (INSERT, UPDATE, EXPLAIN, etc.) are never cached.

---

## Cache Key

```
SHA-256(
    canonical_sql          // AST round-trip: parseToTree → parseToSql
  + "|" + database
  + "|" + schema
  + "|" + sorted_claims    // key=value pairs, sorted by key, joined by "|"
)
→ hex string (64 chars)
```

**Claims included in key:** `filter`, `table`, `access`, `database`, `schema`
**Claims excluded:** `producer_id`, `query_id`, `access-type`

---

## File Layout

Two files per cache entry, stored flat in the cache directory:

```
{cache_dir}/
  {hash}.arrow                       ← Arrow IPC stream content
  {expiry_epoch_seconds}_{hash}.marker   ← empty marker file for housekeeping
```

**Why two files:**
- `{hash}.arrow` — O(1) direct lookup by hash (filename is fully known at lookup time)
- `{expiry}_{hash}.marker` — housekeeping reads only filenames (no file opens),
  sorted lexicographically = chronologically

**Marker filename example:**
```
1746748800_a3f2c9d1e4b7a2f8c3d9e1b4a7f2c8d3.marker
```

Epoch seconds (10 digits, valid until year 2286) sort lexicographically = numerically,
so the oldest entries appear first with no padding required.

---

## Arrow File Format

```
[line 1]  JSON header\n
[line 2]  \n                    ← blank separator line
[rest ]   Arrow IPC stream bytes
```

**JSON header fields:**

| Field        | Type    | Required | Description                              |
|--------------|---------|----------|------------------------------------------|
| `query`      | string  | yes      | canonical SQL (for collision verification) |
| `database`   | string  | yes      | database name                            |
| `schema`     | string  | yes      | schema name                              |
| `ttl`        | long    | yes      | TTL in seconds (informational)           |
| `created_at` | string  | yes      | ISO-8601 instant                         |
| `snapshot_id`| long    | no       | optional data version; null if absent    |

The header is informational and used only for collision verification on lookup.
Expiry is derived from the marker filename, not the header.

---

## Write Sequence

On a cache miss after query execution:

```
1. Execute query → collect Arrow IPC bytes
2. Write {hash}.tmp.{uuid}   (temp Arrow file, not yet visible)
3. Write {expiry}_{hash}.marker  (empty file, atomic on POSIX)
4. Rename {hash}.tmp.{uuid} → {hash}.arrow  (atomic on POSIX)
```

Step 3 before step 4: if the process crashes between 3 and 4, an orphaned marker
exists without an arrow file. Housekeeping detects and cleans orphaned markers.

---

## Lookup Sequence

```
1. Compute hash from cache key
2. Check {hash}.arrow exists          → no  → MISS
3. Glob *_{hash}.marker               → none → MISS + delete orphan arrow file
4. Parse expiry from marker filename
5. expiry < now?                      → yes → MISS + delete both files
6. Read arrow file header (1 line)
7. Verify query/database/schema match → no  → MISS + delete both files
8. Parse and stream Arrow IPC body    → HIT
```

Step 7 guards against SHA-256 collisions (astronomically rare) and any
key construction bugs.

---

## Housekeeping

### At Application Startup
Delete all files in the cache directory (`*.arrow`, `*.marker`, `*.tmp.*`).
Ensures no stale entries survive a restart with a different TTL configuration.

### Periodic (Scheduled)
```
1. List all *.marker files in cache dir
2. Sort lexicographically (= chronological by expiry)
3. Walk from oldest:
   a. Parse expiry from filename
   b. If expiry < now → delete {hash}.arrow + {hash}.marker
   c. If expiry >= now → STOP (all remaining entries are valid)
4. Delete orphaned *.arrow files with no corresponding *.marker
5. Delete leftover *.tmp.* files older than 10 minutes
```

No file opens required for steps 1–3. Only orphan detection (step 4)
requires a set intersection of arrow and marker filenames.

Scheduled via the existing `ScheduledExecutorService` in `DuckDBFlightSqlProducer`.

---

## Configuration (HOCON)

```hocon
dazzleduck_server {
    cache {
        enabled              = false
        ttl                  = 300s
        directory            = ${warehouse}"/query_cache"
        housekeeping_interval = 60s
    }
}
```

`enabled = false` → `QueryResultCache.NOOP` is used; no directory is created.

---

## Classes

### `dazzleduck-sql-commons`

**`CacheKey`**
```
static String compute(String canonicalSql, String database, String schema,
                       Map<String,String> authClaims) → hex SHA-256
```

**`CacheHeader`** (record)
```
String query, String database, String schema,
long ttl, Instant createdAt, Long snapshotId
─────────────────────────────────────────────
String  toJson()
static CacheHeader fromJson(String line)
```

**`QueryResultCache`** (interface)
```
Optional<Path>  lookup(String hash, CacheHeader expected)
void            store(String hash, CacheHeader header, byte[] arrowIpc)
QueryResultCache NOOP  ← always miss, no-op store
```

**`FileBasedQueryResultCache`** (implements QueryResultCache)
```
constructor: (Path cacheDir, long ttlSeconds)
─────────────────────────────────────────────
lookup  → steps 2–8 from Lookup Sequence above
store   → steps 1–4 from Write Sequence above
```

**`CacheHousekeeping`** (implements Runnable)
```
constructor: (Path cacheDir)
run()  → Periodic Housekeeping steps above
```

### `dazzleduck-sql-flight`

**`DuckDBFlightSqlProducer`**
- Add `QueryResultCache cache` constructor parameter
- Default: `QueryResultCache.NOOP`
- Intercept in `getStreamStatement(StatementHandle)`:
  - Call `Transformations.isCacheable(ast)` → skip cache if false
  - Compute `CacheKey.compute(...)` with auth claims from context
  - `cache.lookup(hash, expectedHeader)` → HIT: stream from file
  - MISS: execute → `cache.store(...)` → stream from written file

**`FlightSqlProducerFactory`**
- Read `cache.*` config keys
- If `cache.enabled = true`: create `FileBasedQueryResultCache` + schedule `CacheHousekeeping`
- Else: use `QueryResultCache.NOOP`
- Pass cache instance to all producer constructors

---

## Invariants

- A `{hash}.arrow` file without a `{hash}.marker` is an orphan → delete on housekeeping
- A `{hash}.marker` without a `{hash}.arrow` is a partial write → delete on housekeeping
- Lookup never serves an entry without verifying both expiry and header fields
- Startup wipe ensures TTL config changes take effect immediately on restart
- `QueryResultCache.NOOP` is the default; no cache directory is created unless enabled
