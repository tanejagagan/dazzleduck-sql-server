# OTel Attribute Type Registry — Design Specification

Status: **agreed, not yet merged.** This is the design to review; the implementation follows as its
own PR. Everything below has been prototyped against the real ingestion path and the numbers are
measured, not estimated — so the open points are the design's, not unknowns about feasibility.

Builds on the typed attribute maps shipped in 0.2.24 (#506).

## Problem

The collector writes each attribute to the map column its OTLP value type selects —
`attributes`, `attributes_int`, `attributes_double`, `attributes_bool`, `attributes_bytes`,
`attributes_json` (shipped in 0.2.24). The same attribute name can arrive with different types
over time: a service sends `x` as a string in one release and as an int in the next, so its values
move from `attributes` to `attributes_int`. Nothing in the data records that, so finding `x` means
searching every map.

Goals: answer "which types has `x` had, and when did each first appear?" without scanning data;
let a tool pick the right typed map or build a unified view; and cost no measurable collector
throughput.

Non-goals: enforcing or coercing types (ingestion stays schema-on-read — a type change is
recorded, not rejected); value statistics; exact first-seen times across a fleet.

## The design

| | |
|---|---|
| **Where** | Whatever table each stream's own config names — `attribute_types_table` in that queue's mapping, resolved in the queue's catalog and schema |
| **When** | In the same transaction that registers the batch's Parquet files |
| **Created by** | The collector, `CREATE TABLE IF NOT EXISTS` in that same transaction, so a missing table is created by the first batch that needs it |
| **Shape** | Append-only, duplicates tolerated, nothing ever updated or deleted |
| **Dedup** | A per-process cache of committed mappings, built from the tables at startup |
| **Scope** | DuckLake queues only; opt-in per stream |

### Key and table

A mapping is `(ingestion_queue, signal, attribute_scope, attribute_name, value_type)`:

```sql
CREATE TABLE <attribute_types_table> (
    ingestion_queue  VARCHAR     NOT NULL,
    signal           VARCHAR     NOT NULL,   -- logs | traces | metrics
    attribute_scope  VARCHAR     NOT NULL,   -- attribute | resource | event | link
    attribute_name   VARCHAR     NOT NULL,
    value_type       VARCHAR     NOT NULL,   -- string | int | double | bool | bytes
                                             -- | array | kvlist | empty
    first_seen       TIMESTAMPTZ NOT NULL,
    collector_id     VARCHAR);
```

`array` and `kvlist` stay distinct although both land in `attributes_json`, because the key is the
OTLP `AnyValue` case, not the column.

### Configuration

Per stream, in the queue's own mapping — optional, and the only switch that turns recording on.
How many tables there end up being is whatever these name:

```hocon
ingestion_queue_table_mapping = [
    {
        ingestion_queue = "logs"
        catalog = "my_catalog", schema = "main", table = "logs"
        additional_parameters {
            attribute_types_table = "otel_attribute_types"
        }
    },
    {
        # Same schema, same name: this stream shares the table above.
        ingestion_queue = "traces"
        catalog = "my_catalog", schema = "main", table = "spans"
        additional_parameters {
            attribute_types_table = "otel_attribute_types"
        }
    },
    {
        # Same schema, a different name: its own table, next to the one above.
        ingestion_queue = "audit"
        catalog = "my_catalog", schema = "main", table = "audit"
        additional_parameters {
            attribute_types_table = "audit_attribute_types"
        }
    },
    {
        # No parameter: this stream records nothing and is not even scanned.
        ingestion_queue = "debug"
        catalog = "my_catalog", schema = "main", table = "debug"
    }
]
```

Process-wide, with nothing in it that names a table:

```hocon
otel_collector {
    attribute_types {
        enabled = true        # honour those per-queue tables at all; false turns it off everywhere
        max_entries = 100000  # cap on mappings held in memory, across every stream
    }
}
```

Both levels are optional and default to harmless: `enabled` is on, but with no queue naming a table
nothing is recorded and nothing is scanned.

### Query patterns

```sql
-- the current type of each attribute
SELECT ingestion_queue, signal, attribute_scope, attribute_name,
       arg_max(value_type, first_seen) AS current_type
FROM otel_attribute_types GROUP BY ALL;

-- the ones that have had more than one
SELECT ingestion_queue, signal, attribute_scope, attribute_name,
       list(DISTINCT value_type) AS types
FROM otel_attribute_types GROUP BY ALL
HAVING count(DISTINCT value_type) > 1;

-- the deduplicated history, earliest sighting across collectors
SELECT ingestion_queue, signal, attribute_scope, attribute_name, value_type,
       min(first_seen) AS first_seen
FROM otel_attribute_types GROUP BY ALL;
```

Every read groups, because the table is append-only and duplicates are expected.

## Decisions

**The table is per-stream config, not a fixed layout.** `watermark_table` already works this way:
an unqualified name in the queue's mapping, resolved in that queue's own catalog and schema. So the
registry lands beside the tables it describes and a query can join them, and the operator decides
everything else — two streams may name one table and share it, their rows kept apart by
`ingestion_queue`, or each may name its own, and a stream that names none records nothing.

Nothing here imposes one table per schema, or any other arrangement; the shape of the deployment is
the config's to choose. What follows from it is only that resolution happens in
`DuckLakePostIngestionTask`, the one place that knows which catalog and schema a batch went to —
which is also why the name cannot be configured process-wide: with a dynamic queue registry the
collector does not know a queue's schema until a batch arrives.

**Written in the ingestion transaction, not by a background writer.** A background writer was
built first and replaced. The transaction gives the property that matters — a registered file
always has its mappings committed — and costs the property it had: a row the database rejects now
fails the batch, where a writer would have retried it off the ingest path. That is mitigated, not
eliminated: every statement is generated from a fixed column list with escaped literals, every
identifier is checked against `[A-Za-z_][A-Za-z0-9_]*`, and an attribute name that cannot be
written as a SQL literal at all (a NUL byte) is dropped rather than sent. The mechanism is
`SidecarRows` in commons, the producer-side counterpart to `WatermarkSpec`: rows derived by
whoever built the batch, from information the write path never sees.

**The collector creates the table.** `CREATE TABLE IF NOT EXISTS`, with the schema above, in the
queue's catalog and schema, queued ahead of the insert in that same transaction. So the first batch
carrying a new mapping creates the table and records into it atomically, and nothing has to be
provisioned to turn the feature on — which is what the opt-in-by-config model implies. An operator
who would rather own the DDL can create it beforehand; the statement is then a no-op.

This is also the other half of why the startup cache load tolerates a missing table: before the
first batch there is nothing to read, and that is the normal state of a newly configured stream,
not an error.

**Append-only, duplicates tolerated.** The insert reads nothing, so it cannot conflict with
another process appending the same value and adds no scan to the transaction. Two collectors
seeing a mapping for the first time both record it; a reader deduplicates with `GROUP BY` over the
five key columns, taking `min(first_seen)`. An earlier design used an anti-join to converge on one
row; it was dropped because tolerating duplicates is cheaper and conflict-free, which matters more
than a tidy table.

**The cache is built at startup.** One query per distinct registry table, restricted to the queues
this process serves, so a restart does not re-append what it already recorded and another
process's streams are not cached for nothing. It never fails startup: a table that does not exist
yet is where every deployment begins, and any other failure costs duplicate rows in a table that
tolerates them.

**A batch carries every mapping that is not yet committed, and is settled by its own outcome.** A
mapping is marked known only once a batch that carried it has committed; a batch that fails changes
nothing. Marking a mapping known when it was merely sent would lose it on any rollback.

What a batch carries is decided against committed mappings only — *not* against what other batches
have in flight. An earlier draft skipped a mapping another in-flight batch was already carrying,
which is wrong, and in a way that defeats the whole point of the in-transaction write. With batches
A and B both containing a new `x:int`: A is built and carries it; B is built while A is in flight,
sees it as already handled and carries nothing; B commits; A fails. B's files are registered with
no `x:int` row to explain them. A later batch would repair it, but until then — and forever, if `x`
never arrives as an int again — the data holds a mapping the registry denies.

Carrying it on every batch gives the invariant instead: **a batch that does not carry a mapping its
data contains is one whose mapping was already committed**, since `known` only ever gains a mapping
from a commit that included it. The cost is a duplicate row per overlapping batch, on the first
appearance of a mapping only, bounded by how many batches are in flight at once — which the
append-only table and its grouping readers already absorb.

## Keeping it off the hot path

Per-attribute work dominates collector CPU, and the obvious implementation — hash each key, probe
a set, once per attribute per record — is the one already rejected in this exact loop: memoising
encoded keys by `HashMap.computeIfAbsent` cost **11.8% of collector Java CPU**, because protobuf
materialises a fresh `String` per message so `String`'s cached hash never carries over between
records (see the note in `MapColumnWriter`). Nothing in the detection path hashes a key.

Instead (`AttributeTypeScan`): each attribute set keeps the previous row's key list — protobuf's
own `String` references, never copied — and that row's `ValueCase` ordinals. A row whose keys and
kinds repeat the previous row's, in order, is skipped outright, and OTLP producers emit the same
layout record after record. A resource attribute list short-circuits on object identity, because
one `Resource` covers every record of an export. A row that differs is walked once into a
per-batch set. This is sound because every row is either collected itself or identical to the row
before it, which was.

Measured on the prototype, timing `LogRecordBatchWriter.write` over wire-parsed batches of 40
attributes per record and 17 resource attributes, 21 alternating rounds: the scan adds **66
ns/record at the minimum and 185 at the median**, against 2,600–2,900 ns/record for the batch
writer itself.

End-to-end is less conclusive and worth saying plainly. Four 90-second runs at the benchmark's
15.5k rec/s profile, measuring whole-process CPU (the load generator runs in the same process, so
this understates the collector's own share):

| | registry off | registry on |
|---|---|---|
| pair 1 (off first) | 88.0s | 91.5s |
| pair 2 (on first) | 88.7s | 88.3s |

That is +4.0% and −0.5%, averaging **+1.8%** — consistent with the per-call figure above, and
inside the 3% target on average, but not separated from run-to-run noise by these four runs. The
implementation PR should land a tighter measurement (more pairs, or JFR as in the typed-maps work)
rather than treat the average as settled. `-Dbenchmark.attribute_types=true` reproduces the A/B.

## Memory

A process may run thousands of streams, and a set of mappings would cost an entry per
`(queue, signal, scope, name, type)` — hundreds of thousands of map nodes, each with a five-field
key object, and the queue id arriving as a fresh `String` per request. `AttributeTypeCache` holds
the same information as:

- one entry per **stream and signal** (at most three per stream);
- one entry per **attribute name** within it, valued by an `int` bitmask with one bit per
  `(scope, type)` pair — four scopes by eight types is exactly 32 bits;
- one **shared instance** of each name across every stream that records it.

So a name's second scope or type costs a bit rather than an entry, and a name common to a thousand
streams is stored once. `max_entries` caps the mappings held across all queues; past it new ones
are counted (`otel.attribute_types.dropped`) and logged, not recorded, which bounds attribute names
that embed ids.

## Observability

Micrometer meters, tagged with `queue` like the collector's existing ones except where noted:

| Meter | Type | Meaning |
|---|---|---|
| `otel.attribute_types.new` | counter | mappings seen for the first time |
| `otel.attribute_types.persisted` | counter | mappings committed with a batch's files |
| `otel.attribute_types.rollbacks` | counter | mappings released because their batch did not commit; a later batch carries them again |
| `otel.attribute_types.dropped` | counter | mappings not recorded: past `max_entries`, or a name that cannot be written as a SQL literal |
| `otel.attribute_types.in_flight` | gauge | mappings carried on a batch that has not committed yet (process-wide) |
| `otel.attribute_types.known` | gauge | mappings the cache holds (process-wide) |

A non-zero `rollbacks` rate means batches are failing, which the writer metrics already report; a
climbing `dropped` means `max_entries` is too small for the attribute cardinality being sent.

## Open questions, and how they are answered here

1. **Attributes with no value set** — recorded as `value_type = 'empty'` rather than skipped. The
   key is the `AnyValue` case, and a name that only ever arrives unset is exactly what a reader
   cannot otherwise explain.
2. **Metric name as a dimension** — no. It would multiply rows by metric cardinality, and the
   question the registry answers ("which map holds `x`?") is per queue, signal and scope. It can
   be added as a column later without changing what is there.
3. **Event name as a dimension** — no, same reasoning.
4. **Detection fallback** — not needed; the previous-row filter met the target (above).
5. **Where the registry lives** — wherever each stream's config says. Not a fixed layout (above).
6. **Retention** — none. The table grows only with distinct mappings per stream, and `max_entries`
   bounds what any one process will add. Duplicates make it grow with collector restarts, which a
   reader's `GROUP BY` absorbs; if that ever matters, a periodic `DELETE`-and-reinsert of the
   deduplicated set is a compaction job, not a collector concern.

## Deliberately out of scope

- **A generated view.** The registry makes "one correctly typed column per attribute" possible;
  generating it is a follow-up, not part of this.
- **Dynamic queues.** They cannot record, because `DynamicQueueRepository` stores no
  `additional_parameters` — the same limitation watermarks already have. Lifting it means changing
  the registry schema, which belongs with watermarks rather than here.
- **`/stats`.** The cache size and in-flight count are Micrometer gauges; putting them on the
  dashboard can come later.

## What review should settle

1. The atomicity trade: a rejected row fails the batch. Accepted here, with the mitigations above —
   worth confirming that is the right call versus never touching ingestion.
2. Whether `max_entries` should be per stream as well as per process. It is global, so one
   pathological stream can exhaust the budget for the rest.
3. The end-to-end CPU measurement above, which needs tightening before the code lands.

Settled in review: who creates the table (the collector, in the same transaction), and that a batch
carries every uncommitted mapping rather than skipping one another batch has in flight. Both are
written into the decisions above.
