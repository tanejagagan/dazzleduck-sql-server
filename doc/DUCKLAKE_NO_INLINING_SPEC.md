# DuckLake Data Inlining Must Be Off — Design Specification

Status: **accepted.** Both open questions are settled (see the last section): the collector refuses
to start, and the watermark's `INSERT` becomes a write-then-register. One trap survives into the
implementation: there are five places inlining can be switched on and they are not all visible in
the same view.

## The problem

DuckLake can keep a small write in the catalog database instead of writing a Parquet file. The row
is then real and queryable — reads merge inlined data with the files — but it lives in the catalog,
which may be SQLite, and only reaches storage when `ducklake_flush_inlined_data` runs.

That breaks the premise the whole ingestion path is built on: data reaches object storage as
Parquet, and the catalog holds metadata about it.

**The decisive reason is the distributed catalog.** A production DuckLake catalog is Postgres or
MySQL, shared by every writer. Inlined rows are *data* in that store, so with inlining on, every
batch writes data rows into a remote transactional database on the insertion path — write
amplification and contention on the one component every writer shares, growing with ingest rate.
That is a scalability limit, not an untidiness. Inlining is a convenience for a single-process
catalog in a file; it is not an option once the catalog is shared.

Two things compound it:

- **It is on by default.** `ducklake_default_data_inlining_row_limit` defaults to **10**, so any
  write of ten rows or fewer is inlined unless something says otherwise. A one-row insert produced
  no file and one row in the catalog's inlined-data table.
- **Nothing flushes it on a default deployment.** Collector compaction, which is what would call
  `ducklake_flush_inlined_data`, is `enabled = false` by default, so those rows stay in the catalog
  indefinitely.

The counter-argument — that turning inlining off makes every small write its own file — is answered
by compaction: many small files are a known, solved problem (`ducklake_merge_adjacent_files`),
whereas data rows in a shared remote catalog are on the critical path of every insert. The costs
are not comparable.

## The rule

1. **The collector turns inlining off for its own instance** at startup:
   `SET GLOBAL ducklake_default_data_inlining_row_limit = 0`, which applies to every connection of
   the instance.
2. **Startup checks the effective limit for every table the collector writes with SQL** — which is
   the **watermark tables** — and **refuses to start** when any is above zero, naming the table,
   the source that set it, and the statement that fixes it.

**Only tables the collector inserts into need checking.** Inlining applies to `INSERT`, `UPDATE` and
`DELETE` plans, not to registering a file: `ducklake_add_data_files` with a one-row file at the
default limit of 10 registered it as a data file and put nothing in the catalog's inlined-data
tables. So target tables are exempt, and so is the attribute type registry — it writes Parquet and
registers it, which inlining cannot touch. (That also removes the "it defeats a write-then-register
design" motivation an earlier draft of this gave, and the dependency
`doc/ATTRIBUTE_TYPE_REGISTRY_SPEC.md` declared on this spec: there is none.)

**The limit covers deletes, not just inserts.** At the default limit a `DELETE` of a row living in a
Parquet file wrote no delete file — the delete went into a catalog table
(`ducklake_inlined_delete_<n>`); at limit 0 the same delete wrote a delete file. That is what makes
this matter for the watermark's conflict path, where the snapshot-id tightening is an `UPDATE`, so a
delete plus an insert — and it is why the check is still needed after that path is the *only* SQL
write left on a watermark table (see below).

Step 1 alone guarantees nothing, which is the point of step 2 — see below.

## Where inlining comes from, and what each source is visible in

**Five** sources, in resolution order. All measured, because the visibility differs and that is the
trap:

| # | Source | How it is set | Where it is readable |
|---|---|---|---|
| 1 | Table option | `CALL c.set_option('data_inlining_row_limit', n, table_name => 's.t')` | `ducklake_options(c)`, scope `TABLE`, `scope_entry` `s.t` |
| 2 | **Schema option** | `CALL c.set_option('data_inlining_row_limit', n, schema => 's')` | `ducklake_options(c)`, scope `SCHEMA`, `scope_entry` `s` |
| 3 | Catalog option | `CALL c.set_option('data_inlining_row_limit', n)` | `ducklake_options(c)`, scope `GLOBAL` |
| 4 | **Attach parameter** | `ATTACH 'ducklake:...' AS c (DATA_PATH '...', DATA_INLINING_ROW_LIMIT n)` | **`duckdb_databases().options`** — *not* in `ducklake_options()` |
| 5 | Instance setting | `SET [GLOBAL] ducklake_default_data_inlining_row_limit = n` (default **10**) | `current_setting(...)`, `duckdb_settings()` |

**Most specific wins**, matching `TryGetConfigOption` in DuckLake's `ducklake_catalog.cpp`. Verified
at the three boundaries that matter:

- Attach parameter 10 with the instance setting at 0 → **inlined**. So the attach parameter beats
  the setting, and step 1 of the rule does not cover it.
- Table option 0 with the attach parameter at 10 → **a Parquet file was written**. So a table-level
  zero does rescue a catalog attached with inlining on.
- Schema option 0 on `s2` with the attach parameter at 10 → an insert into `s2.t` **wrote a file**
  while `main.t` **inlined**. So the schema scope is real and sits between table and catalog.

A resolver that goes straight from table to catalog is wrong in both directions: a schema-level zero
is missed, which refuses a table that is fine, and a schema-level 10 under a catalog-level zero
passes a table that inlines.

Three consequences for the check:

- **It must read `duckdb_databases().options`, not just `ducklake_options()`.** An operator who puts
  `DATA_INLINING_ROW_LIMIT` on the `ATTACH` in the startup script — a thoroughly plausible place —
  is invisible to `ducklake_options()` entirely. A check that consulted only that view would pass a
  collector whose rows all go to the catalog.
- **It must include the schema scope**, between table and catalog.
- **It must resolve per table, not per catalog.** A catalog shared with other tools may legitimately
  want inlining for their tables; rejecting it outright would be wrong when the collector's own
  tables can be exempted with a table- or schema-level zero, which demonstrably works even against
  an attach parameter.

Options are resolved per session/attachment, so changing them under a running process does not take
effect — which means the limit the check reads at startup is the limit the writes will use.

## What it changes for a deployment that already runs

Not nothing, and this was measured by turning the rule on against the existing suite rather than
reasoned about. Two existing tests changed behaviour, and both are the rule working:

- A partition-pruning fixture went from **6 files to 9**. Its own comment recorded the old
  behaviour — "the 7 inserts collapse into 4 parquet files (grouped by partition) + 2 manual = 6" —
  and that collapse *was* inlining, plus the `ducklake_flush_inlined_data` the fixture calls to undo
  it. With inlining off each insert is its own file. So an operator should expect more, smaller
  files immediately after this lands, until compaction merges them. That is the cost named in the
  motivation, now with a number against it.
- A watermark test's join started matching every row twice, because the watermark table now has
  **data files of its own** in the same snapshot as the data it describes. Nothing was wrong with
  the rows; a query that joined `ducklake_data_file` on `begin_snapshot` without restricting to a
  table was relying on the watermark's rows not being files. Worth knowing for anyone who has
  written such a query against a watermark table.

A third thing the same exercise exposed, which is about the mechanism rather than the rule: the
pruning fixture had no ingestion provider and never asked about inlining, yet its result depended on
whether something else in the process had already called `validate()`. An instance-wide setting
changed by whoever starts first is order-dependent, and a test or a co-tenant catalog can be on the
receiving end. The fixture now states the limit it needs. See the note under Scope.

## The watermark becomes a write-then-register

Decided in review. The watermark rows stop being an `INSERT ... VALUES` inside the ingestion
transaction and become what the attribute type registry already does: the rows are written as a
Parquet file *before* the transaction, and `ducklake_add_data_files` registers that file *inside*
it, alongside the batch's own data files. Atomicity is unchanged — registration and the data files
still commit or roll back together — and the mechanism is the one `SidecarRows` already provides, so
this unifies the two tables rather than adding a second way of doing it.

Four things were measured before accepting it, because two of them constrain the implementation and
one of them retires an argument this spec was making.

**1. The sidecar file's column types must match the table's.** `ducklake_add_data_files` does not
cast. Registering an all-VARCHAR file — which is exactly what `SidecarRows.copySql` produces today,
since it renders values as string literals in a `VALUES` list — into a table with `TIMESTAMP` and
`BIGINT` columns fails:

```
Invalid Input Error: Failed to map column "min_ts" from file "dd_varchar.parquet" to the
column in table "wm"
* Expected one of TIMESTAMP_NS, TIMESTAMP, TIMESTAMP_MS, TIMESTAMP_S, found type VARCHAR
```

The registry got away with it because its columns are all strings. The watermark's are not, so the
sidecar writer needs the target table's column types in its projection. The same typed file
registers and reads back correctly. This is the one real piece of work in the conversion, and it is
a change to `SidecarRows`, which both tables then share.

**2. The tightening `UPDATE` still finds its rows.** It identifies them by `rowid` from
`ducklake_table_insertions` for the committed snapshot, and that was written against rows inserted
by SQL. Rows that arrive as a *registered file* are returned there too — a one-row sidecar
registered in snapshot 2 came back from `ducklake_table_insertions('c','main','wm',2,2)` with
`rowid = 0`, and the `UPDATE` keyed on it changed exactly that row. So the conflict path survives
the conversion unmodified.

**3. The tightening now rewrites the sidecar instead of editing catalog rows.** With inlining off,
the `UPDATE` expired the registered file (`end_snapshot` set) and wrote a replacement Parquet file in
the new snapshot, with nothing inlined. That is correct but it leaves an expired file for snapshot
expiry and cleanup to remove — housekeeping the compactor already does. The cost is bounded by how
often the tightening fires at all, which is only when a competing writer takes the predicted
snapshot id.

**4. It does *not* remove the need for the startup check** — the old open question asserted it
would, and that was wrong. The tightening is still an `UPDATE`, so it is still an insert/delete plan
and still inlinable. At the default limit of 10 the same tightening put **one row in
`ducklake_inlined_data_<n>` and one in `ducklake_inlined_delete_<n>`**, and left the original file
unexpired — so the file on disk said `snapshot_id = 4` while the table read `2`, with the entire
correction living in the catalog database. With inlining off, zero inlined rows. The check therefore
stays, and this measurement is the sharpest argument for it in the whole spec: the inlined case is
not merely untidy, it is a registered Parquet file that disagrees with the table it belongs to.

So the conversion buys a shorter transaction, one write mechanism instead of two, and a watermark
table whose normal path never issues a row-level write — not an escape from the check.

## Failure message

Naming the table and quoting the remedy, since the fix is not guessable:

```
Data inlining is enabled for 'my_catalog.main.ingest_watermark' (10 rows, from the ATTACH
parameter DATA_INLINING_ROW_LIMIT), so its rows would be written into the catalog database
instead of storage, on the insertion path of every batch. Fix the source: remove
DATA_INLINING_ROW_LIMIT from the ATTACH of 'my_catalog'. If that catalog is not yours to
change, exempt this table instead:
  CALL my_catalog.set_option('data_inlining_row_limit', 0, table_name => 'main.ingest_watermark');
```

The source is named because the remedy follows from it, and the source's **own** fix comes first: a
table option is stored in the shared catalog and so applies to every process touching that table,
whereas an attach parameter is one line in this collector's startup script. Advising the stored
option for a local misconfiguration would be a global change to fix a local one.

## Why fail startup rather than warn

**Decided in review: it refuses to start.** The failure is silent and slow otherwise. Nothing errors, writes succeed, queries return correct
results — and the catalog grows without bound while the operator believes data is in object
storage. By the time it is noticed, the catalog holds rows that have to be flushed before anything
can be migrated or compacted. A collector that refuses to start names the problem at the one moment
someone is watching.

The counter-argument is that this makes the collector refuse to start over a configuration that
"works". That is accepted on the grounds that it works in a way nobody asked for; the fix is one
statement and the message contains it. The measurement in point 4 above settles the proportionality
question the alternative rested on: an inlined tightening leaves a registered file whose contents
contradict the table, which is not a condition to warn about and carry on from.

## Scope

In: `DuckLakeInlining` in commons (disable, resolve the effective limit across all five sources,
render the remedy), the startup check over the watermark tables, and a test per source.

Also in, now that review has decided it: converting the watermark rows to a write-then-register.
That is typed-column support in `SidecarRows` (finding 1 above), the watermark's sidecar write ahead
of the transaction with its registration inside it, and a test of the conflict path proving the
tightening still keys on the right `rowid` after the rows arrive as a file. It depends on
`SidecarRows` landing with the registry (`doc/ATTRIBUTE_TYPE_REGISTRY_SPEC.md`) — the dependency runs
that way only, and it is a sequencing one, not a design one.

Also in, as stated rather than discovered: the disable is **instance-wide and cannot be narrower**.
A per-connection `SET` was tried and does not exist in practice — `ConnectionPool` hands out
duplicates of one instance, and setting it on a duplicate moved it for all of them. So the default
moves for every catalog the process has attached, including ones it does not own. Acceptable,
because it only moves a default that every scoped option overrides, but it is a side effect that
belongs in the log line and in this spec rather than in a surprise. And an attach value that is not
a number must be reported as unknown rather than read with a cast that yields NULL and silently
falls through to the instance setting.

Out: anything about existing inlined rows. If a deployment already has them, the fix is
`ducklake_flush_inlined_data` — a compaction concern, and the collector should say so rather than
try to flush on someone's behalf.

## What review settled

Nothing is left open. The two questions this spec carried were decided as follows:

1. **Refusing to start is right**, not a warning plus a metric — see the section above, and
   particularly the inlined tightening that leaves a file disagreeing with its table.
2. **The watermark's `INSERT` becomes a write-then-register**, with the measured constraints and the
   one retired argument in its own section above. It is in scope now rather than deferred.

Settled earlier in review, with the evidence folded in above: the schema scope as a fifth source;
that registration is immune to inlining, so only tables the collector inserts into need checking;
that
the limit covers deletes as well as inserts; and that the file churn from disabling inlining is
compaction's job, whereas data rows in a shared remote catalog are on every insert's critical
path.
