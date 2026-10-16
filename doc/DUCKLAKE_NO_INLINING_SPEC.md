# DuckLake Data Inlining Must Be Off — Design Specification

Status: **proposed.** Small and self-contained, but it has one trap worth reviewing before the code
lands: there are five places inlining can be switched on and they are not all visible in the same
view.

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
delete plus an insert.

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

The failure is silent and slow otherwise. Nothing errors, writes succeed, queries return correct
results — and the catalog grows without bound while the operator believes data is in object
storage. By the time it is noticed, the catalog holds rows that have to be flushed before anything
can be migrated or compacted. A collector that refuses to start names the problem at the one moment
someone is watching.

The counter-argument is that this makes the collector refuse to start over a configuration that
"works". That is accepted here on the grounds that it works in a way nobody asked for; the fix is
one statement and the message contains it.

## Scope

In: `DuckLakeInlining` in commons (disable, resolve the effective limit across all five sources,
render the remedy), the startup check over the watermark tables, and a test per source.

Also in, as stated rather than discovered: `SET GLOBAL` moves the default for the whole DuckDB
instance, including catalogs the collector does not own. That is acceptable — it only moves a
default that every option above overrides — but it is a process-wide side effect and belongs in the
log line. And an attach value that is not a number must be reported as unknown rather than read with
a cast that yields NULL and silently falls through to the instance setting.

Out: anything about existing inlined rows. If a deployment already has them, the fix is
`ducklake_flush_inlined_data` — a compaction concern, and the collector should say so rather than
try to flush on someone's behalf.

## What review should settle

1. Whether refusing to start is right, or whether a loud warning plus a metric is enough. The case
   for refusing: with a shared catalog this is a scalability fault, not a preference, and it is
   invisible — nothing errors, writes succeed, queries are correct, and the catalog absorbs the
   load until it does not.
2. Whether the watermark's own `INSERT` should become a write-then-register like the registry's,
   now that inlining is ruled out either way. It would avoid relying on this check at all, at the
   cost of the data-path resolution and orphan handling the registry needed. Out of scope here.

Settled in review, with the evidence folded in above: the schema scope as a fifth source; that
registration is immune to inlining, so only tables the collector inserts into need checking; that
the limit covers deletes as well as inserts; and that the file churn from disabling inlining is
compaction's job, whereas data rows in a shared remote catalog are on every insert's critical
path.
