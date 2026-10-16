# DuckLake Data Inlining Must Be Off — Design Specification

Status: **proposed.** Small and self-contained, but it has one trap worth reviewing before the code
lands: there are four places inlining can be switched on and they are not all visible in the same
view.

## The problem

DuckLake can keep a small write in the catalog database instead of writing a Parquet file. The row
is then real and queryable — reads merge inlined data with the files — but it lives in the catalog,
which may be SQLite, and only reaches storage when `ducklake_flush_inlined_data` runs.

That breaks the premise the whole ingestion path is built on: data reaches object storage as
Parquet, and the catalog holds metadata about it. Specifically:

- **It is on by default.** `ducklake_default_data_inlining_row_limit` defaults to **10**, so any
  write of ten rows or fewer is inlined unless something says otherwise. A one-row insert produced
  no file and one row in the catalog's inlined-data table.
- **Nothing flushes it on a default deployment.** Collector compaction, which is what would call
  `ducklake_flush_inlined_data`, is `enabled = false` by default. So those rows accumulate in the
  catalog indefinitely.
- **It silently defeats a write-then-register design.** The attribute type registry
  (`doc/ATTRIBUTE_TYPE_REGISTRY_SPEC.md`) writes Parquet and registers it precisely so its rows go
  to storage; a watermark row is inserted per batch. With inlining on, one of those is not doing
  what it says.

## The rule

1. **The collector turns inlining off for its own instance** at startup:
   `SET GLOBAL ducklake_default_data_inlining_row_limit = 0`, which applies to every connection of
   the instance.
2. **Startup checks the effective limit for every table the collector writes** — the watermark
   tables and the attribute type registry tables — and **refuses to start** when any is above zero,
   naming the table and the statement that fixes it.

Target tables need no check: the collector only ever registers files for them and never inserts, so
inlining cannot apply to its writes.

Step 1 alone guarantees nothing, which is the point of step 2 — see below.

## Where inlining comes from, and what each source is visible in

Four sources. All of these were measured, because the visibility differs and that is the trap:

| Source | How it is set | Where it is readable |
|---|---|---|
| Instance setting | `SET [GLOBAL] ducklake_default_data_inlining_row_limit = n` (default **10**) | `current_setting(...)`, `duckdb_settings()` |
| **Attach parameter** | `ATTACH 'ducklake:...' AS c (DATA_PATH '...', DATA_INLINING_ROW_LIMIT n)` | **`duckdb_databases().options`** — *not* in `ducklake_options()` |
| Catalog option | `CALL c.set_option('data_inlining_row_limit', n)` | `ducklake_options(c)`, scope `GLOBAL` |
| Table option | `CALL c.set_option('data_inlining_row_limit', n, table_name => 's.t')` | `ducklake_options(c)`, scope `TABLE`, `scope_entry` `s.t` |

**Precedence is most-specific-wins:** table, else catalog, else attach parameter, else instance
setting. Verified at the two boundaries that matter:

- Attach parameter 10 with the instance setting at 0 → **inlined**. So the attach parameter beats
  the setting, and step 1 does not cover it.
- Table option 0 with the attach parameter at 10 → **a Parquet file was written**. So a table-level
  zero does rescue a catalog attached with inlining on.

Two consequences for the check:

- **It must read `duckdb_databases().options`, not just `ducklake_options()`.** An operator who puts
  `DATA_INLINING_ROW_LIMIT` on the `ATTACH` in the startup script — a thoroughly plausible place —
  is invisible to `ducklake_options()` entirely. A check that consulted only that view would pass a
  collector whose rows all go to the catalog. This is the review point.
- **It must resolve per table, not per catalog.** A catalog shared with other tools may legitimately
  want inlining for their tables; rejecting it outright would be wrong when the collector's own
  tables can be exempted with a table-level zero, which demonstrably works even against an attach
  parameter.

Options are resolved per session/attachment, so changing them under a running process does not take
effect — which means the limit the check reads at startup is the limit the writes will use.

## Failure message

Naming the table and quoting the remedy, since the fix is not guessable:

```
Data inlining is enabled for 'my_catalog.main.otel_attribute_types'
(data_inlining_row_limit = 10, from the ATTACH parameter), so its rows would be kept in the
catalog database instead of written as Parquet, and would stay there until
ducklake_flush_inlined_data runs. Turn it off for that table with:
  CALL my_catalog.set_option('data_inlining_row_limit', 0, table_name => 'main.otel_attribute_types');
```

Reporting *which source* set it matters: the remedy differs between a table option and an attach
parameter buried in a startup script.

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

In: `DuckLakeInlining` in commons (disable, resolve the effective limit across all four sources,
render the remedy), the startup check over watermark and registry tables, and a test per source.

Out: anything about existing inlined rows. If a deployment already has them, the fix is
`ducklake_flush_inlined_data` — a compaction concern, and the collector should say so rather than
try to flush on someone's behalf.

## What review should settle

1. **The attach parameter.** Confirm the check must cover it. It is the one source the obvious
   implementation misses, and it is the most likely place for an operator to set it.
2. Whether refusing to start is right, or whether a loud warning plus a metric is enough.
3. Whether the check should extend to target tables for completeness, given the collector never
   inserts into them and so cannot be affected by their setting.
