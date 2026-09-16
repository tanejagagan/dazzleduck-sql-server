package io.dazzleduck.sql.compaction;

/**
 * Enough to rebuild a Postgres-backed DuckLake catalog's ATTACH statement with a different
 * {@code idle_in_transaction_session_timeout}. {@code connectionString} is the libpq key=value
 * string with no {@code options=} key (the escalator appends its own) and no {@code postgres:}
 * prefix (the escalator adds that too — see {@link IdleTimeoutEscalator}). {@code attachOptions}
 * is the verbatim clause that follows {@code AS <database>} in the original ATTACH.
 *
 * <p><b>The catalog's own original ATTACH (in the startup script) must itself use the
 * {@code ducklake:postgres:...} DSN form</b>, not the bare {@code ducklake:host=...} form —
 * verified empirically that without the {@code postgres:} sub-scheme, DuckLake silently falls back
 * to a local file catalog (named after the literal connection string) instead of storing metadata
 * in Postgres. A catalog attached the bare way has no real Postgres metadata for escalation to
 * reconnect to, so re-ATTACHing it here would silently produce a disconnected, empty catalog.
 *
 * <p><b>{@code attachOptions} must not include {@code METADATA_PATH ':memory:'}</b> — also
 * verified empirically incompatible with a same-<em>process</em> DETACH/re-ATTACH, which is
 * exactly what escalation does: with it, the re-ATTACH loses visibility into the catalog's own
 * tables; without it (the default local metadata cache), the re-ATTACH correctly re-hydrates from
 * Postgres.
 */
public record PostgresMetadataConfig(String database, String connectionString, String attachOptions) {
}
