-- Startup script for SIEM + marketing demo with named queries.
-- Attaches the pre-built seed databases as read-only schemas.

ATTACH '/data/siem/siem.duckdb'                    AS siem      (READ_ONLY);
ATTACH '/data/marketing/marketing.duckdb'           AS marketing  (READ_ONLY);
ATTACH '/data/named_queries/named_queries.duckdb'   AS app        (READ_ONLY);
