---
name: query-logs
description: Query the DazzleDuck logback demo log table. Use when the user wants to inspect or analyze logs from the running demo server at http://localhost:8081.
argument-hint: "[optional SQL e.g. WHERE level='ERROR' LIMIT 50]"
allowed-tools: Bash
---

Query logs from the logback demo server using DuckDB.

Run the following command, incorporating $ARGUMENTS as an additional SQL clause (WHERE, LIMIT, ORDER BY, etc.) if provided. If no arguments are given, default to `ORDER BY timestamp DESC LIMIT 100`.

```bash
duckdb -c "
INSTALL dazzleduck FROM community;
LOAD dazzleduck;
SET VARIABLE token = (
    SELECT dd_login(
        'http://localhost:8081',
        'admin',
        'admin',
        '{\"database\":\"ollylake\",\"schema\":\"main\",\"table\":\"log\"}'
    )
);
SELECT * FROM dd_read_arrow(
    'http://localhost:8081',
    source_table := 'ollylake.main.log',
    auth_token := getvariable('token')
)
$ARGUMENTS;
"
```

Display the results clearly to the user. If the server is not reachable, remind them to start the demo with `docker-compose up` from the logback-demo directory.
