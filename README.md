# Flight-Sql-Duckdb

## An [Arrow Flight SQL Server](https://arrow.apache.org/docs/format/FlightSql.html) with [DuckDB](https://duckdb.org) back-end execution engines

[<img src="https://img.shields.io/badge/dockerhub-image-green.svg?logo=Docker">](https://hub.docker.com/r/voltrondata/sqlflite)
[<img src="https://img.shields.io/badge/Documentation-dev-yellow.svg?logo=">](https://arrow.apache.org/docs/format/FlightSql.html)
[<img src="https://img.shields.io/badge/Arrow%20JDBC%20Driver-download%20artifact-red?logo=Apache%20Maven">](https://search.maven.org/search?q=a:flight-sql-jdbc-driver)
[<img src="https://img.shields.io/badge/PyPI-Arrow%20ADBC%20Flight%20SQL%20driver-blue?logo=PyPI">](https://pypi.org/project/adbc-driver-flightsql/)
[<img src="https://img.shields.io/badge/PyPI-SQLFlite%20Ibis%20Backend-blue?logo=PyPI">](https://pypi.org/project/ibis-sqlflite/)
[<img src="https://img.shields.io/badge/PyPI-SQLFlite%20SQLAlchemy%20Dialect-blue?logo=PyPI">](https://pypi.org/project/sqlalchemy-sqlflite-adbc-dialect/)
[<img src="https://img.shields.io/badge/API-v1-blue.svg?logo=">](https://github.com)

**DazzleDuck SQL Server** is a high-performance remote DuckDB server that supports both Arrow Flight SQL and RESTful HTTP protocols. It enables multiple users to connect remotely and execute queries through various client libraries.

### Key Features
- **Dual Protocol Support**: Arrow Flight SQL (gRPC) and RESTful HTTP API (versioned with `/v1`)
- **Multiple Client Support**: JDBC, ADBC Python drivers, and CLI tools
- **Arrow-Native**: All data transfers use Apache Arrow format for maximum performance
- **Versioned REST API**: Future-proof HTTP endpoints with `/v1` versioning
- **JWT Authentication**: Secure access control for HTTP endpoints
- **Remote Query Execution**: Run DuckDB queries remotely with distributed execution support

### Client Modules

The project includes JDK 11 compatible client libraries:

| Module | Description |
|--------|-------------|
| `dazzleduck-sql-client` | HTTP client (`HttpArrowProducer`) for Arrow data ingestion |
| `dazzleduck-sql-client-grpc` | gRPC/Flight SQL client (`GrpcArrowProducer`) for Arrow data ingestion |
| `dazzleduck-sql-common` | Shared types and utilities (LoginRequest, LoginResponse, DataType, etc.) |
| `dazzleduck-sql-logger` | SLF4J provider for Arrow-based logging |
| `dazzleduck-sql-logback` | Logback appender for log forwarding |

## Dev Setup
### Requirements
- **Server modules**: JDK 21
- **Client modules** (dazzleduck-sql-client, dazzleduck-sql-client-grpc, dazzleduck-sql-common, dazzleduck-sql-logger, dazzleduck-sql-logback): JDK 11+

## Getting started with Docker
- Build the docker image.
```bash
./mvnw clean package install -DskipTests
./mvnw package -DskipTests jib:dockerBuild -pl dazzleduck-sql-runtime
```
- Start the container with `example/data` mounted to the container
  ```bash
  docker run -ti -p 59307:59307 -p 8081:8081 dazzleduck/dazzleduck:latest --conf warehouse=/data --conf users.0.password="your password"
  ```
  This will print the following on the console:
  ```
  ============================================================
  DazzleDuck SQL Server 0.0.16
  ============================================================
  Warehouse Path: /data
  HTTP Server started successfully
  Listening on: http://0.0.0.0:8081
  Health check: http://0.0.0.0:8081/health
  UI dashboard: http://0.0.0.0:8081/v1/ui
  Flight Server is up: Listening on URI: grpc+tcp://0.0.0.0:59307
  ```

- The server is running in both Arrow Flight SQL (gRPC) and HTTP REST API modes
- HTTP API endpoints are available at `/v1/*` (e.g., `/v1/query`, `/v1/login`, `/v1/ingest`)
- Health check endpoint is available at `/health` (unversioned)

## Getting started in the dev setup 
 ```bash
export MAVEN_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
./mvnw exec:java -pl dazzleduck-sql-runtime -Dexec.mainClass="io.dazzleduck.sql.runtime.Main" -Dexec.args="--conf warehouse=warehouse --conf users.0.password="your password""
```

### Supported functionality
1. Database and schema specified as part of connection url. Passed to server as header database and schema.
2. Fetch size can be specified. It's passed to the server in header fetch_size.
3. Bulk write to parquet file using bulk upload functionality. Idea is to bulk upload and then add those files to metadata.
4. Username and Passwords can be specified in application.conf file.

## HTTP API Endpoints

All HTTP API endpoints use a `/v1` version prefix for backward compatibility and future API evolution.

### Available Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/v1/login` | POST | Authenticate and obtain JWT token |
| `/v1/query` | GET/POST | Execute SQL queries and return results in Arrow format |
| `/v1/plan` | POST | Generate query execution plan with splits |
| `/v1/ingest` | POST | Ingest Arrow data to Parquet files |
| `/v1/cancel` | POST | Cancel a running query |
| `/v1/named-query` | GET | List named queries (paginated) |
| `/v1/named-query/{name}` | GET | Get a named query by name |
| `/v1/named-query` | POST | Execute a named (templated) query |
| `/v1/ui` | GET | Access web UI dashboard |
| `/health` | GET | Health check endpoint (unversioned) |

### API Versioning
All HTTP API endpoints are versioned with a `/v1` prefix to ensure backward compatibility and allow for future API evolution. The health check endpoint remains unversioned as it's a standard operational endpoint.

## Connecting to HTTP server.

The server is running on HTTP mode on port 8081 which can used to query DuckDB with POST and GET methods. This will return data in arrow format.<p>
The return data can itself be queried with duckdb

## Getting start with duckdb.
Learn more about it here [dazzleduck](https://github.com/dazzleduck-web/dazzleduck-sql-duckdb/blob/main/README.md)

## Connecting to the flight server via Flight JDBC
Download the [Apache Arrow Flight SQL JDBC driver](https://search.maven.org/search?q=a:flight-sql-jdbc-driver)

You can then use the JDBC driver to connect from your host computer to the locally running Docker Flight SQL server with this JDBC string (change the password value to match the value specified for the SQLFLITE_PASSWORD environment variable if you changed it from the example above):
```bash
jdbc:arrow-flight-sql://localhost:59307?database=memory&useEncryption=0&user=admin&password=admin
```

For instructions on setting up the JDBC driver in popular Database IDE tool: [DBeaver Community Edition](https://dbeaver.io) - see this [repo](https://github.com/voltrondata/setup-arrow-jdbc-driver-in-dbeaver).

**Note** - if you stop/restart the Flight SQL Docker container, and attempt to connect via JDBC with the same password - you could get error: "Invalid bearer token provided. Detail: Unauthenticated".  This is because the client JDBC driver caches the bearer token signed with the previous instance's secret key.  Just change the password in the new container by changing the "SQLFLITE_PASSWORD" env var setting - and then use that to connect via JDBC.

## Connecting to the flight server via the new [ADBC Python Flight SQL driver](https://pypi.org/project/adbc-driver-flightsql/)

You can now use the new Apache Arrow Python ADBC Flight SQL driver to query the Flight SQL server.  ADBC offers performance advantages over JDBC - because it minimizes serialization/deserialization, and data stays in columnar format at all phases.

You can learn more about ADBC and Flight SQL [here](https://voltrondata.com/resources/simplifying-database-connectivity-with-arrow-flight-sql-and-adbc).

Ensure you have Python 3.9+ installed, then open a terminal, then run:
```bash
# Create a Python virtual environment
python3 -m venv .venv

# Activate the virtual environment
. .venv/bin/activate

# Install the requirements including the new Arrow ADBC Flight SQL driver
pip install --upgrade pip
pip install pandas pyarrow adbc_driver_flightsql

# Start the python interactive shell
python
```

In the Python shell - you can then run:
```python
import os
from adbc_driver_flightsql import dbapi as sqlflite, DatabaseOptions


with sqlflite.connect(uri="grpc+tls://localhost:59307",
                        db_kwargs={"username": os.getenv("SQLFLITE_USERNAME", "admin"),
                                   "password": os.getenv("SQLFLITE_PASSWORD", "admin"),
                                   DatabaseOptions.TLS_SKIP_VERIFY.value: "true"  # Not needed if you use a trusted CA-signed TLS cert
                                   }
                        ) as conn:
   with conn.cursor() as cur:
       cur.execute("select * from generate_series(20)",
                   )
       x = cur.fetch_arrow_table()
       print(x)
```

You should see results:


## Connecting via [Ibis](https://ibis-project.org)
See: https://github.com/ibis-project/ibis-sqlflite

## Connecting via [SQLAlchemy](https://www.sqlalchemy.org)
See: https://github.com/prmoore77/sqlalchemy-sqlflite-adbc-dialect





## Named Queries

Named queries allow pre-defined, parameterized SQL templates to be stored in a DuckDB table and executed by name over HTTP. Templates use [Jinja2](https://jinja.palletsprojects.com) syntax via Jinjava.

### Setup

Enable the named query endpoint by setting `named_query_table` in your configuration:

```hocon
dazzleduck_server {
    named_query_table = "named_queries"
}
```

Create the table in DuckDB:

```sql
CREATE TABLE named_queries (
    name                   VARCHAR PRIMARY KEY,
    template               VARCHAR,
    validators             VARCHAR[],
    description            VARCHAR,
    parameter_descriptions MAP(VARCHAR, VARCHAR)
);
```

Insert a template:

```sql
INSERT INTO named_queries VALUES (
    'top_sales',
    'SELECT * FROM sales WHERE region = ''{{ region }}'' LIMIT {{ limit }}',
    NULL,
    'Returns top sales rows for a given region',
    MAP { 'region': 'Sales region name', 'limit': 'Maximum number of rows' }
);
```

### Executing a Named Query

```bash
curl -X POST http://localhost:8081/v1/named-query \
  -H "Content-Type: application/json" \
  -d '{"name": "top_sales", "parameters": {"region": "WEST", "limit": "10"}}'
```

The response is an Arrow IPC stream, identical to `/v1/query`.

#### Query Modes

An optional `mode` field controls how the SQL is executed:

| Mode | Description |
|------|-------------|
| `EXECUTE` (default) | Run the query and return results |
| `EXPLAIN` | Return the query plan without executing |
| `EXPLAIN_ANALYZE` | Return the query plan with execution statistics |

```bash
curl -X POST http://localhost:8081/v1/named-query \
  -H "Content-Type: application/json" \
  -d '{"name": "top_sales", "parameters": {"region": "WEST", "limit": "10"}, "mode": "EXPLAIN"}'
```

### Listing Named Queries

```bash
# First page
curl http://localhost:8081/v1/named-query?offset=0&limit=20

# Response
{
  "items": [
    {"name": "top_sales", "description": "Returns top sales rows for a given region"}
  ],
  "total": 1,
  "offset": 0,
  "limit": 20
}
```

### Getting a Named Query by Name

```bash
curl http://localhost:8081/v1/named-query/top_sales

# Response
{
  "name": "top_sales",
  "description": "Returns top sales rows for a given region",
  "parameterDescriptions": {"region": "Sales region name", "limit": "Maximum number of rows"},
  "validatorDescriptions": []
}
```

### Parameter Validators

Each named query can reference a list of validator class names. Validators are Java classes that implement `NamedQueryParameterValidator` from `dazzleduck-sql-common`:

```java
public class RegionValidator implements NamedQueryParameterValidator {
    @Override
    public void validate(Map<String, String> parameters) throws ParameterValidationException {
        String region = parameters.get("region");
        if (region == null || region.isBlank()) {
            throw new ParameterValidationException("'region' parameter is required");
        }
    }

    @Override
    public String description() {
        return "Requires a non-blank 'region' parameter";
    }
}
```

All validators run on every request and all failures are collected before returning HTTP 400. Validator instances are cached (up to 500) to avoid repeated reflection overhead.

## Security and Access Modes

DazzleDuck SQL Server supports three access modes that control query permissions and external access capabilities:

### Access Modes

| Mode | Description | Query Types Allowed | External Access |
|-------|-------------|---------------------|-----------------|
| **COMPLETE** | Full access to all SQL operations | All (INSERT, UPDATE, DELETE, CREATE, DROP, etc.) | Enabled by default |
| **READ_ONLY** | Only SELECT queries allowed | SELECT, UNION, CTE, subqueries, joins, aggregates | Controlled by startup script |
| **RESTRICTED** | Only SELECT on one table (requires JWT claims) | SELECT on specific table only | Controlled by startup script |

### Configuring Access Mode

Set the access mode in `application.conf` or via command-line:

```hocon
dazzleduck_server {
    access_mode = COMPLETE  # Options: COMPLETE, READ_ONLY, RESTRICTED
}
```

### External Access Control

External access refers to DuckDB's ability to access external tables and functions:
- `read_parquet`, `read_json`, `read_csv` - External file access
- `httpfs`, `httpsfs` - HTTP/HTTPS file system access
- `s3fs`, `gcsfs` - Cloud storage access

**By access mode:**

- **COMPLETE mode**: All external tables and functions are accessible by default
- **READ_ONLY/RESTRICTED modes**: External access must be explicitly enabled in startup script

```sql
-- Disable external access (recommended for read-only modes)
SET enable_external_access = false;

-- Enable external access (use with caution in read-only modes)
SET enable_external_access = true;
```

This security feature prevents read-only users from accessing arbitrary external data sources while still allowing them to query authorized tables. Configure external access in the startup script:

```hocon
startup_script_provider {
    class = "io.dazzleduck.sql.flight.ConfigBasedStartupScriptProvider"
    content = """
        INSTALL arrow FROM community;
        LOAD arrow;

        -- Disable external access for read-only security
        SET enable_external_access = false;
        """
}
```

### JWT Claims for RESTRICTED Mode

When using `RESTRICTED` access mode, queries must include these JWT claims in headers:

| Claim | Header Name | Description |
|--------|--------------|-------------|
| `database` | `database` | Target database name |
| `schema` | `schema` | Target schema name |
| `table` | `table` | Target table name |

Example:
```bash
curl -H "database: mydb" \
     -H "schema: myschema" \
     -H "table: mytable" \
     -H "Authorization: Bearer <jwt-token>" \
     "http://localhost:8081/v1/query?q=SELECT%20*%20FROM%20mytable"
```

## SSL / TLS Configuration

By default, all HTTP clients in DazzleDuck enforce strict certificate and hostname validation using the JVM's default SSL context.

### Self-Signed Certificates (Dev / Test)

If you are running against a server with a self-signed certificate, set the `DD_TRUST_SELF_SIGNED_CERTS` environment variable before starting the process:

```bash
export DD_TRUST_SELF_SIGNED_CERTS=true
```

When this variable is set (to any non-empty value), all internal HTTP clients — including `HttpArrowProducer`, `RedirectAuthorizer`, `ProxyLoginService`, `HttpCredentialValidator`, `AuthUtils`, `JwtServerInterceptor`, and `MetricsScraper` — will skip certificate validation and hostname verification.

> **Warning:** Never set `DD_TRUST_SELF_SIGNED_CERTS` in production. Use a properly signed certificate instead.

## Enabling Authentication in HTTP Mode.
Authentication is supported with jwt. Client need to invoke login api with username/password this api would return jwt  token. This jwt token can be used for all subsequent invocation
- Run the server with authentication enabled
  `docker run -ti -v "$PWD/example/data":/local-data -p 59307:59307 -p 8081:8081 flight-sql-duckdb --conf useEncryption=false --conf warehouse=/data/warehouse --conf http.authentication=jwt`
- Get the jwt token with login <br>
 ```curl -X POST 'http://localhost:8081/v1/login' -H "Content-Type: application/json" -d '{"username": "admin", "password" : "admin"}'```
- Invoke api with jwt token
```
URL="http://localhost:8081/v1/query?q=select%201"
curl -H "Authorization': 'Bearer <jwt-token>" -s "$URL"
```
- Run the query with jwt token by setting <br>
```
INSTALL arrow FROM community; LOAD arrow;
CREATE SECRET http_auth (
              TYPE http,
              EXTRA_HTTP_HEADERS MAP {
                 'Authorization': 'Bearer <jwt-token>'
                 }
              );
SELECT * FROM read_arrow(concat('http://localhost:8081/v1/query?q=', url_encode('select 1, 2, 3')));
```

## DuckLake Post-Ingestion Provider

After Arrow data is ingested and written as Parquet, DazzleDuck can automatically register those files with a DuckLake catalog table via `ingestion_task_factory_provider` (disabled by default in `reference.conf`).

Set `class` to `io.dazzleduck.sql.commons.ingestion.DuckLakeIngestionTaskFactoryProvider` and configure `ingestion_queue_table_mapping` entries:

| Field | Required | Description |
|-------|----------|-------------|
| `ingestion_queue` | Yes | Queue name sent by the producer via the `ingestion_queue` header |
| `catalog` | Yes | DuckLake catalog owning the target table |
| `schema` | Yes | Schema within the catalog |
| `table` | Yes | Target table name |
| `transformation` | No | SQL `SELECT` referencing placeholder table `__this`; omit to write all columns as-is |
| `additional_parameters` | No | Extra key/value pairs forwarded to the post-ingestion task |

### Transformation

`transformation` is a SQL `SELECT` that runs on each batch before it is persisted. The server wraps it as:

```sql
WITH __this AS (SELECT * FROM read_parquet([...]) ORDER BY ...)
<transformation>
```

Common patterns:

```sql
-- Column subset
SELECT id, ts, msg FROM __this

-- Derived column
SELECT *, upper(level) AS level FROM __this

-- Row filter
SELECT * FROM __this WHERE level != 'DEBUG'

-- Add ingestion timestamp
SELECT id, ts, msg, current_timestamp AS ingested_at FROM __this
```

### Publishing the project
- export GPG_TTY=$(tty)
- ./mvnw -P release-sign-artifacts -DskipTests clean verify
- ./mvnw -P release-sign-artifacts -DskipTests deploy

