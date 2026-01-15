# DazzleDuck SQL Server - Project Documentation

This document provides comprehensive information about the DazzleDuck SQL Server project for AI assistants and developers.

## Overview

DazzleDuck SQL Server is a high-performance remote DuckDB server that supports both Apache Arrow Flight SQL (gRPC) and RESTful HTTP protocols. It enables multiple users to connect remotely and execute queries through various client libraries.

**Key Features:**
- Dual Protocol Support: Arrow Flight SQL (port 59307) and RESTful HTTP API (port 8081)
- Arrow-Native data transfers for maximum performance
- JWT Authentication for secure access
- Remote DuckDB query execution with distributed execution support
- Delta Lake and Hive partition pruning

## Build & Development

### Requirements
- JDK 21
- Maven (wrapper included: `./mvnw`)

### Build Commands
```bash
# Compile all modules
./mvnw compile

# Clean build (skip tests)
./mvnw clean package install -DskipTests

# Run all tests
./mvnw test

# Run tests for specific module
./mvnw test -pl dazzleduck-sql-http

# Build Docker image
./mvnw package -DskipTests jib:dockerBuild -pl dazzleduck-sql-runtime
```

### Maven JVM Flags
Required for Arrow memory management:
```bash
export MAVEN_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
```

### Running Locally
```bash
./mvnw exec:java -pl dazzleduck-sql-runtime -Dexec.mainClass="io.dazzleduck.sql.runtime.Main" -Dexec.args="--conf warehouse=warehouse"
```

### Docker
```bash
docker run -ti -p 59307:59307 -p 8081:8081 dazzleduck/dazzleduck:latest --conf warehouse=/data
```

## Project Structure

```
dazzleduck-sql-server/
├── dazzleduck-sql-runtime/     # Main entry point, server startup orchestration
├── dazzleduck-sql-flight/      # Arrow Flight SQL server implementation
├── dazzleduck-sql-http/        # HTTP REST API (Helidon-based)
├── dazzleduck-sql-common/      # Shared utilities, type handling, config
├── dazzleduck-sql-commons/     # DuckDB utilities, connection pool, transformations
├── dazzleduck-sql-client/      # Client implementations (HTTP & gRPC)
├── dazzleduck-sql-login/       # JWT authentication service
├── dazzleduck-sql-search/      # Full-text search indexing
├── dazzleduck-sql-micrometer/  # Micrometer metrics forwarding
├── dazzleduck-sql-logger/      # SLF4J Arrow logging provider
├── dazzleduck-sql-logback/     # Logback appender for log forwarding
├── dazzleduck-sql-scrapper/    # Prometheus metrics scraping
├── example/                     # Sample data and configurations
├── ui/                          # Frontend UI (arrow-js-frontend)
└── warehouse/                   # Default data warehouse location
```

## Module Details

### dazzleduck-sql-runtime
**Entry point for the entire system**
- `Main.java` - CLI argument parsing, shutdown hooks
- `Runtime.java` - Server lifecycle, configuration loading
- Starts both HTTP and Flight SQL servers

### dazzleduck-sql-flight
**Arrow Flight SQL server**
- `DuckDBFlightSqlProducer.java` - Core producer implementing FlightSqlProducer
- `FlightSqlProducerFactory.java` - Factory for creating producers
- Statement caching with configurable TTL
- Supports bulk ingestion via queues

**Key Auth Classes:**
- `AdvanceJWTTokenAuthenticator.java` - JWT validation
- `AdvanceBasicCallHeaderAuthenticator.java` - Basic auth
- `ConfBasedCredentialValidator.java` - Config-based validation

**Auditing:**
- `MicroMeterFlightRecorder.java` - Metrics recording
- `StatementTrackingInfo.java` - Query execution tracking

### dazzleduck-sql-http
**RESTful HTTP API (Helidon)**

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/v1/login` | POST | Authenticate, get JWT token |
| `/v1/query` | GET/POST | Execute SQL queries (Arrow format) |
| `/v1/plan` | POST | Generate query execution plan |
| `/v1/ingest` | POST | Ingest Arrow data to Parquet |
| `/v1/cancel` | POST | Cancel running query |
| `/v1/ui` | GET | Web UI dashboard |
| `/health` | GET | Health check |

**Key Classes:**
- `QueryService.java` - SQL query execution
- `PlanningService.java` - Query planning
- `IngestionService.java` - Data ingestion
- `JwtAuthenticationFilter.java` - JWT validation filter

### dazzleduck-sql-commons
**Core utilities and DuckDB abstraction layer**

**Connection & Execution:**
- `ConnectionPool.java` - Singleton DuckDB connection management
  - `ConnectionPool.execute(sql)` - Execute query
  - `ConnectionPool.printResult(sql)` - Print results
  - `ConnectionPool.getReader(sql)` - Get Arrow reader

**Query Transformation:**
- `Transformations.java` - SQL transformation engine
  - Parse SQL to JSON AST
  - Apply fingerprinting transformations
  - CTE, subquery handling
- `Fingerprint.java` - SHA-256 hash of normalized queries
- `ExpressionFactory.java` - Expression building utilities

**Partition Pruning:**
- `DucklakePartitionPruning.java` - DuckLake pruning
- `HivePartitionPruning.java` - Hive pruning
- `PartitionPruning.java` (delta) - Delta Lake pruning
- `SplitPlanner.java` - Query split planning

**Bulk Ingestion:**
- `BulkIngestQueue.java` - Batching by size/time
- `ParquetIngestionQueue.java` - Parquet-specific queue
- `PostIngestionTask.java` - Post-write tasks
- `DuckLakePostIngestionTask.java` - DuckLake integration

**Authorization:**
- `SqlAuthorizer.java` - Main authorization interface
- `JwtClaimBasedAuthorizer.java` - JWT claim-based row-level security
- `AccessMode.java` - COMPLETE vs RESTRICTED modes

### dazzleduck-sql-common
**Shared utilities**
- `ConfigUtils.java` - Configuration helpers
- `Headers.java` - HTTP/Flight header constants
- `Validator.java` - Credential validation interface
- `StartupScriptProvider.java` - DuckDB initialization scripts

### dazzleduck-sql-client
**Client implementations**
- `FlightProducer.java` - Abstract client interface
  - Memory-first storage, spill to disk
  - Batching by size and time
  - Retry logic
- `GrpcFlightProducer.java` - gRPC implementation
- `HttpProducer.java` - HTTP implementation

### dazzleduck-sql-login
**Authentication service**
- `LoginService.java` - HTTP login endpoint (`/v1/login`)
- `ProxyLoginService.java` - Proxy to external auth
- JWT token generation with configurable claims

### dazzleduck-sql-search
**Full-text search indexing**
- `Indexer.java` - Index creation from Parquet/Delta
- `Tokenizer.java` - Text tokenization
- Creates inverted index in Parquet format

### dazzleduck-sql-micrometer
**Metrics forwarding**
- `MicrometerForwarder.java` - Main forwarder
- `ArrowMicroMeterRegistry.java` - Micrometer registry
- Converts metrics to Arrow format

### dazzleduck-sql-logger
**Arrow-based logging**
- `ArrowSLF4JServiceProvider.java` - SLF4J provider
- `LogFileTailReader.java` - Log file tailing
- `JsonToArrowConverter.java` - JSON to Arrow conversion

### dazzleduck-sql-logback
**Logback appender**
- `LogForwardingAppender.java` - Custom Logback appender
- Buffers logs, forwards to remote server
- Sequence numbering for reliability

### dazzleduck-sql-scrapper
**Prometheus scraping**
- `MetricsCollector.java` - Orchestrator
- `MetricsScraper.java` - Prometheus endpoint scraper
- `MetricsForwarder.java` - Sends to remote server

## Configuration

Configuration uses TypeSafe Config (HOCON format). Main configuration in each module's `src/main/resources/application.conf`.

### Key Configuration Options

**Flight SQL (`dazzleduck-sql-flight`):**
```hocon
dazzleduck_server = {
    flight_sql = {
        port = 59307
        host = "0.0.0.0"
        use_encryption = true
    }
    warehouse = ${user.dir}"/warehouse"
    secret_key = "base64-encoded-key"

    ingestion = {
        min_bucket_size = 1048576  # 1MB
        max_delay_ms = 2000        # 2 sec
    }

    users = [{
        username = admin
        password = admin
        groups = [admin, general]
    }]

    jwt_token = {
        expiration = 60m
        claims.generate.headers = [database,catalog,schema,table,filter,path,function]
    }

    access_mode = COMPLETE  # or RESTRICTED
}
```

**HTTP (`dazzleduck-sql-http`):**
```hocon
dazzleduck_server = {
    http.port = 8081
    http.host = "0.0.0.0"
    http.authentication = "none"  # or "jwt"
}
```

**Runtime (`dazzleduck-sql-runtime`):**
```hocon
dazzleduck_server = {
    networking_modes = [flight-sql, http]
}
```

### Command-Line Configuration Override
```bash
--conf key=value
# Examples:
--conf warehouse=/data
--conf http.authentication=jwt
--conf flight_sql.port=59308
```

## Testing

### Test Framework
- JUnit 5 with `junit-jupiter`
- JMock for mocking
- Testcontainers for integration tests (MinIO, etc.)

### Running Tests
```bash
# All tests
./mvnw test

# Specific module
./mvnw test -pl dazzleduck-sql-http

# Specific test class
./mvnw test -pl dazzleduck-sql-http -Dtest=QueryServiceTest

# Skip tests
./mvnw install -DskipTests
```

### Test Patterns
- Integration tests use Testcontainers for isolated environments
- `SharedTestServer` pattern for reusing server instances
- Mock clocks (`MutableClock`) for time-sensitive tests
- Parameterized tests for multiple data types

### Key Test Classes
- `dazzleduck-sql-flight`: `DuckDBFlightJDBCTest`, `FlightSqlProducerFactoryTest`
- `dazzleduck-sql-http`: `QueryServiceTest`, `HttpMetricIntegrationTest`
- `dazzleduck-sql-commons`: Various partition pruning tests

## Technology Stack

| Component | Technology | Version |
|-----------|------------|---------|
| Language | Java | 17 |
| Database | DuckDB | 1.4.3.0 |
| Arrow | Apache Arrow | 18.3.0 |
| HTTP Framework | Helidon | 4.2.3 |
| SQL Parser | Calcite | 1.25.0 |
| Config | TypeSafe Config | 1.4.3 |
| JWT | JJWT | 0.12.6 |
| Logging | SLF4J | 2.0.16 |
| Metrics | Micrometer | (latest) |
| Data Formats | Parquet, Delta Lake, Hive | - |
| Storage | Hadoop Client, S3 | 3.3.4 |

## Architecture Patterns

### 1. Shared Producer Pattern
Single `DuckDBFlightSqlProducer` instance shared between HTTP and Flight SQL servers.

### 2. Provider Pattern
Pluggable components via configuration:
- `StartupScriptProvider` - DuckDB initialization
- `PostIngestionTaskFactory` - Post-write tasks
- `QueryOptimizer` - Query optimization

### 3. Bulk Ingestion Queue
- Time and size-based batching
- Async write queue with statistics
- Post-ingestion task hooks

### 4. Authorization Framework
- JWT claim-based row-level security
- Query AST modification for filter injection
- COMPLETE vs RESTRICTED access modes

### 5. Client Buffering
- Memory-first, spill-to-disk strategy
- Automatic retry with configurable backoff

## API Usage Examples

### HTTP Query
```bash
# Simple query
curl "http://localhost:8081/v1/query?q=select%201"

# With authentication
curl -H "Authorization: Bearer <jwt-token>" "http://localhost:8081/v1/query?q=select%201"
```

### Login
```bash
curl -X POST 'http://localhost:8081/v1/login' \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin"}'
```

### Data Ingestion
```bash
curl -X POST 'http://localhost:8081/v1/ingest?path=file1.parquet' \
  -H "Content-Type: application/vnd.apache.arrow.stream" \
  --data-binary "@file.arrow"
```

### Flight SQL JDBC
```
jdbc:arrow-flight-sql://localhost:59307?database=memory&useEncryption=0&user=admin&password=admin
```

## Common Development Tasks

### Adding a New HTTP Endpoint
1. Create service class in `dazzleduck-sql-http/src/main/java/.../service/`
2. Implement Helidon `HttpService` interface
3. Register in `Main.java` routing configuration

### Adding Configuration
1. Add to appropriate `application.conf`
2. Access via `ConfigUtils` or direct TypeSafe Config API

### Adding Tests
1. Create test class with `*Test.java` suffix
2. Use `@Test` annotation from JUnit 5
3. For integration tests, consider Testcontainers

### Partition Pruning Implementation
1. Implement `PartitionPruning` interface in `dazzleduck-sql-commons`
2. Override `prune()` method with format-specific logic

## Publishing

```bash
# Set GPG for signing
export GPG_TTY=$(tty)

# Verify artifacts
./mvnw -P release-sign-artifacts -DskipTests clean verify

# Deploy to Maven Central
./mvnw -P release-sign-artifacts -DskipTests deploy
```

## Useful Utilities

### ConnectionPool (dazzleduck-sql-commons)
```java
// Execute query
ConnectionPool.execute("SELECT * FROM table");

// Get Arrow reader
ArrowReader reader = ConnectionPool.getReader("SELECT * FROM table");

// Print results
ConnectionPool.printResult("SELECT * FROM table");
```

### Transformations (dazzleduck-sql-commons)
```java
// Parse SQL to AST
String ast = Transformations.parseToTree(sql);

// Get fingerprint
String fingerprint = Fingerprint.fingerprint(sql);
```

### ExpressionFactory (dazzleduck-sql-commons)
```java
// Build WHERE clause
Expression expr = ExpressionFactory.eq("column", "value");
Expression combined = ExpressionFactory.and(expr1, expr2);
```

## Troubleshooting

### Common Issues

1. **Arrow Memory Error**: Ensure JVM flags are set
   ```bash
   --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
   ```

2. **Bearer Token Invalid**: Token cached from previous instance; change password

3. **Port Already in Use**: Check for running instances on 59307 (Flight) or 8081 (HTTP)

4. **DuckDB Extension Not Found**: Ensure startup script installs required extensions
   ```sql
   INSTALL arrow FROM community;
   LOAD arrow;
   ```

## Detailed Utility Classes Reference

### ConfigUtils (`dazzleduck-sql-common`)
Central configuration utility with all configuration keys:

```java
import io.dazzleduck.sql.common.util.ConfigUtils;

// Configuration path
ConfigUtils.CONFIG_PATH  // "dazzleduck_server"

// Key constants
ConfigUtils.WAREHOUSE_CONFIG_KEY      // "warehouse"
ConfigUtils.SECRET_KEY_KEY            // "secret_key"
ConfigUtils.ACCESS_MODE_KEY           // "access_mode"
ConfigUtils.TEMP_WRITE_LOCATION_KEY   // "temp_write_location"

// Flight SQL keys
ConfigUtils.FLIGHT_SQL_PREFIX         // "flight_sql"
ConfigUtils.FLIGHT_SQL_PORT_KEY       // "flight_sql.port"
ConfigUtils.FLIGHT_SQL_HOST_KEY       // "flight_sql.host"
ConfigUtils.FLIGHT_SQL_USE_ENCRYPTION_KEY // "flight_sql.use_encryption"

// HTTP keys
ConfigUtils.HTTP_PREFIX               // "http"
ConfigUtils.ALLOW_ORIGIN_KEY          // "allow-origin"

// JWT keys
ConfigUtils.JWT_TOKEN_PREFIX          // "jwt_token"
ConfigUtils.JWT_TOKEN_EXPIRATION_KEY  // "jwt_token.expiration"
ConfigUtils.JWT_TOKEN_GENERATION_KEY  // "jwt_token.generation"

// Ingestion keys
ConfigUtils.INGESTION_KEY             // "ingestion"
ConfigUtils.MIN_BUCKET_SIZE_KEY       // "min_bucket_size"
ConfigUtils.MAX_DELAY_MS_KEY          // "max_delay_ms"

// Batching keys
ConfigUtils.MIN_BATCH_SIZE_KEY        // "min_batch_size"
ConfigUtils.MAX_BATCH_SIZE_KEY        // "max_batch_size"
ConfigUtils.MAX_SEND_INTERVAL_MS_KEY  // "max_send_interval_ms"

// Load command-line config
ConfigUtils.ConfigWithMainParameters config = ConfigUtils.loadCommandLineConfig(args);
String warehousePath = ConfigUtils.getWarehousePath(config.config());
Path tempDir = ConfigUtils.getTempWriteDir(config.config());
```

### Headers (`dazzleduck-sql-common`)
Standard HTTP/Flight header constants:

```java
import io.dazzleduck.sql.common.Headers;

// Default values
Headers.DEFAULT_ARROW_FETCH_SIZE  // 10000
Headers.DEFAULT_SPLIT_SIZE        // 1GB

// Header names
Headers.HEADER_FETCH_SIZE         // "fetch_size"
Headers.HEADER_DATABASE           // "database"
Headers.HEADER_SCHEMA             // "schema"
Headers.HEADER_TABLE              // "table"
Headers.HEADER_PATH               // "path"
Headers.HEADER_FUNCTION           // "function"
Headers.HEADER_FILTER             // "filter"
Headers.HEADER_SPLIT_SIZE         // "split_size"
Headers.HEADER_DATA_PARTITION     // "partition"
Headers.HEADER_DATA_FORMAT        // "format"
Headers.HEADER_PRODUCER_ID        // "producer_id"
Headers.HEADER_QUERY_ID           // "query_id"
Headers.HEADER_PRODUCER_BATCH_ID  // "producer_batch_id"
Headers.HEADER_SORT_ORDER         // "sort_order"
Headers.HEADER_DATA_PROJECTIONS       // "transformation"
Headers.HEADER_APP_DATA_TRANSFORMATION // "udf_transformation"

// Type extractors for parsing header values
Headers.EXTRACTOR.get(Integer.class)  // String -> Integer
Headers.EXTRACTOR.get(Long.class)     // String -> Long
Headers.EXTRACTOR.get(Boolean.class)  // String -> Boolean
Headers.EXTRACTOR.get(String.class)   // String -> String
```

### ConnectionPool (`dazzleduck-sql-commons`)
Full API for DuckDB connection management:

```java
import io.dazzleduck.sql.commons.ConnectionPool;

// Get a connection
try (DuckDBConnection conn = ConnectionPool.getConnection()) {
    // Use connection
}

// Get connection with startup SQL
try (DuckDBConnection conn = ConnectionPool.getConnection(new String[]{
    "SET schema = 'main'",
    "SET search_path = 'public'"
})) {
    // Use connection
}

// Execute SQL
ConnectionPool.execute("CREATE TABLE test (id INT)");
ConnectionPool.execute(connection, "INSERT INTO test VALUES (1)");

// Execute batch
int[] results = ConnectionPool.executeBatch(new String[]{
    "INSERT INTO test VALUES (1)",
    "INSERT INTO test VALUES (2)"
});

// Execute batch in transaction (with rollback on error)
int[] results = ConnectionPool.executeBatchInTxn(connection, queries);

// Get Arrow reader for streaming results
try (ArrowReader reader = ConnectionPool.getReader(connection, allocator, sql, batchSize)) {
    while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        // Process batch
    }
}

// Collect single value
Long count = ConnectionPool.collectFirst("SELECT count(*) FROM table", Long.class);

// Collect all rows into Records
record Person(String name, int age) {}
Iterable<Person> people = ConnectionPool.collectAll(connection,
    "SELECT name, age FROM people", Person.class);

// Collect with custom extractor
Iterable<String> names = ConnectionPool.collectAll(connection, sql,
    rs -> rs.getString("name"));

// Bulk ingest Arrow data to Parquet with partitioning
ConnectionPool.bulkIngestToFile(reader, allocator, "/path/output",
    List.of("partition_col"), "parquet", "col1 + 1 as derived_col");

// Create temp table with mapped reader
Closeable cleanup = ConnectionPool.createTempTableWithMap(connection, allocator,
    reader, mappingFunction, sourceColumns, targetField, "temp_table");
```

### Transformations (`dazzleduck-sql-commons`)
SQL AST parsing and transformation:

```java
import io.dazzleduck.sql.commons.Transformations;

// Parse SQL to JSON AST
JsonNode ast = Transformations.parseToTree(sql);
JsonNode ast = Transformations.parseToTree(connection, sql);

// SQL templates
Transformations.JSON_SERIALIZE_SQL   // "SELECT cast(json_serialize_sql('%s') as string)"
Transformations.JSON_DESERIALIZE_SQL // "SELECT json_deserialize_sql(cast('%s' as json))"

// Predicates for AST node types
Transformations.IS_CONSTANT          // Check if node is a constant value
Transformations.IS_REFERENCE         // Check if node is a column reference
Transformations.IS_CONJUNCTION_AND   // Check if node is AND conjunction
Transformations.IS_CONJUNCTION_OR    // Check if node is OR conjunction
Transformations.IS_COMPARISON        // Check if node is a comparison
Transformations.IS_CAST              // Check if node is a CAST expression
Transformations.IS_SELECT            // Check if node is SELECT
Transformations.IS_SUBQUERY          // Check if node is subquery
Transformations.IS_COMPARE_IN        // Check if node is IN comparison

// Function predicates
Transformations.isFunction("read_parquet")  // Check specific function
Transformations.isTableFunction("read_parquet")

// Constant replacement for fingerprinting
Transformations.REPLACE_CONSTANT     // Replaces values with placeholders

// Transform AST
JsonNode transformed = Transformations.transform(ast, predicate, transformer);

// Extract table information
Transformations.CatalogSchemaTable tableInfo = ...;
// tableInfo.catalog(), tableInfo.schema(), tableInfo.tableOrPath(), tableInfo.type()
```

### ExpressionFactory (`dazzleduck-sql-commons`)
Build SQL AST expression nodes programmatically:

```java
import io.dazzleduck.sql.commons.ExpressionFactory;

// Column references
JsonNode colRef = ExpressionFactory.reference(new String[]{"column_name"});
JsonNode qualified = ExpressionFactory.reference(new String[]{"schema", "table", "column"});

// Constants
JsonNode strConst = ExpressionFactory.constant("hello");
JsonNode intConst = ExpressionFactory.constant(42);
JsonNode doubleConst = ExpressionFactory.constant(3.14);
JsonNode boolConst = ExpressionFactory.constant(true);
JsonNode nullConst = ExpressionFactory.constant(null);

// Boolean constants
JsonNode trueExpr = ExpressionFactory.boolTrue();
JsonNode falseExpr = ExpressionFactory.boolFalse();

// Comparisons
JsonNode eq = ExpressionFactory.equalExpr(left, right);           // =
JsonNode lte = ExpressionFactory.lessThanOrEqualExpr(left, right);    // <=
JsonNode gte = ExpressionFactory.greaterThanOrEqualExpr(left, right); // >=

// Type casting
JsonNode casted = ExpressionFactory.cast(expr, "INTEGER");
JsonNode casted = ExpressionFactory.cast(expr, "VARCHAR");

// Logical operators
JsonNode andExpr = ExpressionFactory.andFilters(left, right);
JsonNode andMany = ExpressionFactory.andFilters(new JsonNode[]{a, b, c});
JsonNode orExpr = ExpressionFactory.orFilters(left, right);

// IN expression
JsonNode inExpr = ExpressionFactory.inExpr(colRef, List.of("a", "b", "c"));

// Example: Build WHERE age >= 18 AND city = 'NYC'
JsonNode ageRef = ExpressionFactory.reference(new String[]{"age"});
JsonNode eighteen = ExpressionFactory.constant(18);
JsonNode ageCheck = ExpressionFactory.greaterThanOrEqualExpr(ageRef, eighteen);

JsonNode cityRef = ExpressionFactory.reference(new String[]{"city"});
JsonNode nyc = ExpressionFactory.constant("NYC");
JsonNode cityCheck = ExpressionFactory.equalExpr(cityRef, nyc);

JsonNode whereClause = ExpressionFactory.andFilters(ageCheck, cityCheck);
```

### ExpressionConstants (`dazzleduck-sql-commons`)
Constants for SQL AST manipulation:

```java
import io.dazzleduck.sql.commons.ExpressionConstants;

// Node class constants
ExpressionConstants.CONSTANT_CLASS      // "CONSTANT"
ExpressionConstants.COLUMN_REF_CLASS    // "COLUMN_REF"
ExpressionConstants.COMPARISON_CLASS    // "COMPARISON"
ExpressionConstants.CONJUNCTION_CLASS   // "CONJUNCTION"
ExpressionConstants.FUNCTION_CLASS      // "FUNCTION"
ExpressionConstants.CAST_CLASS          // "CAST"
ExpressionConstants.CASE_CLASS          // "CASE"
ExpressionConstants.SUBQUERY_CLASS      // "SUBQUERY"
ExpressionConstants.OPERATOR_CLASS      // "OPERATOR"

// Node type constants
ExpressionConstants.SELECT_NODE_TYPE    // "SELECT_NODE"
ExpressionConstants.BASE_TABLE_TYPE     // "BASE_TABLE"
ExpressionConstants.TABLE_FUNCTION_TYPE // "TABLE_FUNCTION"
ExpressionConstants.SUBQUERY_TYPE       // "SUBQUERY"

// Comparison types
ExpressionConstants.COMPARE_TYPE_EQUAL              // "COMPARE_EQUAL"
ExpressionConstants.COMPARE_TYPE_LESSTHAN           // "COMPARE_LESSTHAN"
ExpressionConstants.COMPARE_TYPE_GREATERTHAN        // "COMPARE_GREATERTHAN"
ExpressionConstants.COMPARE_TYPE_LESSTHANOREQUALTO  // "COMPARE_LESSTHANOREQUALTO"
ExpressionConstants.COMPARE_TYPE_GREATERTHANOREQUALTO // "COMPARE_GREATERTHANOREQUALTO"
ExpressionConstants.COMPARE_IN_TYPE                 // "COMPARE_IN"

// Conjunction types
ExpressionConstants.CONJUNCTION_TYPE_AND  // "CONJUNCTION_AND"
ExpressionConstants.CONJUNCTION_TYPE_OR   // "CONJUNCTION_OR"

// Field names in JSON AST
ExpressionConstants.FIELD_CLASS         // "class"
ExpressionConstants.FIELD_TYPE          // "type"
ExpressionConstants.FIELD_CHILDREN      // "children"
ExpressionConstants.FIELD_CHILD         // "child"
ExpressionConstants.FIELD_COLUMN_NAMES  // "column_names"
ExpressionConstants.FIELD_FUNCTION_NAME // "function_name"
ExpressionConstants.FIELD_VALUE         // "value"
ExpressionConstants.FIELD_WHERE_CLAUSE  // "where_clause"
ExpressionConstants.FIELD_FROM_TABLE    // "from_table"
ExpressionConstants.FIELD_TABLE_NAME    // "table_name"
ExpressionConstants.FIELD_SCHEMA_NAME   // "schema_name"
ExpressionConstants.FIELD_LEFT          // "left"
ExpressionConstants.FIELD_RIGHT         // "right"

// Data type constants
ExpressionConstants.TYPE_VARCHAR   // "VARCHAR"
ExpressionConstants.TYPE_INTEGER   // "INTEGER"
ExpressionConstants.TYPE_BIGINT    // "BIGINT"
ExpressionConstants.TYPE_DOUBLE    // "DOUBLE"
ExpressionConstants.TYPE_FLOAT     // "FLOAT"
ExpressionConstants.TYPE_BOOLEAN   // "BOOLEAN"
ExpressionConstants.TYPE_DECIMAL   // "DECIMAL"
ExpressionConstants.TYPE_STRUCT    // "STRUCT"
ExpressionConstants.TYPE_MAP       // "MAP"
ExpressionConstants.TYPE_LIST      // "LIST"
```

### Fingerprint (`dazzleduck-sql-commons`)
Generate query fingerprints for caching/grouping similar queries:

```java
import io.dazzleduck.sql.commons.Fingerprint;

// Generate SHA-256 fingerprint of normalized query
// Replaces all literal values with placeholders
String fingerprint = Fingerprint.generate(sql);

// These will have the same fingerprint:
// "SELECT * FROM t WHERE id = 1"
// "SELECT * FROM t WHERE id = 2"
```

### DataType (`dazzleduck-sql-commons`)
DuckDB data type definitions:

```java
import io.dazzleduck.sql.commons.types.DataType;

// Primitive types
DataType.BIGINT     // BIGINT
DataType.INTEGER    // INTEGER
DataType.SMALLINT   // SMALLINT
DataType.DOUBLE     // DOUBLE
DataType.FLOAT      // FLOAT
DataType.VARCHAR    // VARCHAR
DataType.BOOLEAN    // BOOLEAN
DataType.DATE       // DATE
DataType.TIMESTAMP  // TIMESTAMP
DataType.TIMESTAMPZ // TIMESTAMP WITH TIME ZONE
DataType.INTERVAL   // INTERVAL
DataType.BLOB       // BLOB
DataType.BIT        // BIT
DataType.NULL       // NULL

// Convert to SQL string
String sql = DataType.INTEGER.toSql();  // "INTEGER"

// Struct type with fields
DataType.Struct struct = new DataType.Struct();
struct.add(new String[]{"name"}, DataType.VARCHAR);
struct.add(new String[]{"age"}, DataType.INTEGER);
```

### SchemaUtils (`dazzleduck-sql-commons`)
Build schema from SQL with type inference:

```java
import io.dazzleduck.sql.commons.util.SchemaUtils;

// Build schema from SQL with explicit casts
// SELECT cast(a as int), cast(b as varchar) FROM t
DataType.Struct schema = SchemaUtils.buildSchema(sql);
```

### BulkIngestQueue (`dazzleduck-sql-commons`)
Batched ingestion with configurable size/time triggers:

```java
import io.dazzleduck.sql.commons.ingestion.*;

// Queue batches data by size or time, then writes
// Subclass ParquetIngestionQueue for Parquet output

// Get statistics
Stats stats = queue.getStats();
// stats.totalWriteBatches(), stats.totalWriteBuckets(), stats.pendingBatches()

// Add batch and get future for completion
CompletableFuture<IngestionResult> future = queue.add(batch);

// Write temp Arrow file
Path tempFile = BulkIngestQueue.writeAndValidateTempArrowFile(tempDir, reader);

// Post-ingestion tasks (e.g., DuckLake registration)
// Configure via post_ingestion_task_factory_provider in application.conf
```

### ParameterUtils (`dazzleduck-sql-http`)
Extract parameters from HTTP requests:

```java
import io.dazzleduck.sql.http.server.ParameterUtils;

// Get parameter from query string or header
Long value = ParameterUtils.getParameterValue("param_name", request, 0L, Long.class);
String str = ParameterUtils.getParameterValue("param_name", request, "default", String.class);
Integer num = ParameterUtils.getParameterValue("param_name", request, 10, Integer.class);
Boolean flag = ParameterUtils.getParameterValue("param_name", request, false, Boolean.class);
```

### ContextUtils (`dazzleduck-sql-flight`)
Extract values from Flight context:

```java
import io.dazzleduck.sql.flight.server.ContextUtils;

// Get header value from Flight context
Long fetchSize = ContextUtils.getValue(context, "fetch_size", 10000L, Long.class);
String database = ContextUtils.getValue(context, "database", "main", String.class);
```

### CryptoUtils (`dazzleduck-sql-common`)
Cryptographic utilities:

```java
import io.dazzleduck.sql.common.util.CryptoUtils;

// Generate HMAC-SHA1 signature
String signature = CryptoUtils.generateHMACSHA1(secretKey, data);
```

### ResultSetStreamUtil (`dazzleduck-sql-flight`)
Stream JDBC results as Arrow:

```java
import io.dazzleduck.sql.flight.server.ResultSetStreamUtil;

// Stream results to Flight listener
ResultSetStreamUtil.streamResultSet(
    executorService,
    resultSetSupplier,
    allocator,
    batchSize,
    listener,
    finalBlock,
    recorder
);
```

### ErrorHandling (`dazzleduck-sql-flight`)
Standardized Flight error handling:

```java
import io.dazzleduck.sql.flight.server.ErrorHandling;

// Handle throwable and convert to Flight error
ErrorHandling.handleThrowable(throwable);
ErrorHandling.handleThrowable(listener, throwable);

// Specific error types
ErrorHandling.throwUnimplemented("methodName");
ErrorHandling.handleContextNotFound();
ErrorHandling.handleSignatureMismatch();
```

## Error Handling Patterns

### Flight SQL Errors
```java
// Use ErrorHandling utility
try {
    // operation
} catch (SQLException e) {
    ErrorHandling.handleSqlException(e);  // Returns INVALID_ARGUMENT
} catch (IOException e) {
    ErrorHandling.handleIOException(listener, e);  // Returns INTERNAL
}

// For streaming responses
ErrorHandling.handleThrowable(listener, throwable);

// Unimplemented methods
ErrorHandling.throwUnimplemented("getFlightInfo");
```

### HTTP Errors
```java
// Throw HTTP exceptions
throw new InternalErrorException("Error message");
throw new HttpException(400, "Bad request");
```

### Common Error Codes
- `INVALID_ARGUMENT` - SQL syntax errors, bad parameters
- `INTERNAL` - Server errors, IO errors
- `NOT_FOUND` - Resource not found
- `UNAUTHORIZED` - Authentication failure
- `UNIMPLEMENTED` - Method not implemented

## Code Conventions

### Package Structure
```
io.dazzleduck.sql.<module>           # Main package
io.dazzleduck.sql.<module>.server    # Server implementations
io.dazzleduck.sql.<module>.auth      # Authentication
io.dazzleduck.sql.<module>.util      # Utilities
```

### Naming Conventions
- Classes: PascalCase (`ConnectionPool`, `FlightSqlProducer`)
- Methods: camelCase (`getConnection`, `parseToTree`)
- Constants: UPPER_SNAKE_CASE (`DEFAULT_FETCH_SIZE`)
- Config keys: snake_case (`flight_sql.port`)

### Test Conventions
- Test class suffix: `*Test.java`
- Use JUnit 5 `@Test` annotation
- Use `Assertions.*` for assertions
- Use `TestUtils.isEqual()` for comparing query results

### Logging
```java
// Use SLF4J fluent API
logger.atInfo().log("Message");
logger.atError().setCause(e).log("Error message");
logger.atDebug().log("Debug: {}", value);
```

### Resource Management
```java
// Always use try-with-resources
try (DuckDBConnection conn = ConnectionPool.getConnection();
     BufferAllocator allocator = new RootAllocator();
     ArrowReader reader = ConnectionPool.getReader(conn, allocator, sql, batchSize)) {
    // Use resources
}
```

## Test Utilities

### TestUtils (`dazzleduck-sql-commons`)
```java
import io.dazzleduck.sql.commons.util.TestUtils;

// Compare query results
TestUtils.isEqual(expectedSql, actualSql);
TestUtils.isEqual(sql, allocator, reader);
```

### MutableClock (`dazzleduck-sql-commons`)
```java
import io.dazzleduck.sql.commons.util.MutableClock;

// Create testable clock
MutableClock clock = new MutableClock(Instant.now());

// Advance time in tests
clock.advance(Duration.ofSeconds(10));
```

### SharedTestServer Pattern
```java
// Reuse server across tests for performance
@BeforeAll
static void setup() {
    server = SharedTestServer.getInstance();
}
```

## Important Files by Module

### dazzleduck-sql-runtime
- `Main.java` - Entry point
- `Runtime.java` - Server lifecycle

### dazzleduck-sql-flight
- `DuckDBFlightSqlProducer.java` - Core producer (large, ~1500 lines)
- `FlightSqlProducerFactory.java` - Producer factory
- `ErrorHandling.java` - Error utilities
- `ResultSetStreamUtil.java` - Streaming utilities

### dazzleduck-sql-http
- `Main.java` - HTTP server setup
- `QueryService.java` - Query endpoint
- `IngestionService.java` - Ingestion endpoint
- `ParameterUtils.java` - Parameter extraction

### dazzleduck-sql-commons
- `ConnectionPool.java` - DuckDB connections
- `Transformations.java` - SQL AST (large, ~600 lines)
- `ExpressionFactory.java` - Expression building
- `BulkIngestQueue.java` - Ingestion batching

### dazzleduck-sql-common
- `ConfigUtils.java` - Configuration
- `Headers.java` - Header constants
- `CryptoUtils.java` - Crypto utilities

## Version

Current version: **0.0.14**

## License

Apache-2.0
