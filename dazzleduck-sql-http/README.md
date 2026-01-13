# DazzleDuck SQL HTTP Module

This module provides HTTP REST API endpoints for the DazzleDuck SQL Server, built on **Helidon WebServer 4.x**.

## Overview

- **Framework**: Helidon WebServer
- **API Version**: v1
- **Base Path**: `/v1` (except health check and UI)
- **Authentication**: JWT token-based (configurable)
- **Data Format**: Apache Arrow IPC streaming format for query results

## API Endpoints

### Health Check

**Endpoint**: `GET /health`

Check server and database health status.

**Response**:
| Status | Description |
|--------|-------------|
| 200 OK | Database is up |
| 503 Service Unavailable | Database is down |

**Response Body**:
```json
{
  "status": "UP|DEGRADED",
  "uptime_seconds": 12345,
  "database": {
    "status": "UP|DOWN",
    "check": "SELECT 1",
    "error": "error message if DOWN"
  },
  "metrics": {
    "bytes_in": 1024.0,
    "bytes_out": 2048.0
  },
  "timestamp": "2024-01-15T10:30:00Z"
}
```

---

### Login

**Endpoint**: `POST /v1/login`

Authenticate users and obtain JWT access tokens.

**Request Body**:
```json
{
  "username": "user@example.com",
  "password": "password123",
  "claims": {
    "custom_claim": "value"
  }
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| username | string | Yes | User identifier |
| password | string | Yes | User password |
| claims | object | No | Custom JWT claims |

**Response**:
| Status | Description |
|--------|-------------|
| 200 OK | Authentication successful |
| 401 Unauthorized | Invalid credentials |

**Response Body**:
```json
{
  "accessToken": "<JWT token>",
  "username": "user@example.com",
  "tokenType": "Bearer"
}
```

---

### Query Execution

**Endpoint**: `GET|POST /v1/query`

Execute SQL queries and return results in Apache Arrow format.

#### GET Request
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| q | string | Yes | URL-encoded SQL query |
| id | long | No | Statement ID (auto-generated if omitted) |

**Example**: `GET /v1/query?q=SELECT%20*%20FROM%20users&id=123`

#### POST Request
```json
{
  "query": "SELECT * FROM users",
  "id": 123
}
```

**Response**:
| Status | Description |
|--------|-------------|
| 200 OK | Query executed successfully |
| 400 Bad Request | Invalid query or parameters |
| 500 Internal Server Error | Execution error |
| 504 Gateway Timeout | Query timeout exceeded |

- **Content-Type**: `application/vnd.apache.arrow.stream`
- **Body**: Binary Apache Arrow IPC stream
- **Timeout**: Default 120 seconds (configurable)

---

### Query Planning

**Endpoint**: `GET|POST /v1/plan`

Get query execution plan information.

#### GET Request
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| q | string | Yes | URL-encoded SQL query |
| id | long | No | Statement ID |

#### POST Request
```json
{
  "query": "SELECT * FROM users",
  "id": 123
}
```

**Response**:
| Status | Description |
|--------|-------------|
| 200 OK | Plan retrieved successfully |
| 500 Internal Server Error | Planning error |

**Response Body**:
```json
[
  {
    "id": "handle_id",
    "query": "SELECT * FROM users",
    "producerId": "producer_id",
    "statementHandle": -1
  }
]
```

---

### Query Cancellation

**Endpoint**: `GET|POST /v1/cancel`

Cancel a running query by statement ID.

#### GET Request
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| q | string | No | SQL query (usually empty) |
| id | long | Yes | Statement ID to cancel |

#### POST Request
```json
{
  "query": "",
  "id": 123
}
```

**Response**:
| Status | Description |
|--------|-------------|
| 200 OK | Query cancelled successfully |
| 400 Bad Request | Missing statement ID |
| 500 Internal Server Error | Cancellation error |

**Response Body**: Plain text message
- `"query cancel successfully."`
- `"failed to cancel query."`

---

### Data Ingestion

**Endpoint**: `POST /v1/ingest`

Bulk ingest data in Arrow format into tables.

**Query Parameters**:
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| path | string | Yes | Target table path (cannot start with "/" or contain "..") |

**Headers**:
| Header | Type | Required | Description |
|--------|------|----------|-------------|
| Content-Type | string | Yes | Must be `application/vnd.apache.arrow.stream` |
| X-Data-Format | string | No | Data format (default: "parquet") |
| X-Data-Partition | string | No | Partition spec (comma-separated, URL-encoded) |
| X-Data-Transformation | string | No | Transformation spec (comma-separated, URL-encoded) |
| X-Producer-Id | string | No | Producer identifier |
| X-Producer-Batch-Id | long | No | Producer batch ID |
| X-Sort-Order | string | No | Sort order spec (comma-separated, URL-encoded) |

**Request Body**: Binary Apache Arrow IPC stream

**Example**:
```http
POST /v1/ingest?path=my_table HTTP/1.1
Content-Type: application/vnd.apache.arrow.stream
X-Data-Format: parquet
X-Data-Partition: year,month

[binary Arrow stream data]
```

**Response**:
| Status | Description |
|--------|-------------|
| 200 OK | Ingestion completed |
| 400 Bad Request | Invalid parameters |
| 415 Unsupported Media Type | Wrong content type |
| 500 Internal Server Error | Ingestion error |

---

### Metrics Dashboard (UI)

**Endpoint**: `GET /v1/ui`

Web-based monitoring dashboard for real-time metrics and query management.

| Route | Description |
|-------|-------------|
| `GET /v1/ui/` | HTML dashboard page |
| `GET /v1/ui/styles.css` | Dashboard CSS styles |
| `GET /v1/ui/script.js` | Dashboard JavaScript |
| `GET /v1/ui/api/metrics` | Metrics data (HTML tables) |

**Dashboard Features**:
- Application metrics (start time, statement counts)
- Network metrics (bytes in/out, arrow batches)
- Running statements with details
- Open prepared statements
- Running bulk ingestion status
- Query cancellation support

---

## Authentication

### JWT Authentication

When JWT authentication is enabled, protected endpoints require a valid Bearer token.

**Protected Endpoints**:
- `/v1/query`
- `/v1/plan`
- `/v1/ingest`
- `/v1/cancel`
- `/v1/ui`

**Unprotected Endpoints**:
- `/health`
- `/v1/login`

**Authorization Header**:
```
Authorization: Bearer <JWT token>
```

### CORS Configuration

- **Default Allow-Origin**: `*` (configurable)
- **Allowed Methods**: GET, POST
- **Allowed Headers**: Content-Type, Authorization

---

## Configuration

| Key | Description | Default |
|-----|-------------|---------|
| `http.host` | Server host | localhost |
| `http.port` | Server port | 8080 |
| `http.authentication` | Auth mode ("none" or "jwt") | none |
| `jwt_token.expiration` | JWT token expiration | - |
| `jwt_token.claims.generate.headers` | Headers to extract as JWT claims | - |
| `jwt_token.claims.validate.headers` | Headers to validate | - |
| `allow-origin` | CORS allow-origin value | * |
| `warehouse_path` | DuckDB warehouse path | - |
| `secret_key` | Base64-encoded JWT secret key | - |

---

## Content Types

| Content-Type | Usage |
|--------------|-------|
| `application/json` | JSON requests/responses |
| `application/vnd.apache.arrow.stream` | Arrow IPC streaming format |
| `text/html` | UI dashboard pages |
| `text/css` | CSS stylesheets |
| `application/javascript` | JavaScript |

---

## Error Handling

### HTTP Status Codes

| Code | Description |
|------|-------------|
| 200 | Success |
| 400 | Bad Request - Validation/parse errors |
| 401 | Unauthorized - Authentication failure |
| 403 | Forbidden - Authorization failure |
| 404 | Not Found |
| 409 | Conflict |
| 415 | Unsupported Media Type |
| 500 | Internal Server Error |
| 501 | Not Implemented |
| 503 | Service Unavailable - Database down |
| 504 | Gateway Timeout - Query timeout |

---

## Quick Start Examples

### Execute a Query (curl)

```bash
# Without authentication
curl -X POST http://localhost:8080/v1/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM users LIMIT 10"}' \
  --output result.arrow

# With JWT authentication
curl -X POST http://localhost:8080/v1/login \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "secret"}'

# Use the returned token
curl -X POST http://localhost:8080/v1/query \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '{"query": "SELECT * FROM users"}' \
  --output result.arrow
```

### Check Health Status

```bash
curl http://localhost:8080/health
```

### Ingest Data

```bash
curl -X POST "http://localhost:8080/v1/ingest?path=my_table" \
  -H "Content-Type: application/vnd.apache.arrow.stream" \
  -H "X-Data-Format: parquet" \
  --data-binary @data.arrow
```

---

## Module Structure

```
dazzleduck-sql-http/
└── src/main/java/io/dazzleduck/sql/http/server/
    ├── Main.java                        # Application entry point
    ├── QueryService.java                # Query execution endpoint
    ├── HealthCheckService.java          # Health check endpoint
    ├── PlanningService.java             # Query planning endpoint
    ├── CancelService.java               # Query cancellation endpoint
    ├── IngestionService.java            # Data ingestion endpoint
    ├── UIService.java                   # Metrics dashboard UI
    ├── AbstractQueryBasedService.java   # Base service for query endpoints
    ├── JwtAuthenticationFilter.java     # JWT authentication filter
    ├── QueryRequest.java                # Query request model
    ├── ContentTypes.java                # Content type constants
    ├── HttpConfig.java                  # HTTP configuration
    └── HttpException.java               # Exception base class
```
