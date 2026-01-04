# DazzleDuck Log Tail → Arrow Pipeline

This project implements a **log ingestion pipeline** that tails JSON log files from disk, converts them into **Apache Arrow** format, and sends them to a **DazzleDuck HTTP server** for ingestion and storage (Parquet).

It supports **real-time log ingestion**, batching, fault tolerance, and full **end-to-end testing** with real servers and real files.

---

## What This Project Does

- Watches a directory for log files (`*.log`)
- Reads new JSON log entries incrementally (tailing)
- Converts log records into Apache Arrow batches
- Sends Arrow data to a DazzleDuck HTTP ingestion endpoint
- Writes ingested data as Parquet in the warehouse
- Supports local testing, unit tests, and real end-to-end runs

---

## Core Components

### Log Processing
- **LogFileTailReader**  
  Detects new log files and tails appended lines safely.

- **LogTailToArrowProcessor**  
  Orchestrates tailing → JSON parsing → Arrow conversion → sending.

- **JsonToArrowConverter**  
  Converts log JSON records into Arrow vectors using a fixed schema.

### Sending & Ingestion
- **HttpSender**  
  Handles authentication (JWT), batching, retries, and backpressure when sending Arrow streams.

### Log Generation (Testing)
- **SimpleLogGenerator**  
  Writes basic static JSON logs (unit tests).

- **LogFileGenerator**  
  Generates realistic rolling log files for end-to-end testing.

---

## Running the Log Processor

The processor can be run as a standalone application.

### Entry Point
```
LogProcessorMain
```

### What it does
- Reads configuration from `application.conf`
- Starts directory monitoring
- Continuously processes logs until shutdown

---

## End-to-End Testing

### EndToEndTest

Runs a **real pipeline**:
1. Starts the real DazzleDuck HTTP server
2. Creates temporary log & warehouse directories
3. Generates real log files on disk
4. Tails logs and sends Arrow data
5. Verifies Parquet ingestion using DuckDB
6. Cleans up resources

This test validates:
- File tailing
- JSON parsing
- Arrow conversion
- HTTP ingestion
- Parquet output correctness

---

## Unit Tests

### LogTailToArrowProcessorTest

Covers:
- Single and multiple log files
- Invalid JSON handling
- Empty files
- Missing files
- Correct Parquet record counts

All tests use temporary directories and clean up automatically.

---

## Log Format

Logs must be **one JSON object per line**, for example:

```json
{
  "timestamp": "2024-01-01T10:00:00Z",
  "level": "INFO",
  "thread": "main",
  "logger": "App",
  "message": "Hello world"
}
```

Invalid JSON lines are safely skipped.

---

## Requirements

- Java 21+
- Apache Arrow
- DuckDB
- SLF4J
- DazzleDuck SQL Server (HTTP mode)

---

## Design Goals

- Streaming-friendly
- Low memory overhead
- Safe file tailing
- Backpressure-aware ingestion
- Production-like testing with real servers

---

## When to Use This

Use this project if you need:
- File-based log ingestion
- Arrow-based transport
- Real-time or near-real-time analytics
- Reliable end-to-end validation

---

## Status

✅ Fully working  
✅ End-to-end verified  
✅ Production-ready pipeline  
