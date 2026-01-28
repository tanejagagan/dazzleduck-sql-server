package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Generates realistic log data in Parquet format for load testing using DuckDB.
 *
 * Log schema:
 * - timestamp: TIMESTAMP (milliseconds)
 * - level: VARCHAR (INFO, WARN, ERROR, DEBUG)
 * - service: VARCHAR (service name)
 * - host: VARCHAR (hostname)
 * - message: VARCHAR (log message)
 * - trace_id: VARCHAR (trace identifier)
 * - duration_ms: BIGINT (request duration)
 * - status_code: INT (HTTP status code)
 */
public class LogDataGenerator {

    /**
     * Generates a Parquet file with the specified number of log records using DuckDB.
     *
     * @param outputPath the path to write the Parquet file
     * @param rowCount number of log records to generate
     * @return the size of the generated file in bytes
     */
    public static long generateParquetFile(Path outputPath, int rowCount) throws Exception {
        // SQL to generate realistic log data with weighted distributions
        String sql = """
            COPY (
                SELECT
                    now() - INTERVAL (random() * 3600) SECOND as timestamp,
                    CASE
                        WHEN random() < 0.70 THEN 'INFO'
                        WHEN random() < 0.85 THEN 'DEBUG'
                        WHEN random() < 0.95 THEN 'WARN'
                        ELSE 'ERROR'
                    END as level,
                    CASE floor(random() * 8)::int
                        WHEN 0 THEN 'api-gateway'
                        WHEN 1 THEN 'user-service'
                        WHEN 2 THEN 'order-service'
                        WHEN 3 THEN 'payment-service'
                        WHEN 4 THEN 'inventory-service'
                        WHEN 5 THEN 'notification-service'
                        WHEN 6 THEN 'auth-service'
                        ELSE 'analytics-service'
                    END as service,
                    'host-' || lpad((floor(random() * 8)::int + 1)::varchar, 3, '0') as host,
                    CASE floor(random() * 10)::int
                        WHEN 0 THEN 'Request processed successfully'
                        WHEN 1 THEN 'Connection established to database'
                        WHEN 2 THEN 'Cache miss for key: user_' || floor(random() * 1000)::int
                        WHEN 3 THEN 'Retry attempt ' || floor(random() * 5)::int || ' for operation'
                        WHEN 4 THEN 'Rate limit exceeded for client'
                        WHEN 5 THEN 'Authentication successful for user'
                        WHEN 6 THEN 'Background job completed: batch_' || floor(random() * 1000)::int
                        WHEN 7 THEN 'Metrics exported to monitoring system'
                        WHEN 8 THEN 'Configuration reloaded from source'
                        ELSE 'Health check passed for service'
                    END as message,
                    printf('%%016x%%016x', floor(random() * 9223372036854775807)::bigint, floor(random() * 9223372036854775807)::bigint) as trace_id,
                    CASE
                        WHEN random() < 0.60 THEN floor(random() * 50)::bigint
                        WHEN random() < 0.85 THEN 50 + floor(random() * 200)::bigint
                        WHEN random() < 0.95 THEN 250 + floor(random() * 750)::bigint
                        ELSE 1000 + floor(random() * 5000)::bigint
                    END as duration_ms,
                    CASE
                        WHEN random() < 0.80 THEN 200
                        WHEN random() < 0.85 THEN 201
                        WHEN random() < 0.88 THEN 204
                        WHEN random() < 0.92 THEN 400
                        WHEN random() < 0.94 THEN 401
                        WHEN random() < 0.96 THEN 404
                        WHEN random() < 0.98 THEN 500
                        ELSE 503
                    END as status_code
                FROM generate_series(1, %d)
            ) TO '%s' (FORMAT PARQUET)
            """.formatted(rowCount, outputPath.toString().replace("'", "''"));

        ConnectionPool.execute(sql);
        return Files.size(outputPath);
    }

    /**
     * Convenience method to generate a file and return the path.
     */
    public static GeneratedFile generate(Path directory, String filename, int rowCount) throws Exception {
        // Ensure filename ends with .parquet
        if (!filename.endsWith(".parquet")) {
            filename = filename.replace(".arrow", ".parquet");
            if (!filename.endsWith(".parquet")) {
                filename = filename + ".parquet";
            }
        }
        Path filePath = directory.resolve(filename);
        long size = generateParquetFile(filePath, rowCount);
        return new GeneratedFile(filePath, size, rowCount);
    }

    public void close() {
        // No-op - no resources to close with DuckDB approach
    }

    public record GeneratedFile(Path path, long sizeBytes, int rowCount) {}
}
