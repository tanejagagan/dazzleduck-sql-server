package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.flight.FlightRecorder;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class ResultSetStreamUtil {

    private static final Logger logger = LoggerFactory.getLogger(ResultSetStreamUtil.class);

    private ResultSetStreamUtil() {
        throw new UnsupportedOperationException("Utility class");
    }
    static void streamResultSet(ExecutorService executorService,
                                ResultSetSupplier supplier,
                                BufferAllocator allocator,
                                final int batchSize,
                                final FlightProducer.ServerStreamListener listener,
                                Runnable finalBlock,
                                FlightRecorder recorder) {
        executorService.submit(() -> {
            BufferAllocator childAllocator = null;
            var error = false;
            try {
                childAllocator = allocator.newChildAllocator("statement-allocator", 0, allocator.getLimit());
                recorder.startStream(false);
                try (DuckDBResultSet resultSet = supplier.get();
                     ArrowReader reader = (ArrowReader) resultSet.arrowExportStream(childAllocator, batchSize)) {
                    listener.start(reader.getVectorSchemaRoot());
                    while (reader.loadNextBatch()) {
                        var size = childAllocator.getAllocatedMemory();
                        recorder.recordGetStream(false, size);
                        listener.putNext();
                    }
                }
            } catch (Throwable throwable) {
                error = true;
                recorder.errorStream(false);
                ErrorHandling.handleThrowable(listener, throwable);
            } finally {
                if (!error) {
                    listener.completed();
                }
                recorder.endStream(false);
                finalBlock.run();
                if (childAllocator != null) {
                    childAllocator.close();
                }
            }
        });
    }

    static <T extends Statement> void streamResultSet(ExecutorService executorService,
                                                      StatementContext<T> statementContext,
                                                      DuckDBFlightSqlProducer.CacheKey key,
                                                      OptionalResultSetSupplier supplier,
                                                      BufferAllocator allocator,
                                                      final int batchSize,
                                                      final FlightProducer.ServerStreamListener listener,
                                                      Runnable finalBlock, FlightRecorder recorder) {

        executorService.submit(() -> {
            BufferAllocator childAllocator = null;
            var error = false;
            try {
                childAllocator = allocator.newChildAllocator("statement-allocator", 0, allocator.getLimit());
                statementContext.start();
                recorder.startStream(statementContext.isPreparedStatementContext());
                recorder.recordStatementStreamStart(key, statementContext);
                supplier.execute();
                if (supplier.hasResultSet()) {
                    try (DuckDBResultSet resultSet = supplier.get();
                         ArrowReader reader = (ArrowReader) resultSet.arrowExportStream(childAllocator, batchSize)) {
                        listener.start(reader.getVectorSchemaRoot());
                        while (reader.loadNextBatch()) {
                            listener.putNext();
                            var size = childAllocator.getAllocatedMemory();
                            statementContext.bytesOut(size);
                            recorder.recordGetStream(statementContext.isPreparedStatementContext(),
                                    size);
                        }
                    }
                } else {
                    listener.start(new VectorSchemaRoot(List.of()));
                }
            } catch (Throwable throwable) {
                error = true;
                recorder.errorStream(statementContext.isPreparedStatementContext());
                recorder.recordStatementStreamError(key, statementContext, throwable);
                ErrorHandling.handleThrowable(listener, throwable);
            } finally {
                try {
                    if (!error) {
                        listener.completed();
                    }
                    statementContext.end();
                    recorder.endStream(statementContext.isPreparedStatementContext());
                    recorder.recordStatementStreamEnd(key, statementContext);
                    finalBlock.run();
                    if (childAllocator != null) {
                        childAllocator.close();
                    }
                } catch (Exception e){
                    logger.atError().setCause(e).log("Error running finally block");
                }
            }
        });
    }

    static void streamResultSet(ExecutorService executorService,
                                ResultSetSupplierFromConnection supplier,
                                FlightProducer.CallContext context, AccessMode accessMode,
                                BufferAllocator allocator,
                                final FlightProducer.ServerStreamListener listener, FlightRecorder recorder) {

        streamResultSet(executorService, supplier, context, accessMode, allocator, listener, () -> {}, recorder);
    }

    static void streamResultSet(ExecutorService executorService,
                                        ResultSetSupplierFromConnection supplier,
                                        FlightProducer.CallContext context,
                                        AccessMode  accessMode,
                                        BufferAllocator allocator,
                                        final FlightProducer.ServerStreamListener listener,
                                        Runnable finalBlock,
                                        FlightRecorder recorder) {
        try {
            DuckDBConnection connection = DuckDBFlightSqlProducer.getConnection(context, accessMode );
            streamResultSet(executorService,
                    () -> supplier.get(connection),
                    allocator,
                    DuckDBFlightSqlProducer.getBatchSize(context),
                    listener,
                    () -> {
                        try {
                            connection.close();
                        } catch (SQLException e) {
                            logger.atError().setCause(e).log("Error closing connection");
                        }
                        finalBlock.run();
                    }, recorder);
        } catch (Throwable t) {
            ErrorHandling.handleThrowable(listener, t);
        }
    }
}
