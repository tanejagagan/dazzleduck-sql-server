package io.dazzleduck.sql.flight.server;

import com.google.protobuf.*;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.authorization.SubjectAndVerifiedClaims;
import io.dazzleduck.sql.commons.ingestion.PostIngestionTaskFactoryProvider;
import io.dazzleduck.sql.flight.context.SyntheticFlightContext;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.FlightProducer.*;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertTrue;


public class DuckDBSqlProducerTest {
    protected static final String LOCALHOST = "localhost";
    private static final BufferAllocator serverAllocator = new RootAllocator(Integer.MAX_VALUE);
    private static final String LONG_RUNNING_QUERY = "with t as " +
            "(select len(split(concat('abcdefghijklmnopqrstuvwxyz:', generate_series), ':')) as len  from generate_series(1, 1000000000) )" +
            " select count(*) from t where len = 10";
    private static final Logger log = LoggerFactory.getLogger(DuckDBSqlProducerTest.class);
    protected static String warehousePath;
    private static DuckDBFlightSqlProducer duckDBSqlProducer;
    private static AtomicReference<FlightSql.ActionCreatePreparedStatementResult> resultHolder = new AtomicReference<>();
    private static AtomicReference<Throwable> errorHolder = new AtomicReference<>();
    private static CountDownLatch latch = new CountDownLatch(1);
    private static StreamListener<Result> streamListener = new StreamListener<Result>() {
        @Override
        public void onNext(Result val) {
            try {
                Any any = Any.parseFrom(val.getBody());
                FlightSql.ActionCreatePreparedStatementResult result =
                        any.unpack(FlightSql.ActionCreatePreparedStatementResult.class);
                resultHolder.set(result);
            } catch (Exception e) {
                errorHolder.set(e);
            }
        }

        @Override
        public void onError(Throwable t) {
            errorHolder.set(t);
            latch.countDown();
        }

        @Override
        public void onCompleted() {
            latch.countDown();
        }
    };

    @BeforeAll
    public static void setDuckDBSqlProducer() {
        final Location serverLocation = Location.forGrpcInsecure(LOCALHOST, 55556);
        duckDBSqlProducer = new DuckDBFlightSqlProducer(serverLocation,
                UUID.randomUUID().toString(),
                "change me",
                serverAllocator, warehousePath, AccessMode.COMPLETE,
                DuckDBFlightSqlProducer.newTempDir(),
                PostIngestionTaskFactoryProvider.NO_OP.getPostIngestionTaskFactory(), Duration.ofMinutes(1));
    }

    @Test
    public void testAutoCancelForPreparedStatement() throws InterruptedException {
        FlightSql.ActionCreatePreparedStatementRequest request =
                FlightSql.ActionCreatePreparedStatementRequest.newBuilder()
                        .setQuery(LONG_RUNNING_QUERY)
                        .build();
        var context = new SyntheticFlightContext(
                Map.of("database", List.of(), "schema", List.of()),
                new SubjectAndVerifiedClaims("admin", Map.of()),
                Map.of()
        );

        duckDBSqlProducer.createPreparedStatement(request, context, streamListener);
        // Wait for the async operation to complete (with timeout)
        var completed = latch.await(10, TimeUnit.SECONDS);
        assertTrue(completed, "Timeout waiting for prepared statement to complete");

        FlightSql.ActionCreatePreparedStatementResult preparedResult = resultHolder.get();
        ByteString preparedStatementHandle = preparedResult.getPreparedStatementHandle();
        FlightSql.CommandPreparedStatementQuery command =
                FlightSql.CommandPreparedStatementQuery.newBuilder()
                        .setPreparedStatementHandle(preparedStatementHandle)
                        .build();
        try {
            duckDBSqlProducer.getFlightInfo(context, FlightDescriptor.command(Any.pack(command).toByteArray()));
        } catch (Exception e) {
            log.error("e: ", e);
        }
    }

    @Test
    public void testAutoCancelForStatement() {
        var context = new SyntheticFlightContext(
                Map.of("database", List.of(), "schema", List.of()),
                new SubjectAndVerifiedClaims("admin", Map.of()),
                Map.of()
        );
//        StatementHandle handle = newStatementHandle(newStatementHandle(newStatementHandle(LONG_RUNNING_QUERY)));
//        ByteString serializedHandle = copyFrom(handle.serialize());
//        FlightSql.TicketStatementQuery statement = FlightSql.TicketStatementQuery.newBuilder().setStatementHandle(serializedHandle).build();
//        duckDBSqlProducer.getStreamStatement(statement, context, new ServerStreamListener() {
//            @Override
//            public boolean isCancelled() {
//                return false;
//            }
//
//            @Override
//            public void setOnCancelHandler(Runnable handler) {
//
//            }
//
//            @Override
//            public boolean isReady() {
//                return false;
//            }
//
//            @Override
//            public void start(VectorSchemaRoot root, DictionaryProvider dictionaries, IpcOption option) {
//
//            }
//
//            @Override
//            public void putNext() {
//
//            }
//
//            @Override
//            public void putNext(ArrowBuf metadata) {
//
//            }
//
//            @Override
//            public void putMetadata(ArrowBuf metadata) {
//
//            }
//
//            @Override
//            public void error(Throwable ex) {
//
//            }
//
//            @Override
//            public void completed() {
//
//            }
//        });
    }
}
