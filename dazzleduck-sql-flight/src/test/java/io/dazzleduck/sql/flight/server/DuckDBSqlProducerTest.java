package io.dazzleduck.sql.flight.server;

import com.google.protobuf.*;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.authorization.SubjectAndVerifiedClaims;
import io.dazzleduck.sql.commons.ingestion.PostIngestionTaskFactoryProvider;
import io.dazzleduck.sql.flight.context.SyntheticFlightContext;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.FlightProducer.*;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.IpcOption;
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

import static com.google.protobuf.ByteString.copyFrom;
import static io.dazzleduck.sql.flight.server.StatementHandle.newStatementHandle;
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

        AtomicReference<FlightSql.ActionCreatePreparedStatementResult> resultHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> errorHolder = new AtomicReference<>();

        duckDBSqlProducer.createPreparedStatement(request, context, new StreamListener<Result>() {
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
        });

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
    public void testAutoCancelForStatement() throws InterruptedException {
    }
}
