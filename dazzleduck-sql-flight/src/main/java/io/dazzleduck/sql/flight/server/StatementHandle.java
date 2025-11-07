package io.dazzleduck.sql.flight.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.dazzleduck.sql.common.util.CryptoUtils;
import org.apache.arrow.flight.FlightDescriptor;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public record StatementHandle(String query, long queryId, @Nullable String producerId, long splitSize,
                              @Nullable String queryChecksum) {

    final private static AtomicLong queryIdCounter = new AtomicLong();

    public static long nextStatementId(){
        return queryIdCounter.incrementAndGet();
    }

    public StatementHandle(String query, long queryId, String producerId, long splitSize){
        this(query, queryId, producerId, splitSize, null);
    }


    private static final ObjectMapper objectMapper = new ObjectMapper();

    byte[] serialize() {
        try {
            return objectMapper.writeValueAsBytes(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean signatureMismatch(String key) {
        return !CryptoUtils.generateHMACSHA1(key, queryId + ":" + query).equals(queryChecksum);
    }

    public StatementHandle signed(String key) {
        String checksum = CryptoUtils.generateHMACSHA1(key, queryId + ":" + query);
        return new StatementHandle(this.query, this.queryId, this.producerId(), this.splitSize, checksum);
    }
    public static StatementHandle deserialize(byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, StatementHandle.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static StatementHandle deserialize(ByteString bytes) {
        return deserialize(bytes.toByteArray());
    }

    public static StatementHandle fromFlightDescriptor(FlightDescriptor flightDescriptor) {
        return deserialize(flightDescriptor.getCommand());
    }

    public static StatementHandle newStatementHandle(String query, String producerId, long splitSize) {
        return new StatementHandle(query, queryIdCounter.incrementAndGet(), producerId, splitSize);
    }

    public static StatementHandle newStatementHandle(long id, String query, String producerId, long splitSize) {
        return new StatementHandle(query, id, producerId, splitSize);
    }
}
