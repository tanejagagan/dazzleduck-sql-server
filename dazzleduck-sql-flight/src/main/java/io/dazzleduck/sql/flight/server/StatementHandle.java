package io.dazzleduck.sql.flight.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.dazzleduck.sql.flight.util.CryptoUtils;
import org.apache.arrow.flight.FlightDescriptor;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A query handle, carried in Flight tickets and prepared-statement handles. When signed, the
 * server trusts its query without re-authorizing it (it may already carry the planning user's
 * row filters), so the signature also covers who it was issued to ({@code principal}) and until
 * when it may be used ({@code expiresAtMillis}, 0 = no expiry): a leaked or logged handle cannot
 * be replayed by another user, nor forever.
 */
public record StatementHandle(String query, long queryId, @Nullable String producerId, long splitSize,
                              @Nullable String queryChecksum, @Nullable String principal, long expiresAtMillis) {

    final private static AtomicLong queryIdCounter = new AtomicLong();

    public static long nextStatementId(){
        return queryIdCounter.incrementAndGet();
    }

    public StatementHandle(String query, long queryId, String producerId, long splitSize){
        this(query, queryId, producerId, splitSize, null, null, 0);
    }


    private static final ObjectMapper objectMapper = new ObjectMapper();

    byte[] serialize() {
        try {
            return objectMapper.writeValueAsBytes(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    // The principal is length-prefixed so no principal/query split of the same bytes can collide.
    private static String signedContent(long queryId, String query, String principal, long expiresAtMillis) {
        String p = principal == null ? "" : principal;
        return queryId + ":" + expiresAtMillis + ":" + p.length() + ":" + p + ":" + query;
    }

    // Private: a matching signature alone is not enough to trust a handle. Callers use validFor,
    // which also checks the principal and the expiry.
    private boolean signatureMismatch(String key) {
        return !CryptoUtils.generateHMACSHA1(key, signedContent(queryId, query, principal, expiresAtMillis))
                .equals(queryChecksum);
    }

    /**
     * Whether a caller may use this signed handle: the signature matches, it was issued to
     * {@code caller}, and (if it expires) it has not expired at {@code nowMillis}.
     */
    public boolean validFor(String key, String caller, long nowMillis) {
        return !signatureMismatch(key)
                && Objects.equals(principal, caller)
                && (expiresAtMillis == 0 || nowMillis < expiresAtMillis);
    }

    /**
     * Signs the handle for {@code principal}; {@code expiresAtMillis} 0 means it does not expire
     * (prepared-statement handles, whose lifetime the server's prepared-statement cache bounds).
     */
    public StatementHandle signed(String key, String principal, long expiresAtMillis) {
        String checksum = CryptoUtils.generateHMACSHA1(key, signedContent(queryId, query, principal, expiresAtMillis));
        return new StatementHandle(this.query, this.queryId, this.producerId(), this.splitSize, checksum,
                principal, expiresAtMillis);
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
