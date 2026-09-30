package io.dazzleduck.sql.flight.server;

import com.fasterxml.jackson.core.JsonGenerationException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/** A disconnect and a server-side write failure both end the response, but only one is an error. */
class HttpResponseListenerTest {

    @Test
    void aBrokenConnectionIsTheClientGoingAway() {
        assertTrue(HttpResponseListener.isClientGone(new IOException("Broken pipe")));
        assertTrue(HttpResponseListener.isClientGone(new java.net.SocketException("Connection reset")));
    }

    @Test
    void aSerializationFailureIsAServerError() {
        assertFalse(HttpResponseListener.isClientGone(new JsonGenerationException("bad value", (com.fasterxml.jackson.core.JsonGenerator) null)));
        assertFalse(HttpResponseListener.isClientGone(new IllegalStateException("bug")));
        assertFalse(HttpResponseListener.isClientGone(null));
    }

    @Test
    void theListenerRecordsWhyItsWriteFailed() throws Exception {
        var future = new CompletableFuture<Void>();
        var failing = new OutputStream() {
            @Override public void write(int b) throws IOException { throw new IOException("Broken pipe"); }
        };
        var listener = new TsvOutputStreamListener(() -> failing, future);
        try (var allocator = new org.apache.arrow.memory.RootAllocator();
             var root = org.apache.arrow.vector.VectorSchemaRoot.create(new org.apache.arrow.vector.types.pojo.Schema(
                     java.util.List.of(org.apache.arrow.vector.types.pojo.Field.nullable("x",
                             new org.apache.arrow.vector.types.pojo.ArrowType.Int(32, true)))), allocator)) {
            listener.start(root, null, org.apache.arrow.vector.ipc.message.IpcOption.DEFAULT);
            listener.putNext();
        }
        assertTrue(listener.isCancelled(), "the response is over");
        assertInstanceOf(IOException.class, listener.writeFailure());
        assertTrue(HttpResponseListener.isClientGone(listener.writeFailure()));
    }
}
