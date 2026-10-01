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

    @Test
    void helidonsWrappedSocketErrorIsTheClientGoingAway() {
        // Helidon's PlainSocket.write throws UncheckedIOException wrapping the IOException.
        assertTrue(HttpResponseListener.isClientGone(new java.io.UncheckedIOException(new IOException("Broken pipe"))));
        assertFalse(HttpResponseListener.isClientGone(new java.io.UncheckedIOException(
                new JsonGenerationException("bad value", (com.fasterxml.jackson.core.JsonGenerator) null))));
    }

    @Test
    void aListenerRecordsAHelidonStyleDisconnect() throws Exception {
        var future = new CompletableFuture<Void>();
        var helidonLike = new OutputStream() {
            @Override public void write(int b) { throw new java.io.UncheckedIOException(new IOException("Connection reset")); }
        };
        var listener = new TsvOutputStreamListener(() -> helidonLike, future);
        try (var allocator = new org.apache.arrow.memory.RootAllocator();
             var root = org.apache.arrow.vector.VectorSchemaRoot.create(new org.apache.arrow.vector.types.pojo.Schema(
                     java.util.List.of(org.apache.arrow.vector.types.pojo.Field.nullable("x",
                             new org.apache.arrow.vector.types.pojo.ArrowType.Int(32, true)))), allocator)) {
            listener.start(root, null, org.apache.arrow.vector.ipc.message.IpcOption.DEFAULT);
            listener.putNext(); // must not throw out of the listener
        }
        assertTrue(listener.isCancelled());
        assertTrue(HttpResponseListener.isClientGone(listener.writeFailure()), String.valueOf(listener.writeFailure()));
    }

    @Test
    void anHttp2StreamThatHelidonAlreadyClosedIsTheClientGoingAway() {
        // Helidon's Http2ServerStream throws IllegalStateException("Stream is already closed.") when the
        // client reset the stream before the write. (Its CloseConnectionException and Http2Exception
        // cases are covered through the real server in the http module's HttpListenerDisconnectTest.)
        var alreadyClosed = new IllegalStateException("Stream is already closed.");
        alreadyClosed.setStackTrace(new StackTraceElement[]{
                new StackTraceElement("io.helidon.webserver.http2.Http2ServerStream$WriteState", "checkAndMove",
                        "Http2ServerStream.java", 1)});
        assertTrue(HttpResponseListener.isClientGone(alreadyClosed));
        // The same exception type thrown anywhere else is a server error.
        assertFalse(HttpResponseListener.isClientGone(new IllegalStateException("Stream is already closed.")));
    }

    @Test
    void aServerSideRuntimeFailureEndsTheResponseAndStillCountsAsAnError() throws Exception {
        var future = new CompletableFuture<Void>();
        var buggy = new OutputStream() {
            @Override public void write(int b) { throw new IllegalArgumentException("bug"); }
        };
        var listener = new TsvOutputStreamListener(() -> buggy, future);
        try (var allocator = new org.apache.arrow.memory.RootAllocator();
             var root = org.apache.arrow.vector.VectorSchemaRoot.create(new org.apache.arrow.vector.types.pojo.Schema(
                     java.util.List.of(org.apache.arrow.vector.types.pojo.Field.nullable("x",
                             new org.apache.arrow.vector.types.pojo.ArrowType.Int(32, true)))), allocator)) {
            listener.start(root, null, org.apache.arrow.vector.ipc.message.IpcOption.DEFAULT);
            listener.putNext(); // recorded, not thrown: the stream counts it from writeFailure()
        }
        assertTrue(listener.isCancelled());
        assertInstanceOf(IllegalArgumentException.class, listener.writeFailure());
        assertFalse(HttpResponseListener.isClientGone(listener.writeFailure()));
    }
}
