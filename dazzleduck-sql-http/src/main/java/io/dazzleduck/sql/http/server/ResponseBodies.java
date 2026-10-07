package io.dazzleduck.sql.http.server;

import io.helidon.http.http2.Http2Exception;
import io.helidon.webserver.CloseConnectionException;
import io.helidon.webserver.http.ServerResponse;
import io.helidon.webserver.http2.Http2Config;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.function.Supplier;

/**
 * The body of a streamed query response, as handed to the result-stream listeners.
 *
 * <p>The listeners tell a client that went away (not a server error) from a server-side failure by
 * the exception a write throws: an {@link IOException} means the client went away. Helidon doesn't
 * report it that way, and how it does depends on the protocol:
 * <ul>
 *   <li>HTTP/1.1: an {@link UncheckedIOException} wrapping the socket's IOException;</li>
 *   <li>HTTP/2, connection closed: a {@link CloseConnectionException} (e.g. its subclass
 *       {@code ServerConnectionException}, "Failed to write frame data");</li>
 *   <li>HTTP/2, stream reset with the connection still open: the write waits for flow control the
 *       client will never grant, then fails with an {@link Http2Exception} (this also ends a client
 *       that stopped reading for the flow-control timeout); or, if the reset arrived first, an
 *       IllegalStateException from Helidon's HTTP/2 stream ("Stream is already closed.").</li>
 * </ul>
 * This stream reports each of those as an IOException, so the listeners need no knowledge of
 * Helidon. Any other exception passes through unchanged and still counts as a server error.
 *
 * <p>It also asks Helidon for the body stream only when the first byte is written. Once Helidon has
 * handed it out, {@code response.send(...)} is refused ("When output stream is used, response is
 * completed by closing the output stream, do not call send()"), even if nothing was written yet. Opened
 * lazily, a query that fails before its first byte (e.g. while a listener sets up its writer, or
 * formats its first batch) can still be answered with an error status and its real message, which
 * {@link #canSendError} tells. Flushing or closing a body with nothing written does not open it — a
 * listener's {@code error()} closes it — so a service ends a successful response with
 * {@link #finish}, which sends it empty if nothing was written.
 */
final class ResponseBodies {

    private ResponseBodies() {
    }

    /** The response's body stream, opened on the first byte, reporting a gone client as an IOException. */
    static OutputStream of(ServerResponse response) {
        return new ClientGoneAsIOException(() -> {
            if (response.isSent()) {
                // The service already answered (e.g. 504 on a timeout) while the query kept going:
                // nobody is listening for this body any more, the same as a client that went away.
                throw new UncheckedIOException(new IOException("The response was already sent"));
            }
            return response.outputStream();
        });
    }

    /**
     * Whether an error status and message can still be sent: nothing was written to {@code body}
     * (from {@link #of}) and the response was not sent otherwise.
     */
    static boolean canSendError(ServerResponse response, OutputStream body) {
        return !response.isSent() && !(body instanceof ClientGoneAsIOException b && b.opened());
    }

    /**
     * Ends a successful response. A body that had bytes written was completed by its own close; one
     * with nothing written (e.g. an empty result) is sent now, empty.
     */
    static void finish(OutputStream body) throws IOException {
        if (body instanceof ClientGoneAsIOException b && !b.opened()) {
            b.openAndClose();
        }
    }

    /** Whether Helidon threw {@code failure} because the client went away. */
    static boolean isClientGone(RuntimeException failure) {
        if (failure instanceof UncheckedIOException
                || failure instanceof CloseConnectionException
                // Any error code: one raised while writing a response is about that client's
                // connection or stream, not about the query.
                || failure instanceof Http2Exception) {
            return true;
        }
        StackTraceElement[] thrownFrom = failure.getStackTrace();
        return failure instanceof IllegalStateException
                && thrownFrom.length > 0
                && thrownFrom[0].getClassName().startsWith(Http2Config.class.getPackageName() + ".Http2ServerStream");
    }

    static final class ClientGoneAsIOException extends OutputStream {
        private final Supplier<OutputStream> open;
        private OutputStream body;

        ClientGoneAsIOException(OutputStream body) {
            this(() -> body);
        }

        /** {@code open} is called once, on the first write (or by {@link ResponseBodies#finish}). */
        ClientGoneAsIOException(Supplier<OutputStream> open) {
            this.open = open;
        }

        private OutputStream body() {
            if (body == null) {
                body = open.get();
            }
            return body;
        }

        boolean opened() {
            return body != null;
        }

        void openAndClose() throws IOException {
            try {
                body().close();
            } catch (RuntimeException e) {
                throw translate(e);
            }
        }

        @Override
        public void write(int b) throws IOException {
            try {
                body().write(b);
            } catch (RuntimeException e) {
                throw translate(e);
            }
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            try {
                body().write(b, off, len);
            } catch (RuntimeException e) {
                throw translate(e);
            }
        }

        @Override
        public void flush() throws IOException {
            if (body == null) {
                return; // nothing written, nothing to flush
            }
            try {
                body.flush();
            } catch (RuntimeException e) {
                throw translate(e);
            }
        }

        @Override
        public void close() throws IOException {
            if (body == null) {
                return; // nothing written: left unopened, so an error can still be sent (see finish)
            }
            try {
                body.close();
            } catch (RuntimeException e) {
                throw translate(e);
            }
        }

        private static RuntimeException translate(RuntimeException e) throws IOException {
            if (!isClientGone(e)) {
                return e;
            }
            if (e instanceof UncheckedIOException unchecked) {
                throw unchecked.getCause();
            }
            throw new IOException("Client went away", e);
        }
    }
}
