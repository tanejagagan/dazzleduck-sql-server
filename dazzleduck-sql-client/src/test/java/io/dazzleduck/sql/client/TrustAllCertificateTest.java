package io.dazzleduck.sql.client;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that HttpArrowProducer connects to an HTTPS server with a self-signed
 * certificate without throwing an SSLHandshakeException.
 */
public class TrustAllCertificateTest {

    @TempDir
    Path tempDir;

    @Test
    void testConnectsToHttpsWithSelfSignedCertificate() throws Exception {
        // 1. Generate a self-signed PKCS12 keystore via keytool
        Path keystoreFile = tempDir.resolve("test-keystore.p12");
        Process keytool = new ProcessBuilder(
                "keytool", "-genkeypair",
                "-keystore", keystoreFile.toString(),
                "-storepass", "changeit",
                "-keypass", "changeit",
                "-alias", "test",
                "-keyalg", "RSA",
                "-keysize", "2048",
                "-validity", "1",
                "-dname", "CN=localhost",
                "-storetype", "PKCS12",
                "-noprompt"
        ).redirectErrorStream(true).start();

        int exitCode = keytool.waitFor();
        assertTrue(exitCode == 0, "keytool failed with exit code " + exitCode
                + ": " + new String(keytool.getInputStream().readAllBytes()));

        // 2. Load the keystore and build an SSLContext for the server
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (InputStream is = Files.newInputStream(keystoreFile)) {
            ks.load(is, "changeit".toCharArray());
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, "changeit".toCharArray());
        SSLContext serverSsl = SSLContext.getInstance("TLS");
        serverSsl.init(kmf.getKeyManagers(), null, null);

        // 3. Start an HttpsServer on a random port
        HttpsServer httpsServer = HttpsServer.create(new InetSocketAddress(0), 0);
        httpsServer.setHttpsConfigurator(new HttpsConfigurator(serverSsl));

        httpsServer.createContext("/v1/login", exchange -> {
            byte[] body = "{\"accessToken\":\"mock-token\",\"tokenType\":\"Bearer\",\"expiresIn\":3600}"
                    .getBytes();
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.getResponseBody().close();
        });

        AtomicBoolean ingestCalled = new AtomicBoolean(false);
        httpsServer.createContext("/v1/ingest", exchange -> {
            exchange.getRequestBody().readAllBytes();
            ingestCalled.set(true);
            exchange.sendResponseHeaders(200, 0);
            exchange.getResponseBody().close();
        });

        httpsServer.start();
        int port = httpsServer.getAddress().getPort();

        try {
            Schema schema = new Schema(List.of(
                    new Field("val", FieldType.nullable(new ArrowType.Utf8()), null)));

            // 4. HttpArrowProducer must connect successfully despite the self-signed cert
            try (HttpArrowProducer producer = new HttpArrowProducer(
                    schema,
                    "https://localhost:" + port,
                    "admin", "admin",
                    "test-queue",
                    Duration.ofSeconds(10),
                    100_000, 200_000,
                    Duration.ofMillis(200),
                    1, 100,
                    List.of(),
                    100_000, 500_000
            )) {
                producer.enqueue(emptyArrowBytes(schema));
            }

            assertTrue(ingestCalled.get(), "Ingest endpoint should have been called over HTTPS");
        } finally {
            httpsServer.stop(0);
        }
    }

    private static byte[] emptyArrowBytes(Schema schema) throws IOException {
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
             ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ArrowStreamWriter writer = new ArrowStreamWriter(root, null, baos)) {
            writer.start();
            writer.end();
            return baos.toByteArray();
        }
    }
}
