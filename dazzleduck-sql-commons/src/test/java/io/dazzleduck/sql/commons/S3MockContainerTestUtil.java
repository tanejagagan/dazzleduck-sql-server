package io.dazzleduck.sql.commons;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

public class S3MockContainerTestUtil {

    public static final int S3MOCK_HTTP_PORT = 9090;
    public static String TEST_BUCKET_NAME = "test-bucket";

    public static Map<String, String> duckDBSecretForS3Access(GenericContainer<?> s3mock) {
        // S3Mock accepts any credentials, but DuckDB's S3 secret still needs a key id and secret.
        return Map.of("TYPE", "S3",
                "KEY_ID", "test",
                "SECRET", "test",
                "ENDPOINT", s3mock.getHost() + ":" + s3mock.getMappedPort(S3MOCK_HTTP_PORT),
                "USE_SSL", "false",
                "URL_STYLE", "path");
    }

    public static GenericContainer<?> createContainer(String alias, Network network) {
        // MinIO no longer publishes community images, so tests use Adobe S3Mock instead.
        // The test bucket is created at startup, so no S3 client is needed.
        return new GenericContainer<>(DockerImageName.parse("adobe/s3mock:5.2.3"))
                .withNetwork(network)
                .withNetworkAliases(alias)
                .withEnv("COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS", TEST_BUCKET_NAME)
                .withExposedPorts(S3MOCK_HTTP_PORT)
                .waitingFor(Wait.forHttp("/").forPort(S3MOCK_HTTP_PORT));
    }
}
