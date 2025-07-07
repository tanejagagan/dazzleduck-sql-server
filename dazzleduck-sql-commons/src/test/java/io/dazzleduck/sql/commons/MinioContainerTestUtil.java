package io.dazzleduck.sql.commons;

import io.minio.MinioClient;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.Network;

import java.net.URI;
import java.util.Map;

public class MinioContainerTestUtil {

    public static final int MINIO_S3_PORT = 9000;
    public static final int MINIO_MGMT_PORT = 9001;
    public static String TEST_BUCKET_NAME = "test-bucket";

    public static Map<String, String> duckDBSecretForS3Access(MinIOContainer minio) {
        var uri = URI.create(minio.getS3URL());
        return Map.of("TYPE", "S3",
                "KEY_ID", minio.getUserName(),
                "SECRET", minio.getPassword(),
                "ENDPOINT", uri.getHost() + ":" + uri.getPort(),
                "USE_SSL", "false",
                "URL_STYLE", "path");
    }


    public static MinIOContainer createContainer(String alias, Network network) {
        return new MinIOContainer("minio/minio:RELEASE.2023-09-04T19-57-37Z")
                .withNetwork(network)
                .withNetworkAliases(alias)
                .withExposedPorts(MINIO_S3_PORT, MINIO_MGMT_PORT);
    }

    public static MinioClient createClient(MinIOContainer minio) {
        return MinioClient.builder().endpoint(minio.getS3URL())
                .credentials(minio.getUserName(), minio.getPassword()).build();
    }
}
