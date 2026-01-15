package io.dazzleduck.sql.logback;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Configuration for LogForwarder.
 * Use the builder pattern for flexible configuration.
 */
public record LogForwarderConfig(
        // Application metadata
        String applicationId,
        String applicationName,
        String applicationHost,

        // HTTP settings
        String baseUrl,
        String username,
        String password,
        String targetPath,
        Duration httpClientTimeout,

        // Buffer settings
        int maxBufferSize,
        Duration pollInterval,

        // Sender settings
        long minBatchSize,
        long maxBatchSize,
        Duration maxSendInterval,
        long maxInMemorySize,
        long maxOnDiskSize,
        int retryCount,
        long retryIntervalMillis,
        List<String> projections,
        List<String> partitionBy,

        // Feature flags
        boolean enabled
) {
    public LogForwarderConfig {
        Objects.requireNonNull(applicationId, "applicationId must not be null");
        Objects.requireNonNull(applicationName, "applicationName must not be null");
        Objects.requireNonNull(applicationHost, "applicationHost must not be null");
        Objects.requireNonNull(baseUrl, "baseUrl must not be null");
        Objects.requireNonNull(username, "username must not be null");
        Objects.requireNonNull(password, "password must not be null");
        Objects.requireNonNull(targetPath, "targetPath must not be null");
        Objects.requireNonNull(httpClientTimeout, "httpClientTimeout must not be null");
        Objects.requireNonNull(pollInterval, "pollInterval must not be null");
        Objects.requireNonNull(maxSendInterval, "maxSendInterval must not be null");
        projections = List.copyOf(projections);
        partitionBy = List.copyOf(partitionBy);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String applicationId = "default-app";
        private String applicationName = "DefaultApplication";
        private String applicationHost = getDefaultHostname();
        private String baseUrl = "http://localhost:8081";
        private String username = "admin";
        private String password = "admin";
        private String targetPath = "logs";
        private Duration httpClientTimeout = Duration.ofSeconds(3);
        private int maxBufferSize = 10000;
        private Duration pollInterval = Duration.ofSeconds(5);
        private long minBatchSize = 1024 * 1024; // 1 MB
        private long maxBatchSize = 10 * 1024 * 1024; // 10 MB
        private Duration maxSendInterval = Duration.ofSeconds(2);
        private long maxInMemorySize = 10 * 1024 * 1024; // 10 MB
        private long maxOnDiskSize = 1024 * 1024 * 1024L; // 1 GB
        private int retryCount = 3;
        private long retryIntervalMillis = 1000; // 1 second
        private List<String> projections = List.of();
        private List<String> partitionBy = List.of();
        private boolean enabled = true;

        private Builder() {
        }

        private static String getDefaultHostname() {
            try {
                return java.net.InetAddress.getLocalHost().getHostName();
            } catch (java.net.UnknownHostException e) {
                return "localhost";
            }
        }

        public Builder applicationId(String applicationId) {
            this.applicationId = Objects.requireNonNull(applicationId);
            return this;
        }

        public Builder applicationName(String applicationName) {
            this.applicationName = Objects.requireNonNull(applicationName);
            return this;
        }

        public Builder applicationHost(String applicationHost) {
            this.applicationHost = Objects.requireNonNull(applicationHost);
            return this;
        }

        public Builder baseUrl(String baseUrl) {
            this.baseUrl = Objects.requireNonNull(baseUrl);
            return this;
        }

        public Builder username(String username) {
            this.username = Objects.requireNonNull(username);
            return this;
        }

        public Builder password(String password) {
            this.password = Objects.requireNonNull(password);
            return this;
        }

        public Builder targetPath(String targetPath) {
            this.targetPath = Objects.requireNonNull(targetPath);
            return this;
        }

        public Builder httpClientTimeout(Duration httpClientTimeout) {
            this.httpClientTimeout = Objects.requireNonNull(httpClientTimeout);
            return this;
        }

        public Builder maxBufferSize(int maxBufferSize) {
            if (maxBufferSize <= 0) throw new IllegalArgumentException("maxBufferSize must be positive");
            this.maxBufferSize = maxBufferSize;
            return this;
        }

        public Builder pollInterval(Duration pollInterval) {
            this.pollInterval = Objects.requireNonNull(pollInterval);
            return this;
        }

        public Builder minBatchSize(long minBatchSize) {
            if (minBatchSize <= 0) throw new IllegalArgumentException("minBatchSize must be positive");
            this.minBatchSize = minBatchSize;
            return this;
        }

        public Builder maxBatchSize(long maxBatchSize) {
            if (maxBatchSize <= 0) throw new IllegalArgumentException("maxBatchSize must be positive");
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public Builder maxSendInterval(Duration maxSendInterval) {
            this.maxSendInterval = Objects.requireNonNull(maxSendInterval);
            return this;
        }

        public Builder maxInMemorySize(long maxInMemorySize) {
            if (maxInMemorySize <= 0) throw new IllegalArgumentException("maxInMemorySize must be positive");
            this.maxInMemorySize = maxInMemorySize;
            return this;
        }

        public Builder maxOnDiskSize(long maxOnDiskSize) {
            if (maxOnDiskSize <= 0) throw new IllegalArgumentException("maxOnDiskSize must be positive");
            this.maxOnDiskSize = maxOnDiskSize;
            return this;
        }

        public Builder retryCount(int retryCount) {
            if (retryCount < 0) throw new IllegalArgumentException("retryCount must be non-negative");
            this.retryCount = retryCount;
            return this;
        }

        public Builder retryIntervalMillis(long retryIntervalMillis) {
            if (retryIntervalMillis < 0) throw new IllegalArgumentException("retryIntervalMillis must be non-negative");
            this.retryIntervalMillis = retryIntervalMillis;
            return this;
        }

        public Builder projections(List<String> projections) {
            this.projections = Objects.requireNonNull(projections);
            return this;
        }

        public Builder partitionBy(List<String> partitionBy) {
            this.partitionBy = Objects.requireNonNull(partitionBy);
            return this;
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public LogForwarderConfig build() {
            return new LogForwarderConfig(
                    applicationId,
                    applicationName,
                    applicationHost,
                    baseUrl,
                    username,
                    password,
                    targetPath,
                    httpClientTimeout,
                    maxBufferSize,
                    pollInterval,
                    minBatchSize,
                    maxBatchSize,
                    maxSendInterval,
                    maxInMemorySize,
                    maxOnDiskSize,
                    retryCount,
                    retryIntervalMillis,
                    projections,
                    partitionBy,
                    enabled
            );
        }
    }
}
