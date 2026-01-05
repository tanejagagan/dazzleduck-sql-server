package io.dazzleduck.sql.micrometer.config;

import lombok.Getter;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Configuration for MicrometerForwarder.
 * Use the builder pattern for flexible configuration.
 */
@Getter
public final class MicrometerForwarderConfig {

    // Application metadata
    private final String applicationId;
    private final String applicationName;
    private final String applicationHost;

    // HTTP settings
    private final String baseUrl;
    private final String username;
    private final String password;
    private final String targetPath;
    private final Duration httpClientTimeout;

    // Micrometer step interval
    private final Duration stepInterval;

    // Sender settings
    private final long minBatchSize;
    private final long maxBatchSize;
    private final Duration maxSendInterval;
    private final long maxInMemorySize;
    private final long maxOnDiskSize;
    private final int retryCount;
    private final long retryIntervalMillis;
    private final List<String> transformations;
    private final List<String> partitionBy;

    // Feature flags
    private final boolean enabled;

    private MicrometerForwarderConfig(Builder builder) {
        this.applicationId = builder.applicationId;
        this.applicationName = builder.applicationName;
        this.applicationHost = builder.applicationHost;
        this.baseUrl = builder.baseUrl;
        this.username = builder.username;
        this.password = builder.password;
        this.targetPath = builder.targetPath;
        this.httpClientTimeout = builder.httpClientTimeout;
        this.stepInterval = builder.stepInterval;
        this.minBatchSize = builder.minBatchSize;
        this.maxBatchSize = builder.maxBatchSize;
        this.maxSendInterval = builder.maxSendInterval;
        this.maxInMemorySize = builder.maxInMemorySize;
        this.maxOnDiskSize = builder.maxOnDiskSize;
        this.retryCount = builder.retryCount;
        this.retryIntervalMillis = builder.retryIntervalMillis;
        this.transformations = List.copyOf(builder.transformations);
        this.partitionBy = List.copyOf(builder.partitionBy);
        this.enabled = builder.enabled;
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
        private String targetPath = "metrics";
        private Duration httpClientTimeout = Duration.ofSeconds(3);
        private Duration stepInterval = Duration.ofSeconds(10);
        private long minBatchSize = 1024 * 1024; // 1 MB
        private long maxBatchSize = 10 * 1024 * 1024; // 10 MB
        private Duration maxSendInterval = Duration.ofSeconds(2);
        private long maxInMemorySize = 10 * 1024 * 1024; // 10 MB
        private long maxOnDiskSize = 1024 * 1024 * 1024L; // 1 GB
        private int retryCount = 3;
        private long retryIntervalMillis = 1000; // 1 second
        private List<String> transformations = List.of();
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
            this.applicationId = Objects.requireNonNull(applicationId, "applicationId must not be null");
            return this;
        }

        public Builder applicationName(String applicationName) {
            this.applicationName = Objects.requireNonNull(applicationName, "applicationName must not be null");
            return this;
        }

        public Builder applicationHost(String applicationHost) {
            this.applicationHost = Objects.requireNonNull(applicationHost, "applicationHost must not be null");
            return this;
        }

        public Builder baseUrl(String baseUrl) {
            this.baseUrl = Objects.requireNonNull(baseUrl, "baseUrl must not be null");
            return this;
        }

        public Builder username(String username) {
            this.username = Objects.requireNonNull(username, "username must not be null");
            return this;
        }

        public Builder password(String password) {
            this.password = Objects.requireNonNull(password, "password must not be null");
            return this;
        }

        public Builder targetPath(String targetPath) {
            this.targetPath = Objects.requireNonNull(targetPath, "targetPath must not be null");
            return this;
        }

        public Builder httpClientTimeout(Duration httpClientTimeout) {
            this.httpClientTimeout = Objects.requireNonNull(httpClientTimeout, "httpClientTimeout must not be null");
            return this;
        }

        public Builder stepInterval(Duration stepInterval) {
            this.stepInterval = Objects.requireNonNull(stepInterval, "stepInterval must not be null");
            return this;
        }

        public Builder minBatchSize(long minBatchSize) {
            if (minBatchSize <= 0) {
                throw new IllegalArgumentException("minBatchSize must be positive");
            }
            this.minBatchSize = minBatchSize;
            return this;
        }

        public Builder maxBatchSize(long maxBatchSize) {
            if (maxBatchSize <= 0) {
                throw new IllegalArgumentException("maxBatchSize must be positive");
            }
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public Builder maxSendInterval(Duration maxSendInterval) {
            this.maxSendInterval = Objects.requireNonNull(maxSendInterval, "maxSendInterval must not be null");
            return this;
        }

        public Builder maxInMemorySize(long maxInMemorySize) {
            if (maxInMemorySize <= 0) {
                throw new IllegalArgumentException("maxInMemorySize must be positive");
            }
            this.maxInMemorySize = maxInMemorySize;
            return this;
        }

        public Builder maxOnDiskSize(long maxOnDiskSize) {
            if (maxOnDiskSize <= 0) {
                throw new IllegalArgumentException("maxOnDiskSize must be positive");
            }
            this.maxOnDiskSize = maxOnDiskSize;
            return this;
        }

        public Builder retryCount(int retryCount) {
            if (retryCount < 0) {
                throw new IllegalArgumentException("retryCount must be non-negative");
            }
            this.retryCount = retryCount;
            return this;
        }

        public Builder retryIntervalMillis(long retryIntervalMillis) {
            if (retryIntervalMillis < 0) {
                throw new IllegalArgumentException("retryIntervalMillis must be non-negative");
            }
            this.retryIntervalMillis = retryIntervalMillis;
            return this;
        }

        public Builder transformations(List<String> transformations) {
            this.transformations = Objects.requireNonNull(transformations, "transformations must not be null");
            return this;
        }

        public Builder partitionBy(List<String> partitionBy) {
            this.partitionBy = Objects.requireNonNull(partitionBy, "partitionBy must not be null");
            return this;
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public MicrometerForwarderConfig build() {
            Objects.requireNonNull(baseUrl, "baseUrl is required");
            Objects.requireNonNull(username, "username is required");
            Objects.requireNonNull(password, "password is required");
            Objects.requireNonNull(targetPath, "targetPath is required");
            return new MicrometerForwarderConfig(this);
        }
    }
}
