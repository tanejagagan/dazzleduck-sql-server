package io.dazzleduck.sql.otel.collector.config;

import io.dazzleduck.sql.commons.ingestion.IngestionTaskFactory;
import io.dazzleduck.sql.commons.ingestion.NOOPIngestionTaskFactoryProvider;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CollectorProperties {

    private int grpcPort = 4317;
    private String logsOutputPath = "./otel-logs";
    private String tracesOutputPath = "./otel-traces";
    private String metricsOutputPath = "./otel-metrics";
    private long minBucketSizeBytes = 1_048_576; // 1 MB
    private long maxDelayMs = 5000;
    private IngestionTaskFactory logIngestionTaskFactory =
            new NOOPIngestionTaskFactoryProvider(logsOutputPath).getIngestionTaskFactory();
    private IngestionTaskFactory traceIngestionTaskFactory =
            new NOOPIngestionTaskFactoryProvider(tracesOutputPath).getIngestionTaskFactory();
    private IngestionTaskFactory metricIngestionTaskFactory =
            new NOOPIngestionTaskFactoryProvider(metricsOutputPath).getIngestionTaskFactory();
    private String startupScript = "INSTALL arrow FROM community; LOAD arrow;";
    private List<String> partitionBy = List.of();
    private String transformations = null;
    private String serviceName = "open-telemetry-collector";
    private String authentication = "jwt";
    private String secretKey = null;
    private String loginUrl = null;
    private Map<String, String> users = new HashMap<>();
    private Duration jwtExpiration = Duration.ofHours(1);
    private MeterRegistry meterRegistry = new SimpleMeterRegistry();

    public int getGrpcPort() {
        return grpcPort;
    }

    public void setGrpcPort(int grpcPort) {
        this.grpcPort = grpcPort;
    }

    public String getLogsOutputPath() {
        return logsOutputPath;
    }

    public void setLogsOutputPath(String logsOutputPath) {
        this.logsOutputPath = logsOutputPath;
    }

    public String getTracesOutputPath() {
        return tracesOutputPath;
    }

    public void setTracesOutputPath(String tracesOutputPath) {
        this.tracesOutputPath = tracesOutputPath;
    }

    public String getMetricsOutputPath() {
        return metricsOutputPath;
    }

    public void setMetricsOutputPath(String metricsOutputPath) {
        this.metricsOutputPath = metricsOutputPath;
    }

    public IngestionTaskFactory getLogIngestionTaskFactory() {
        return logIngestionTaskFactory;
    }

    public void setLogIngestionTaskFactory(IngestionTaskFactory logIngestionTaskFactory) {
        this.logIngestionTaskFactory = logIngestionTaskFactory;
    }

    public IngestionTaskFactory getTraceIngestionTaskFactory() {
        return traceIngestionTaskFactory;
    }

    public void setTraceIngestionTaskFactory(IngestionTaskFactory traceIngestionTaskFactory) {
        this.traceIngestionTaskFactory = traceIngestionTaskFactory;
    }

    public IngestionTaskFactory getMetricIngestionTaskFactory() {
        return metricIngestionTaskFactory;
    }

    public void setMetricIngestionTaskFactory(IngestionTaskFactory metricIngestionTaskFactory) {
        this.metricIngestionTaskFactory = metricIngestionTaskFactory;
    }

    public long getMinBucketSizeBytes() {
        return minBucketSizeBytes;
    }

    public void setMinBucketSizeBytes(long minBucketSizeBytes) {
        this.minBucketSizeBytes = minBucketSizeBytes;
    }

    public long getMaxDelayMs() {
        return maxDelayMs;
    }

    public void setMaxDelayMs(long maxDelayMs) {
        this.maxDelayMs = maxDelayMs;
    }

    public List<String> getPartitionBy() {
        return partitionBy;
    }

    public void setPartitionBy(List<String> partitionBy) {
        this.partitionBy = partitionBy;
    }

    public String getTransformations() {
        return transformations;
    }

    public void setTransformations(String transformations) {
        this.transformations = transformations;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getAuthentication() {
        return authentication;
    }

    public void setAuthentication(String authentication) {
        this.authentication = authentication;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getLoginUrl() {
        return loginUrl;
    }

    public void setLoginUrl(String loginUrl) {
        this.loginUrl = loginUrl;
    }

    public Map<String, String> getUsers() {
        return users;
    }

    public void setUsers(Map<String, String> users) {
        this.users = users;
    }

    public Duration getJwtExpiration() {
        return jwtExpiration;
    }

    public void setJwtExpiration(Duration jwtExpiration) {
        this.jwtExpiration = jwtExpiration;
    }

    public String getStartupScript() {
        return startupScript;
    }

    public void setStartupScript(String startupScript) {
        this.startupScript = startupScript;
    }

    public MeterRegistry getMeterRegistry() {
        return meterRegistry;
    }

    public void setMeterRegistry(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }
}
