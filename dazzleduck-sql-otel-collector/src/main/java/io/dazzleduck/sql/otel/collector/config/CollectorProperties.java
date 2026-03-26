package io.dazzleduck.sql.otel.collector.config;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CollectorProperties {

    private int grpcPort = 4317;
    private String outputPath = "./otel-logs";
    private String tracesOutputPath = "./otel-traces";
    private String metricsOutputPath = "./otel-metrics";
    private int flushThreshold = 1000;
    private long flushIntervalMs = 5000;
    private List<String> partitionBy = List.of();
    private String transformations = null;
    private String authentication = "jwt";
    private String secretKey = null;
    private String loginUrl = null;
    private Map<String, String> users = new HashMap<>();
    private Duration jwtExpiration = Duration.ofHours(1);

    public int getGrpcPort() {
        return grpcPort;
    }

    public void setGrpcPort(int grpcPort) {
        this.grpcPort = grpcPort;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
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

    public int getFlushThreshold() {
        return flushThreshold;
    }

    public void setFlushThreshold(int flushThreshold) {
        this.flushThreshold = flushThreshold;
    }

    public long getFlushIntervalMs() {
        return flushIntervalMs;
    }

    public void setFlushIntervalMs(long flushIntervalMs) {
        this.flushIntervalMs = flushIntervalMs;
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
}
