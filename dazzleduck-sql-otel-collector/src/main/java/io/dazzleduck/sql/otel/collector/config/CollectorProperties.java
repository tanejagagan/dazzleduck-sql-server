package io.dazzleduck.sql.otel.collector.config;

import io.dazzleduck.sql.commons.ingestion.IngestionConfig;
import io.dazzleduck.sql.commons.ingestion.IngestionHandler;
import io.dazzleduck.sql.commons.ingestion.NOOPIngestionTaskFactoryProvider;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CollectorProperties {

    private int grpcPort = 4317;
    private IngestionHandler ingestionHandler =
            new NOOPIngestionTaskFactoryProvider("./otel-output").getIngestionHandler();
    private IngestionConfig ingestionConfig = new IngestionConfig(
            1_048_576L, IngestionConfig.DEFAULT_MAX_BUCKET_SIZE, IngestionConfig.DEFAULT_MAX_BATCHES,
            IngestionConfig.DEFAULT_MAX_PENDING_WRITE, Duration.ofSeconds(5),
            IngestionConfig.DEFAULT_CONFIG_REFRESH);
    private String startupScript = "INSTALL arrow FROM community; LOAD arrow;";
    private String serviceName = "open-telemetry-collector";
    // "jwt" is the only supported authentication mode.
    private String authentication = "jwt";
    private String secretKey = null;
    private String loginUrl = null;
    private Map<String, String> users = new HashMap<>();
    private Duration jwtExpiration = Duration.ofHours(1);
    private MeterRegistry meterRegistry = new SimpleMeterRegistry();
    // Queue IDs derived from ingestion_queue_table_mapping entries; must be set explicitly
    // when not using CollectorConfig.toProperties() (e.g. in tests).
    private List<String> queues = List.of("logs", "traces", "metrics");
    private boolean verifySignature = true;

    public int getGrpcPort() {
        return grpcPort;
    }

    public void setGrpcPort(int grpcPort) {
        this.grpcPort = grpcPort;
    }

    public IngestionHandler getIngestionHandler() {
        return ingestionHandler;
    }

    public void setIngestionHandler(IngestionHandler ingestionHandler) {
        this.ingestionHandler = ingestionHandler;
    }

    public IngestionConfig getIngestionConfig() {
        return ingestionConfig;
    }

    public void setIngestionConfig(IngestionConfig ingestionConfig) {
        this.ingestionConfig = ingestionConfig;
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

    public List<String> getQueues() {
        return queues;
    }

    public void setQueues(List<String> queues) {
        this.queues = queues;
    }

    public boolean isVerifySignature() {
        return verifySignature;
    }

    public void setVerifySignature(boolean verifySignature) {
        this.verifySignature = verifySignature;
    }
}
