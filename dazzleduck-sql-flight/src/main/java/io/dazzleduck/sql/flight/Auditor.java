package io.dazzleduck.sql.flight;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

public class Auditor {
    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule()).disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    private static final Logger auditLogger = LoggerFactory.getLogger("dazzleduck.audit");
    private static final Logger internal = LoggerFactory.getLogger(Auditor.class);

    private final Marker marker;

    public Auditor(Marker marker) {
        this.marker = marker;
    }

    public void audit(Object obj) {
        try {
            auditLogger.info(marker, objectMapper.writeValueAsString(obj));
        } catch (JsonProcessingException e) {
            internal.atError().setCause(e).log("Error auditing object: {}", obj);
        }
    }
}