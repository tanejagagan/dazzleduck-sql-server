package io.dazzleduck.sql.logger.tailing.model;

public record LogMessage(
        String timestamp,
        String level,
        String thread,
        String logger,
        String message
) {}