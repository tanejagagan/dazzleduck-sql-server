package io.dazzleduck.sql.http.server;

import javax.annotation.Nullable;

public record CancelResponse(Long id,
                             String message,
                             @Nullable String error) { }
