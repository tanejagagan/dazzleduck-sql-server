package io.dazzleduck.sql.flight.server;

public class NoSuchCatalogSchemaError extends Exception {
    private final String catalogAndSchema;
    public NoSuchCatalogSchemaError(String catalogAndSchema) {
        super(String.format("Catalog or Schema %s Not Fount", catalogAndSchema));
        this.catalogAndSchema = catalogAndSchema;
    }
}
