package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.Config;
import io.dazzleduck.sql.common.ConfigConstants;
import io.dazzleduck.sql.commons.Transformations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;

/**
 * Provider for DuckLakeIngestionTaskFactory.
 * Reads configuration and creates factory instances for DuckLake table ingestion.
 */
public class DuckLakeIngestionTaskFactoryProvider implements IngestionTaskFactoryProvider {

    private static final Logger logger = LoggerFactory.getLogger(DuckLakeIngestionTaskFactoryProvider.class);

    private Config config;

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public IngestionHandler getIngestionHandler() {
        Map<String, QueueIdToTableMapping> pathToTableMappings = loadMappings(config);
        Duration refreshInterval = config.hasPath(ConfigConstants.QUEUE_CONFIG_REFRESH_DELAY_MS_KEY)
                ? Duration.ofMillis(config.getLong(ConfigConstants.QUEUE_CONFIG_REFRESH_DELAY_MS_KEY))
                : Duration.ofMinutes(2);
        return new DuckLakeIngestionHandler(pathToTableMappings, refreshInterval);
    }

    @Override
    public void validate() {
        if (!config.hasPath(ConfigConstants.INGESTION_QUEUE_TABLE_MAPPING_KEY)) return;
        config.getConfigList(ConfigConstants.INGESTION_QUEUE_TABLE_MAPPING_KEY).forEach(c -> {
            String queueId         = c.getString(ConfigConstants.INGESTION_QUEUE_KEY);
            // Skip entries that don't have DuckLake fields — they are output-path-only entries.
            if (!c.hasPath("catalog") || !c.hasPath("schema") || !c.hasPath("table")) return;
            boolean hasTransform   = c.hasPath(ConfigConstants.TRANSFORMATION_KEY);
            boolean hasView        = c.hasPath(ConfigConstants.VIEW_KEY);
            boolean hasInputTable  = c.hasPath(ConfigConstants.INPUT_TABLE_KEY);

            // mutual-exclusivity check
            if (hasTransform && (hasView || hasInputTable)) {
                throw new IllegalArgumentException(
                        "Queue '%s': 'transformation' and 'view'/'input_table' are mutually exclusive"
                                .formatted(queueId));
            }
            if (hasView != hasInputTable) {
                throw new IllegalArgumentException(
                        "Queue '%s': 'view' and 'input_table' must both be provided or both omitted"
                                .formatted(queueId));
            }

            if (hasTransform) {
                String rawTransformation = c.getString(ConfigConstants.TRANSFORMATION_KEY);
                // Wrap raw otel additive expression into full SQL before validation
                String wrappedTransformation = rawTransformation.trim().toUpperCase().startsWith("SELECT")
                        ? rawTransformation
                        : "SELECT *, " + rawTransformation + " FROM __this";
                validateTransformation(queueId, wrappedTransformation);
            }
            if (hasView) {
                // eagerly resolve to validate the view exists and the rewrite succeeds
                String fqView       = c.getString(ConfigConstants.VIEW_KEY);
                String fqInputTable = c.getString(ConfigConstants.INPUT_TABLE_KEY);
                validateViewTransformation(queueId, fqView, fqInputTable);
            }
        });
    }

    static void validateViewTransformation(String queueId, String fqView, String fqInputTable) {
        try {
            String derived = DuckLakeIngestionHandler.resolveViewTransformationStatic(fqView, fqInputTable);
            validateTransformation(queueId, derived);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Failed to validate view transformation for queue '%s' (view='%s', input_table='%s'): %s"
                            .formatted(queueId, fqView, fqInputTable, e.getMessage()), e);
        }
    }

    /**
     * Validates that the transformation SQL is parseable and references {@code __this} as a base table.
     *
     * @throws IllegalArgumentException if the SQL cannot be parsed or does not reference {@code __this}
     */
    static void validateTransformation(String ingestionQueue, String transformation) {
        try {
            var ast = Transformations.parseToTree(transformation);
            var statementNode = Transformations.getFirstStatementNode(ast);
            List<Transformations.CatalogSchemaTable> tables =
                    Transformations.getAllTablesOrPathsFromSelect(statementNode, "", "");
            boolean hasThis = tables.stream()
                    .anyMatch(t -> Transformations.TableType.BASE_TABLE == t.type()
                            && "__this".equals(t.tableOrPath()));
            if (!hasThis) {
                throw new IllegalArgumentException(
                        "transformation for queue '%s' must reference '__this' as a table (e.g. SELECT ... FROM __this), but found tables: %s"
                                .formatted(ingestionQueue, tables.stream().map(Transformations.CatalogSchemaTable::tableOrPath).toList()));
            }
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Invalid transformation SQL for queue '%s': %s".formatted(ingestionQueue, e.getMessage()), e);
        }
    }

    private Map<String, QueueIdToTableMapping> loadMappings(Config config) {
        if (!config.hasPath(ConfigConstants.INGESTION_QUEUE_TABLE_MAPPING_KEY)) return Map.of();
        Map<String, QueueIdToTableMapping> mappings = new LinkedHashMap<>();
        config.getConfigList(ConfigConstants.INGESTION_QUEUE_TABLE_MAPPING_KEY).forEach(c -> {
            String ingestionQueue = c.getString(ConfigConstants.INGESTION_QUEUE_KEY);
            // Skip entries that don't have the required DuckLake fields.
            if (!c.hasPath("catalog") || !c.hasPath("schema") || !c.hasPath("table")) {
                logger.debug("Skipping DuckLake mapping for queue '{}': missing catalog/schema/table", ingestionQueue);
                return;
            }
            Map<String, String> additionalParameters = new LinkedHashMap<>();
            if (c.hasPath("additional_parameters")) {
                c.getConfig("additional_parameters").entrySet().forEach(e ->
                    additionalParameters.put(e.getKey(), e.getValue().unwrapped().toString()));
            }

            String transformation = c.hasPath(ConfigConstants.TRANSFORMATION_KEY)
                    ? c.getString(ConfigConstants.TRANSFORMATION_KEY) : null;
            // Wrap raw otel additive expression (e.g. "upper(level) as level") into full SQL
            if (transformation != null && !transformation.trim().toUpperCase().startsWith("SELECT")) {
                transformation = "SELECT *, " + transformation + " FROM __this";
            }
            String view           = c.hasPath(ConfigConstants.VIEW_KEY)
                    ? c.getString(ConfigConstants.VIEW_KEY) : null;
            String inputTable     = c.hasPath(ConfigConstants.INPUT_TABLE_KEY)
                    ? c.getString(ConfigConstants.INPUT_TABLE_KEY) : null;
            QueueIdToTableMapping mapping = new QueueIdToTableMapping(
                    ingestionQueue,
                    c.getString("catalog"),
                    c.getString("schema"),
                    c.getString("table"),
                    additionalParameters,
                    transformation,
                    view,
                    inputTable);
            mappings.put(mapping.ingestionQueue(), mapping);
        });
        return mappings;
    }
}