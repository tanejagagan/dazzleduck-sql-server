package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.Config;
import io.dazzleduck.sql.common.ConfigConstants;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.SqlVariables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The DuckDB session variables of one ingestion queue: values its transformation reads with
 * {@code getvariable('name')} instead of carrying them as literals in the SQL.
 *
 * <p>A queue declares them inline in the config file, in a key/value relation, or both:
 * <pre>
 *   ingestion_queue_table_mapping = [{
 *     ingestion_queue = "logs"
 *     catalog = "loglake", schema = "main", table = "logs"
 *     transformation = "SELECT *, getvariable('env') AS env FROM __this"
 *
 *     variables { env = "prod", tier = "hot" }          # static, from this file
 *     variables_view = "loglake.main.v_logs_vars"       # reloadable, from a relation
 *     variables_key_column   = "key"                    # default
 *     variables_value_column = "value"                  # default
 *     variables_expiration_column = "expires_at"        # optional; see below
 *   }]
 * </pre>
 *
 * <p>The relation holds one queue's variables: every row in it is this queue's own, so a row that
 * cannot be applied is an error rather than something to skip. A deployment that keeps its
 * variables in one shared table gives each queue a view that selects (and renames) its own rows.
 *
 * <p>Static pairs are read once, when the config is loaded. The relation is read at startup and
 * again on every queue-config refresh — the same tick that re-derives a view-based transformation
 * (see {@link DuckLakeIngestionHandler}) — so changing a row changes what the next batch is written
 * with, without restarting the server. Rows change the data of a relation, not the schema, so this
 * reload deliberately does not wait for a DuckLake schema change.
 *
 * <p>When both sources define a name, the relation wins: the file holds the deployment's defaults
 * and the relation is what an operator changes at runtime.
 *
 * <p>The relation can live anywhere the ingestion connection can read: a DuckLake table or view,
 * a plain table, or — named by its full {@code catalog.schema.table} path — a SQLite table reached
 * through {@code ATTACH} (the attach itself belongs in the startup script, which runs before any
 * queue is served).
 *
 * <p><b>Expiry.</b> When {@code variables_expiration_column} names a timestamp column, a row whose
 * expiration has passed is treated as absent: the variable is not set, so the transformation reads
 * the config file's value for that name if it declares one, and otherwise {@code NULL}. A row with
 * no expiration ({@code NULL}) never expires. The comparison is made by DuckDB against its own
 * {@code now()}, in the same query that reads the rows, so a stored {@code TIMESTAMP} and a
 * {@code TIMESTAMPTZ} are each compared the way that engine defines. The column is cast to
 * {@code TIMESTAMPTZ} for the comparison, which is what lets a SQLite-backed relation work at all:
 * SQLite has no timestamp type, so such a column arrives as {@code VARCHAR}. A value that will not
 * cast is an error naming the variable, not a row that never expires.
 *
 * <p>An expiry takes effect on the refresh that follows it, so a value is live for at most
 * {@code queue_config_refresh_delay_ms} past its expiration.
 *
 * <p><b>Every value is a VARCHAR</b>, as {@code SET VARIABLE} applies it. A transformation
 * comparing against a number or a timestamp casts: {@code getvariable('retention_days')::INT}.
 *
 * @param staticVariables name/value pairs declared in the config file, read once when it is loaded
 * @param view            the key/value relation to re-read on every refresh, or null when the
 *                        queue declares none
 */
public record IngestionVariables(Map<String, String> staticVariables, View view) {

    private static final Logger logger = LoggerFactory.getLogger(IngestionVariables.class);

    /** Names the variables in failure messages, so a rejection points at the queue's config. */
    static final String NOUN = "ingestion variable";

    /** A queue with no variables configured. */
    public static final IngestionVariables NONE = new IngestionVariables(Map.of(), null);

    private static final String DEFAULT_KEY_COLUMN = "key";
    private static final String DEFAULT_VALUE_COLUMN = "value";

    /**
     * A two-column key/value table or view holding variables, with the namespace this queue reads.
     *
     * @param relation    table or view name, optionally catalog- and schema-qualified
     * @param keyColumn   column holding the variable name
     * @param valueColumn column holding its value
     * @param expirationColumn timestamp column after which a row no longer counts, or null when the
     *                    relation has none. Null is only read when configured, so a relation
     *                    without such a column keeps working
     */
    public record View(String relation, String keyColumn, String valueColumn, String expirationColumn) {
        public View {
            // Interpolated into SQL, so they must be plain identifiers (never bound parameters).
            SqlVariables.identifier(ConfigConstants.VARIABLES_VIEW_KEY, relation);
            SqlVariables.identifier(ConfigConstants.VARIABLES_KEY_COLUMN_KEY, keyColumn);
            SqlVariables.identifier(ConfigConstants.VARIABLES_VALUE_COLUMN_KEY, valueColumn);
            if (expirationColumn != null) {
                SqlVariables.identifier(ConfigConstants.VARIABLES_EXPIRATION_COLUMN_KEY, expirationColumn);
            }
        }

        /** Whether rows carry an expiration this queue honours. */
        public boolean hasExpiration() {
            return expirationColumn != null;
        }
    }

    /** Validates the static pairs on every construction path and keeps them in declared order. */
    public IngestionVariables {
        staticVariables = staticVariables == null
                ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(staticVariables));
        // A bad name or value here is an error to report at startup, not something to drop: a
        // transformation that then read NULL would write a batch nobody asked for.
        SqlVariables.validate(staticVariables, NOUN);
    }

    /** Whether a relation is configured, and so whether {@link #resolve} does a query. */
    public boolean hasView() {
        return view != null;
    }

    public boolean isEmpty() {
        return staticVariables.isEmpty() && view == null;
    }

    /**
     * The queue's variables now: the static pairs overlaid with the relation's rows when one is
     * configured. Empty when nothing is configured; never null.
     *
     * @throws RuntimeException if the relation cannot be read, or holds a row whose name or value
     *         breaks the rules in {@link SqlVariables}. Failing is deliberate: the alternative is
     *         writing a batch whose transformation silently read NULL.
     */
    public Map<String, String> resolve(String queueId) {
        if (view == null) {
            return staticVariables;
        }
        Map<String, String> resolved = new LinkedHashMap<>(staticVariables);
        resolved.putAll(readView(queueId));
        SqlVariables.validate(resolved, NOUN);
        return Collections.unmodifiableMap(resolved);
    }

    private Map<String, String> readView(String queueId) {
        // DuckDB decides whether a row has expired, in the same query that reads it: comparing
        // against its own now() is the one comparison that cannot disagree with how the value was
        // stored. The cast is what lets the relation live anywhere — a SQLite table reached through
        // ATTACH presents the column as VARCHAR (SQLite has no timestamp type), and comparing that
        // to now() is a binder error without it; for a native TIMESTAMP or TIMESTAMPTZ column the
        // cast changes nothing. TRY_CAST so an unparseable value is reported as such rather than
        // failing the whole read, and a relation with no expiration column selects constants, so
        // there is a single row-reading path.
        String cast = view.hasExpiration()
                ? "TRY_CAST(%s AS TIMESTAMPTZ)".formatted(view.expirationColumn())
                : null;
        String badExpiry = cast == null ? "false"
                : "(%s IS NOT NULL AND %s IS NULL)".formatted(view.expirationColumn(), cast);
        String expired = cast == null ? "false"
                : "(%1$s IS NOT NULL AND %1$s <= now())".formatted(cast);
        String sql = "SELECT %s, %s, %s, %s FROM %s"
                .formatted(view.keyColumn(), view.valueColumn(), badExpiry, expired, view.relation());
        Map<String, String> rows = new LinkedHashMap<>();
        List<String> expiredNames = new ArrayList<>();
        try (Connection connection = ConnectionPool.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                String name = rs.getString(1);
                String value = rs.getString(2);
                if (name == null || value == null) {
                    // An unset name or value says nothing; getvariable() of a name never set
                    // already reads NULL, so there is nothing to apply either way.
                    continue;
                }
                // The relation is this queue's own, so a key that could not be a variable name is a
                // mistake in it, not another consumer's row. Fail, naming both — the same rule the
                // config file's static pairs follow, and the one its values already follow below.
                if (!SqlVariables.isValidName(name)) {
                    throw new IllegalArgumentException(
                            "Queue '%s': %s is not a valid %s name in %s"
                                    .formatted(queueId, name, NOUN, view.relation()));
                }
                if (rs.getBoolean(3)) {
                    // An expiration that is set but unreadable: silently treating it as "never
                    // expires" would keep a value alive forever on a typo.
                    throw new IllegalArgumentException(
                            "Queue '%s': %s '%s' in %s has an expiration that is not a timestamp"
                                    .formatted(queueId, NOUN, name, view.relation()));
                }
                if (rs.getBoolean(4)) {
                    // Expired: the variable is not set at all, so the file's value for this name
                    // applies if it declares one, and otherwise getvariable() reads NULL.
                    expiredNames.add(name);
                    continue;
                }
                // Two live rows for one name leave which value wins up to the scan order, which a
                // view without an ORDER BY does not fix. Fail instead of letting the transformation
                // read whichever row came back first. An expired row alongside a live one is not
                // this case — it was skipped above, so rotating a value by expiring the old row
                // keeps working.
                String previous = rows.put(name, value);
                if (previous != null) {
                    throw new IllegalArgumentException(
                            "Queue '%s': %s '%s' is defined more than once in %s (values '%s' and '%s')"
                                    .formatted(queueId, NOUN, name, view.relation(), previous, value));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to load %ss for queue '%s' from %s"
                    .formatted(NOUN, queueId, view.relation()), e);
        }
        if (!expiredNames.isEmpty()) {
            // Info, not a warning: an expiry is the configured outcome, not a fault — but it stays
            // on by default, because a transformation reading an expired variable sees the file's
            // default or NULL, which is easy to mistake for a bug. Repeats each refresh while the
            // row stays expired.
            logger.atInfo().log("Queue '{}': {} expired in {} and {} not set",
                    queueId, expiredNames, view.relation(),
                    expiredNames.size() == 1 ? "is" : "are");
        }
        return rows;
    }

    /**
     * Reads the {@code variables*} keys of one {@code ingestion_queue_table_mapping} entry.
     * Returns {@link #NONE} when the entry declares no variables.
     */
    public static IngestionVariables fromConfig(Config entry) {
        Map<String, String> statics = new LinkedHashMap<>();
        if (entry.hasPath(ConfigConstants.VARIABLES_KEY)) {
            // Values are read as strings whatever they look like in the file: a bare 42 or true is
            // a convenience here, not an error as it is in the JWT claim, since a config file has
            // no ambiguity about who wrote it. SET VARIABLE applies all of them as VARCHAR.
            entry.getConfig(ConfigConstants.VARIABLES_KEY).entrySet().forEach(e ->
                    statics.put(e.getKey(), String.valueOf(e.getValue().unwrapped())));
        }
        View view = null;
        if (entry.hasPath(ConfigConstants.VARIABLES_VIEW_KEY)) {
            view = new View(
                    entry.getString(ConfigConstants.VARIABLES_VIEW_KEY),
                    entry.hasPath(ConfigConstants.VARIABLES_KEY_COLUMN_KEY)
                            ? entry.getString(ConfigConstants.VARIABLES_KEY_COLUMN_KEY) : DEFAULT_KEY_COLUMN,
                    entry.hasPath(ConfigConstants.VARIABLES_VALUE_COLUMN_KEY)
                            ? entry.getString(ConfigConstants.VARIABLES_VALUE_COLUMN_KEY) : DEFAULT_VALUE_COLUMN,
                    entry.hasPath(ConfigConstants.VARIABLES_EXPIRATION_COLUMN_KEY)
                            ? entry.getString(ConfigConstants.VARIABLES_EXPIRATION_COLUMN_KEY) : null);
        }
        return statics.isEmpty() && view == null ? NONE : new IngestionVariables(statics, view);
    }
}
