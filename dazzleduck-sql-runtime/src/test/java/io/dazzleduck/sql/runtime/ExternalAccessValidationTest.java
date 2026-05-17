package io.dazzleduck.sql.runtime;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.commons.ConnectionPool;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link Runtime#validateExternalAccessSetting} — the startup
 * defense-in-depth check that refuses to start when access_mode is non-COMPLETE
 * unless DuckDB's enable_external_access has been set to false (typically via
 * the startup script).
 *
 * <p>DuckDB makes {@code enable_external_access} a one-way switch: once disabled
 * it cannot be re-enabled within the same process. Tests are ordered so that all
 * "should throw" cases (which require external access to still be on its default
 * {@code true}) run before we flip it off — after which only the "passes" cases
 * remain valid.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ExternalAccessValidationTest {

    private static Config config(String accessMode) {
        return ConfigFactory.parseString("dazzleduck_server { access_mode = " + accessMode + " }")
                .getConfig("dazzleduck_server");
    }

    // ── Phase 1: external access is still on its default value (true). ────────
    //    Non-COMPLETE modes must reject; COMPLETE must pass.

    @Test
    @Order(10)
    void completeMode_skipsCheck_regardlessOfSetting() {
        // COMPLETE never enforces the check.
        assertDoesNotThrow(() -> Runtime.validateExternalAccessSetting(config("COMPLETE")));
    }

    @Test
    @Order(11)
    void noAccessModeConfigured_skipsCheck() {
        // Missing access_mode key → check is skipped (effectively COMPLETE).
        Config empty = ConfigFactory.empty();
        assertDoesNotThrow(() -> Runtime.validateExternalAccessSetting(empty));
    }

    @Test
    @Order(12)
    void restrictReadOnly_externalAccessTrue_throws() {
        var ex = assertThrows(IllegalStateException.class,
                () -> Runtime.validateExternalAccessSetting(config("RESTRICT_READ_ONLY")));
        assertTrue(ex.getMessage().contains("enable_external_access"),
                "Error must mention enable_external_access: " + ex.getMessage());
    }

    @Test
    @Order(13)
    void readOnly_externalAccessTrue_throws() {
        assertThrows(IllegalStateException.class,
                () -> Runtime.validateExternalAccessSetting(config("READ_ONLY")));
    }

    @Test
    @Order(14)
    void restricted_externalAccessTrue_throws() {
        assertThrows(IllegalStateException.class,
                () -> Runtime.validateExternalAccessSetting(config("RESTRICTED")));
    }

    // ── Phase 2: flip the setting once, then verify non-COMPLETE modes pass. ──

    @Test
    @Order(20)
    void restrictReadOnly_externalAccessFalse_passes() {
        // One-way flip — from this point on the setting stays false for the rest of the JVM.
        ConnectionPool.executeOnSingleton("SET enable_external_access = false;");
        assertDoesNotThrow(() -> Runtime.validateExternalAccessSetting(config("RESTRICT_READ_ONLY")));
    }

    @Test
    @Order(21)
    void readOnly_externalAccessFalse_passes() {
        assertDoesNotThrow(() -> Runtime.validateExternalAccessSetting(config("READ_ONLY")));
    }

    @Test
    @Order(22)
    void restricted_externalAccessFalse_passes() {
        assertDoesNotThrow(() -> Runtime.validateExternalAccessSetting(config("RESTRICTED")));
    }
}
