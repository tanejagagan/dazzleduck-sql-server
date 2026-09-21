package io.dazzleduck.sql.compaction;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Guards against bumping the parent pom's {@code duckdb.version} without also rebuilding and
 * republishing the patched DuckLake extension this module bakes into its images (see
 * DUCKLAKE_PATCH.md) — the extension is built for one exact DuckDB build and silently fails to
 * load (or is skipped entirely by an unrelated version) against any other.
 */
class DuckLakePatchVersionTest {

    @Test
    void patchedExtensionMatchesDuckDbVersion() throws IOException {
        Properties properties = new Properties();
        try (InputStream in = getClass().getClassLoader()
                .getResourceAsStream("ducklake-patch-version.properties")) {
            assertNotNull(in, "ducklake-patch-version.properties missing from test classpath");
            properties.load(in);
        }

        String duckdbVersion = properties.getProperty("duckdb.version");
        String patchedForVersion = properties.getProperty("ducklake.patch.built.for.duckdb.version");
        String patchRelease = properties.getProperty("ducklake.patch.release");

        assertEquals(duckdbVersion, patchedForVersion, () -> """
                duckdb.version (%s) no longer matches ducklake.patch.built.for.duckdb.version (%s).

                The patched DuckLake extension published as release '%s' on dazzleduck-web/ducklake \
                was built against a specific DuckDB build and is not safe to load against a \
                different one. Before merging this duckdb.version bump:
                  1. Rebuild the patch: rebase/re-cherry-pick the backport branch in \
                     dazzleduck-web/ducklake onto the new DuckDB-pinned ducklake commit, update its \
                     .github/duckdb-version, and dispatch build-and-publish-extension.yml with a new \
                     release_tag.
                  2. Update this module's pom.xml: ducklake.patch.release, \
                     ducklake.extension.cache.version, and ducklake.patch.built.for.duckdb.version.
                See DUCKLAKE_PATCH.md for the full procedure.
                """.formatted(duckdbVersion, patchedForVersion, patchRelease));
    }
}
