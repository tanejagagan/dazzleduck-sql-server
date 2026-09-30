package io.dazzleduck.sql.commons.planner;

import io.dazzleduck.sql.commons.Transformations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;

import static io.dazzleduck.sql.commons.util.TestConstants.SUPPORTED_DELTA_PATH_QUERY;
import static io.dazzleduck.sql.commons.util.TestConstants.SUPPORTED_HIVE_PATH_QUERY;

public class SplitPlannerTest {
    @Test
    public void testSplitHive() throws SQLException, IOException {
        var splits = SplitPlanner.getSplitTreeAndSize(Transformations.parseToTree(SUPPORTED_HIVE_PATH_QUERY), 1024 * 1024 * 1024);
        Assertions.assertEquals(1, splits.size());
        Assertions.assertEquals(762, splits.get(0).size());
    }

    @Test
    @Disabled("Delta partition pruning is no longer used, and fails on JDK 25: Delta Kernel reads through Hadoop, whose UserGroupInformation calls Subject.getSubject (unsupported since JDK 23)")
    public void testSplitDelta() throws SQLException, IOException {
        var splits = SplitPlanner.getSplitTreeAndSize(Transformations.parseToTree(SUPPORTED_DELTA_PATH_QUERY),
                1024 * 1024 * 1024);
        Assertions.assertEquals(1, splits.size());
        Assertions.assertEquals(5378, splits.get(0).size());
    }
}
