package io.dazzleduck.sql.commons.planner;

import io.dazzleduck.sql.commons.Transformations;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

public class SplitPlannerTest {


    @Test
    public void testSplitHive() throws SQLException, IOException {

        var path = "example/hive_table";
        var sql = String.format("select * from read_parquet('%s', hive_partitioning = true, hive_types = {'dt': DATE, 'p': VARCHAR})", path);
        var splits = SplitPlanner.getSplits(Transformations.parseToTree(sql), 1024 * 1024 * 1024);
        assertEquals(1, splits.size());
        assertEquals(3, splits.get(0).size());
    }

    @Test
    public void testSplitDelta() throws SQLException, IOException {

        var path = "example/delta_table";
        var sql = String.format("select * from read_delta('%s')", path);
        var splits = SplitPlanner.getSplits(Transformations.parseToTree(sql),
                1024 * 1024 * 1024);
        assertEquals(1, splits.size());
        assertEquals(8, splits.get(0).size());
    }
}
