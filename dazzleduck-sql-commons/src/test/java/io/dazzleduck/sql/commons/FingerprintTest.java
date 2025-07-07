package io.dazzleduck.sql.commons;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FingerprintTest {

    @Test
    public void fingerprintTest() throws SQLException, IOException {
        String sql1 = "select * from t where num > 1 and num2 >= 1.0 and str = 'str1' and date in ('2014-01-01')";
        String sql2 = "select * from t where num > 2 and num2 >= 2.0 and str = 'str2' and date in ('2014-01-02')";
        String sql3 = "select * from t where num > 3 and num2 >= 3.0 and str = 'str2' and date in ('2014-01-03')";
        String[] sqls = {sql1, sql2, sql3};
        String[] res = new String[sqls.length];
        for (int i = 0; i < sqls.length; i++) {
            res[i] = Fingerprint.generate(sqls[i]);
        }
        assertEquals (1, Arrays.stream(res).distinct().toArray().length);
    }
}
