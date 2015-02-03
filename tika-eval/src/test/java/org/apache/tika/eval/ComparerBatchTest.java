package org.apache.tika.eval;

import static junit.framework.TestCase.assertEquals;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.tika.batch.BatchProcessDriverCLI;
import org.apache.tika.batch.fs.FSBatchTestBase;
import org.apache.tika.eval.db.SqliteUtil;
import org.junit.Test;

public class ComparerBatchTest extends FSBatchTestBase {

    @Override
    public File getInputRoot(String subdir) throws Exception {
        String path = (subdir == null || subdir.length() == 0) ? "/test-documents" : "/test-documents/"+subdir;
        File inputRoot = new File(this.getClass().getResource(path).toURI());
        return inputRoot;
    }


    @Test
    public void testSimpleDBWriteAndRead() throws Exception {
        File dbFile = new File(getInputRoot(""), "comparisons2.db");
        Map<String, String> m = new HashMap<String, String>();
        m.put("-dbDir", dbFile.getAbsolutePath());
        String[] args = getDefaultCommandLineArgsArr("testA", null, m);

        BatchProcessDriverCLI driver = getNewDriver("/tika-batch-comparison-eval-config.xml", args);
        driver.execute();
        int rows = 0;

        SqliteUtil dbUtil = new SqliteUtil();
        Connection conn = null;
        Statement st = null;
        try {
            conn = dbUtil.getConnection(dbFile);
            String sql = "select * from comparisons";
            st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);

            while (rs.next()) {
                rows++;
            }
        } finally {
            if (st != null) {
                st.close();
            }
            if (conn != null) {
                dbUtil.shutDownDB(conn);
            }
        }
        assertEquals(2, rows);
    }
}
