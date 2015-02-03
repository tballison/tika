package org.apache.tika.eval;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.tika.batch.BatchProcessDriverCLI;
import org.apache.tika.batch.fs.FSBatchTestBase;
import org.apache.tika.eval.db.SqliteUtil;
import org.junit.Test;

public class ProfilerBatchTest extends FSBatchTestBase {

    @Override
    public File getInputRoot(String subdir) throws Exception {
        String path = (subdir == null || subdir.length() == 0) ? "/test-documents" : "/test-documents/"+subdir;
        File inputRoot = new File(this.getClass().getResource(path).toURI());
        return inputRoot;
    }


    @Test
    public void testSimpleDBWriteAndRead() throws Exception {
        File dbFile = new File(getInputRoot(""), "profiler.db");
        Map<String, String> m = new HashMap<String, String>();
        m.put("-dbDir", dbFile.getAbsolutePath());
        String[] args = getDefaultCommandLineArgsArr("testA", null, m);

        BatchProcessDriverCLI driver = getNewDriver("/tika-batch-single-file-profiler-config.xml", args);
        driver.execute();

        SqliteUtil dbUtil = new SqliteUtil();
        Connection conn = null;
        Statement st = null;
        Set<String> fNames = new HashSet<String>();
        try {
            conn = dbUtil.getConnection(dbFile);
            String sql = "select * from comparisons";
            st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            while (rs.next()) {
                String fileName = rs.getString(1);
                fNames.add(fileName);
            }
        } finally {
            if (st != null) {
                st.close();
            }
            if (conn != null) {
                dbUtil.shutDownDB(conn);
            }
        }

        assertEquals(2, fNames.size());
        assertTrue("file1.json", fNames.contains("file1.json"));
        assertTrue("file2.json", fNames.contains("file1.json"));
    }
}
