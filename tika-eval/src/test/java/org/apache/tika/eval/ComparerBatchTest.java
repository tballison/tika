package org.apache.tika.eval;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

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
import org.apache.tika.eval.db.H2Util;
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
        File dbFile = new File(getInputRoot(""), "comparisons2");
        if (dbFile.exists()) {
            dbFile.delete();
        }
        Map<String, String> m = new HashMap<String, String>();
        m.put("-dbDir", dbFile.getAbsolutePath());
        String[] args = getDefaultCommandLineArgsArr("testA", null, m);

        BatchProcessDriverCLI driver = getNewDriver("/tika-batch-comparison-eval-config.xml", args);
        driver.setRedirectChildProcessToStdOut(true);
        driver.execute();

//        SqliteUtil dbUtil = new SqliteUtil();
        H2Util dbUtil = new H2Util();
        Connection conn = null;
        Statement st = null;
        Set<String> fileNames = new HashSet<String>();
        try {
            conn = dbUtil.getConnection(dbFile);
            String sql = "select * from comparisons";
            st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            while (rs.next()) {
                fileNames.add(rs.getString("FILE_PATH"));
            }
        } finally {
            if (st != null) {
                st.close();
            }
            if (conn != null) {
                dbUtil.shutDownDB(conn);
            }
        }
        assertEquals(3, fileNames.size());
        assertTrue(fileNames.contains("file1.pdf.json"));
    }
}
