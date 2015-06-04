/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tika.eval;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.tika.batch.fs.FSBatchTestBase;
import org.apache.tika.batch.testutils.BatchProcessTestExecutor;
import org.apache.tika.batch.testutils.StreamStrings;
import org.apache.tika.eval.db.H2Util;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ComparerBatchTest extends FSBatchTestBase {
    public final static String COMPARER_PROCESS_CLASS = "org.apache.tika.batch.fs.FSBatchProcessCLI";

    private static Path dbDir;
    private static Connection conn;

    private final static String cTable = FileComparer.COMPARISONS_TABLE;
    private final static String thisExTable = AbstractProfiler.EXCEPTIONS_TABLE+ FileComparer.thisExtension;
    private final static String thatExTable = AbstractProfiler.EXCEPTIONS_TABLE+ FileComparer.thatExtension;
    private final static String fp = AbstractProfiler.HEADERS.FILE_PATH.name();

    @BeforeClass
    public static void setUp() throws Exception {

        File inputRoot = new File(new ComparerBatchTest().getClass().getResource("/test-documents").toURI());
        dbDir = Files.createTempDirectory(inputRoot.toPath(), "tika-test-db-dir-");
        Map<String, String> args = new HashMap<String, String>();
        Path dbFile = FileSystems.getDefault().getPath(dbDir.toString(), "comparisons_test");
        args.put("-dbDir", dbFile.toString());

        //for debugging, you can use this to select only one file pair to load
        //args.put("-includeFilePat", "file8.*");

        BatchProcessTestExecutor ex = new BatchProcessTestExecutor(COMPARER_PROCESS_CLASS, args,
                "/tika-batch-comparison-eval-config.xml");
        StreamStrings streamStrings = ex.execute();
//        System.out.println(streamStrings.getErrString());
//        System.out.println(streamStrings.getOutString());
        H2Util dbUtil = new H2Util(dbFile.toFile());
        conn = dbUtil.getConnection();
    }

    @AfterClass
    public static void tearDown() throws Exception {

        conn.close();

        FileUtils.deleteDirectory(dbDir.toFile());
    }


    @Test
    public void testSimpleDBWriteAndRead() throws Exception {
        List<String> fileNames = getColStrings(fp);
        assertEquals(10, fileNames.size());
        assertTrue(fileNames.contains("file1.pdf"));
    }

    @Test
    public void testFile1PDFRow() throws Exception {
        String where = fp+"='file1.pdf'";
        Map<String, String> data = getRow(cTable, where);
        String result = data.get(FileComparer.COMPARISON_HEADERS.TOP_10_UNIQUE_TOKEN_DIFFS + "_A");
        assertTrue(result.startsWith("over: 1"));

        result = data.get(FileComparer.COMPARISON_HEADERS.TOP_10_UNIQUE_TOKEN_DIFFS + "_B");
        assertTrue(result.startsWith("aardvark: 3 | bear: 2"));


        assertEquals("aardvark: 3 | bear: 2",
                data.get(FileComparer.COMPARISON_HEADERS.TOP_10_MORE_IN_B.toString()));
        assertEquals("fox: 2 | lazy: 1 | over: 1",
                data.get(FileComparer.COMPARISON_HEADERS.TOP_10_MORE_IN_A.toString()));
        assertEquals("12", data.get(FileComparer.HEADERS.TOKEN_COUNT+"_A"));
        assertEquals("13", data.get(FileComparer.HEADERS.TOKEN_COUNT+"_B"));
        assertEquals("8", data.get(FileComparer.HEADERS.NUM_UNIQUE_TOKENS+"_A"));
        assertEquals("9", data.get(FileComparer.HEADERS.NUM_UNIQUE_TOKENS+"_B"));

        assertEquals(FileComparer.COMPARISON_HEADERS.OVERLAP.name(),
                0.64f, Float.parseFloat(data.get("OVERLAP")), 0.0001f);

        assertEquals(FileComparer.COMPARISON_HEADERS.DICE_COEFFICIENT.name(),
                0.8235294f, Float.parseFloat(data.get("DICE_COEFFICIENT")), 0.0001f);

        assertEquals(FileComparer.HEADERS.TOKEN_LENGTH_MEAN+"_A", 3.83333d,
                Double.parseDouble(
                        data.get(FileComparer.HEADERS.TOKEN_LENGTH_MEAN+"_A")), 0.0001d);

        assertEquals(FileComparer.HEADERS.TOKEN_LENGTH_MEAN+"_B", 4.923d,
                Double.parseDouble(
                        data.get(FileComparer.HEADERS.TOKEN_LENGTH_MEAN+"_B")), 0.0001d);

        assertEquals(FileComparer.HEADERS.TOKEN_LENGTH_STD_DEV+"_A", 1.0298d,
                Double.parseDouble(
                        data.get(FileComparer.HEADERS.TOKEN_LENGTH_STD_DEV+"_A")), 0.0001d);

        assertEquals(FileComparer.HEADERS.TOKEN_LENGTH_STD_DEV+"_B", 1.9774d,
                Double.parseDouble(data.get(FileComparer.HEADERS.TOKEN_LENGTH_STD_DEV+"_B")), 0.0001d);

        assertEquals(FileComparer.HEADERS.TOKEN_LENGTH_SUM+"_A", 46,
                Integer.parseInt(
                        data.get(FileComparer.HEADERS.TOKEN_LENGTH_SUM+"_A")));

        assertEquals(FileComparer.HEADERS.TOKEN_LENGTH_SUM+"_B", 64,
                Integer.parseInt(data.get(FileComparer.HEADERS.TOKEN_LENGTH_SUM+"_B")));

        assertEquals("TOKEN_ENTROPY_RATE_A", 0.237949,
                Double.parseDouble(data.get("TOKEN_ENTROPY_RATE_A")), 0.0001d);

        assertEquals("TOKEN_ENTROPY_RATE_B", 0.232845,
                Double.parseDouble(data.get("TOKEN_ENTROPY_RATE_B")), 0.0001d);

    }

    @Test
    public void testEmpty() throws Exception {
        String where = fp+"='file4_emptyB.pdf'";
        Map<String, String> data = getRow(cTable, where);
        assertNull(data.get(AbstractProfiler.HEADERS.JSON_EX +
                FileComparer.thisExtension));
        assertTrue(data.get(AbstractProfiler.HEADERS.JSON_EX +
                FileComparer.thatExtension).equals(AbstractProfiler.JSON_PARSE_EXCEPTION));

        where = fp+"='file5_emptyA.pdf'";
        data = getRow(cTable, where);
        assertNull(data.get(AbstractProfiler.HEADERS.JSON_EX +
                FileComparer.thatExtension));
        assertTrue(data.get(AbstractProfiler.HEADERS.JSON_EX+
                FileComparer.thisExtension).equals(AbstractProfiler.JSON_PARSE_EXCEPTION));
    }

    @Test
    public void testMissingAttachment() throws Exception {
        String where = fp+"='file2_attachANotB.doc' and "+AbstractProfiler.HEADERS.EMBEDDED_FILE_PATH+
                "='inner.txt'";
        Map<String, String> data = getRow(cTable, where);
        assertContains("attachment: 1", data.get(FileComparer.COMPARISON_HEADERS.TOP_10_MORE_IN_A.name()));
        assertNotContained("fox", data.get(FileComparer.COMPARISON_HEADERS.TOP_10_MORE_IN_B.name()));
        assertNull(data.get(FileComparer.HEADERS.TOP_N_WORDS +
                FileComparer.thatExtension));
        assertNotContained("fox", data.get(FileComparer.COMPARISON_HEADERS.TOP_10_UNIQUE_TOKEN_DIFFS +
                FileComparer.thatExtension));

        assertEquals("3", data.get("NUM_METADATA_VALUES_A"));
        assertNull(data.get("DIFF_NUM_ATTACHMENTS"));
        assertNull(data.get("NUM_METADATA_VALUES_B"));
        assertEquals("0", data.get("NUM_UNIQUE_TOKENS_B"));
        assertNull(data.get("TOKEN_ENTROPY_RATE_B"));
        assertNull(data.get("NUM_EN_STOPS_TOP_N_B"));

        where = fp+"='file3_attachBNotA.doc' and "+AbstractProfiler.HEADERS.EMBEDDED_FILE_PATH+
                "='inner.txt'";
        data = getRow(cTable, where);
        assertContains("attachment: 1", data.get(FileComparer.COMPARISON_HEADERS.TOP_10_MORE_IN_B.name()));
        assertNotContained("fox", data.get(FileComparer.COMPARISON_HEADERS.TOP_10_MORE_IN_A.name()));
        assertNull(data.get(FileComparer.HEADERS.TOP_N_WORDS +
                FileComparer.thisExtension));
        assertNotContained("fox", data.get(FileComparer.COMPARISON_HEADERS.TOP_10_UNIQUE_TOKEN_DIFFS +
                FileComparer.thisExtension));

        assertEquals("3", data.get("NUM_METADATA_VALUES_B"));
        assertNull(data.get("DIFF_NUM_ATTACHMENTS"));
        assertNull(data.get("NUM_METADATA_VALUES_A"));
        assertEquals("0", data.get("NUM_UNIQUE_TOKENS_A"));
        assertNull(data.get("TOKEN_ENTROPY_RATE_A"));
        assertNull(data.get("NUM_EN_STOPS_TOP_N_A"));

    }

    @Test
    public void testBothBadJson() throws Exception {
        String where = fp+"='file7_badJson.pdf'";
        Map<String, String> data = getRow(cTable, where);
        assertEquals(AbstractProfiler.JSON_PARSE_EXCEPTION,
                data.get(AbstractProfiler.HEADERS.JSON_EX+ FileComparer.thisExtension));
        assertEquals(AbstractProfiler.JSON_PARSE_EXCEPTION,
                data.get(AbstractProfiler.HEADERS.JSON_EX+ FileComparer.thatExtension));
        assertEquals("file7_badJson.pdf",
                data.get(AbstractProfiler.HEADERS.FILE_PATH.name()));
        assertEquals("61", data.get("JSON_FILE_LENGTH_A"));
        assertEquals("0", data.get("JSON_FILE_LENGTH_B"));
        assertEquals("pdf", data.get("FILE_EXTENSION"));

    }

    @Test
    public void testAccessPermissionException() throws Exception {
        String sql = "select "+
                AbstractProfiler.EXCEPTION_HEADERS.ACCESS_PERMISSION_EXCEPTION.name() +
                " from " + AbstractProfiler.EXCEPTIONS_TABLE+"_A exA "+
                " join " + FileComparer.COMPARISONS_TABLE + " c on c.ID=exA.ID "+
                "where "+fp+"='file6_accessEx.pdf'";
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(sql);
        List<String> results = new ArrayList<String>();
        while (rs.next()) {
            results.add(rs.getString(1));
        }
        assertEquals(1, results.size());
        assertEquals("TRUE", results.get(0));

        sql = "select "+
                AbstractProfiler.EXCEPTION_HEADERS.ACCESS_PERMISSION_EXCEPTION.name() +
                " from " + AbstractProfiler.EXCEPTIONS_TABLE+"_B exB "+
                " join " + FileComparer.COMPARISONS_TABLE + " c on c.ID=exB.ID "+
                "where "+fp+"='file6_accessEx.pdf'";
        st = conn.createStatement();
        rs = st.executeQuery(sql);
        results = new ArrayList<String>();
        while (rs.next()) {
            results.add(rs.getString(1));
        }
        assertEquals(1, results.size());
        assertEquals("TRUE", results.get(0));

    }

    @Test
    public void testContainerException() throws Exception {
        String sql = "select * "+
                " from " + AbstractProfiler.EXCEPTIONS_TABLE+"_A exA "+
                " join " + FileComparer.COMPARISONS_TABLE + " c on c.ID=exA.ID "+
                "where "+fp+"='file8_IOEx.pdf'";
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(sql);

        Map<String, String> data = new HashMap<String,String>();
        ResultSetMetaData rsM = rs.getMetaData();
        while (rs.next()) {
            for (int i = 1; i <= rsM.getColumnCount(); i++)
            data.put(rsM.getColumnName(i), rs.getString(i));
        }

        String sortStack = data.get(AbstractProfiler.EXCEPTION_HEADERS.SORT_STACK_TRACE.name());
        sortStack = sortStack.replaceAll("[\r\n]", "<N>");
        assertTrue(sortStack.startsWith("java.lang.RuntimeException<N>"));

        String fullStack = data.get(AbstractProfiler.EXCEPTION_HEADERS.ORIG_STACK_TRACE.name());
        assertTrue(
                fullStack.startsWith("java.lang.RuntimeException: java.io.IOException: Value is not an integer"));
    }

    private void debugDumpAll(String table) throws Exception {
        Statement st = conn.createStatement();
        String sql = "select * from "+table;
        ResultSet rs = st.executeQuery(sql);
        ResultSetMetaData m = rs.getMetaData();
        for (int i = 1; i <= m.getColumnCount(); i++) {
            System.out.print(m.getColumnName(i) + ", ");
        }
        System.out.println("\n");
        while (rs.next()) {
            for (int i = 1; i <= m.getColumnCount(); i++) {
                System.out.print(rs.getString(i)+", ");
            }
            System.out.println("\n");
        }
        st.close();

    }
    private void debugShowColumns(String table) throws Exception {
        Statement st = conn.createStatement();
        String sql = "select * from "+table;
        ResultSet rs = st.executeQuery(sql);
        ResultSetMetaData m = rs.getMetaData();
        for (int i = 1; i <= m.getColumnCount(); i++) {
            System.out.println(i+" : "+m.getColumnName(i));
        }
        st.close();
    }

    //return the string value for one cell
    private String getString(String colName, String table, String where) throws Exception {
        List<String> results = getColStrings(colName, table, where);
        if (results.size() > 1) {
            throw new RuntimeException("more than one result");
        } else if (results.size() == 0) {
            throw new RuntimeException("no results");
        }

        return results.get(0);
    }


    private Map<String, String> getRow(String table, String where) throws Exception {
        String sql = getSql("*", table, where);
        Map<String, String> results = new HashMap<String, String>();
        Statement st = null;

        try {
            st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            ResultSetMetaData m = rs.getMetaData();
            int rows = 0;
            while (rs.next()) {
                if (rows > 0) {
                    throw new RuntimeException("returned more than one row!");
                }
                for (int i = 1; i <= m.getColumnCount(); i++) {
                    results.put(m.getColumnName(i), rs.getString(i));
                }
                rows++;
            }
        } finally {
            if (st != null) {
                st.close();
            }
        }
        return results;

    }

    //return the string representations of the column values for one column
    //as a list of strings
    private List<String> getColStrings(String colName) throws Exception {
        return getColStrings(colName, FileComparer.COMPARISONS_TABLE, null);
    }

    private List<String> getColStrings(String colName, String table, String where) throws Exception {
        String sql = getSql(colName, table, where);
        List<String> results = new ArrayList<String>();
        Statement st = null;
        try {
            st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            while (rs.next()) {
                results.add(rs.getString(1));
            }
        } finally {
            if (st != null) {
                st.close();
            }
        }
        return results;
    }

    private String getSql(String colName, String table, String where) {
        StringBuilder sb = new StringBuilder();
        sb.append("select ").append(colName).append(" from ").append(table);
        if (where != null && ! where.equals("")) {
            sb.append(" where ").append(where);
        }
        return sb.toString();
    }

}
