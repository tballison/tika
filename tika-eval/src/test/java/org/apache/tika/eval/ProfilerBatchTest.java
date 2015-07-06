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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.tika.batch.testutils.BatchProcessTestExecutor;
import org.apache.tika.batch.testutils.StreamStrings;
import org.apache.tika.eval.db.Cols;
import org.apache.tika.eval.db.H2Util;
import org.apache.tika.eval.db.TableInfo;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ProfilerBatchTest {

    public final static String COMPARER_PROCESS_CLASS = "org.apache.tika.batch.fs.FSBatchProcessCLI";

    private static Path dbDir;
    private static Connection conn;

    private final static String profileTable = SingleFileProfiler.PROFILE_TABLE.getName();
    private final static String exTable = SingleFileProfiler.PARSE_EXCEPTION_TABLE.getName();
    private final static String fpCol = Cols.FILE_PATH.name();

    @BeforeClass
    public static void setUp() throws Exception {

        File inputRoot = new File(new ComparerBatchTest().getClass().getResource("/test-dirs/testA").toURI());
        dbDir = Files.createTempDirectory(inputRoot.toPath(), "tika-test-db-dir-");
        Map<String, String> args = new HashMap<String, String>();
        Path dbFile = FileSystems.getDefault().getPath(dbDir.toString(), "profiler_test");
        args.put("-dbDir", dbFile.toString());

        //for debugging, you can use this to select only one file pair to load
        //args.put("-includeFilePat", "file8.*");

        BatchProcessTestExecutor ex = new BatchProcessTestExecutor(COMPARER_PROCESS_CLASS, args,
                "/tika-batch-single-file-profiler-config.xml");
        StreamStrings streamStrings = ex.execute();
        System.out.println(streamStrings.getErrString());
        System.out.println(streamStrings.getOutString());
        H2Util dbUtil = new H2Util(dbFile.toFile());
        conn = dbUtil.getConnection();
    }
    @AfterClass
    public static void tearDown() {
        try{
            conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
     }
    @Test
    public void testSimpleDBWriteAndRead() throws Exception {

        Statement st = null;
        List<String> fNameList = new ArrayList<String>();
        try {
            String sql = "select * from "+SingleFileProfiler.CONTAINER_TABLE.getName();
            st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            while (rs.next()) {
                String fileName = rs.getString(Cols.FILE_PATH.name());
                fNameList.add(fileName);
            }
        } finally {
            if (st != null) {
                st.close();
            }
        }
        debugTable(SingleFileProfiler.CONTAINER_TABLE);
        debugTable(SingleFileProfiler.PROFILE_TABLE);
        debugTable(SingleFileProfiler.CONTENTS_TABLE);
        debugTable(SingleFileProfiler.PARSE_EXCEPTION_TABLE);
        assertEquals(8, fNameList.size());
        assertTrue("file1.pdf", fNameList.contains("file1.pdf"));
        assertTrue("file2_attachANotB.doc", fNameList.contains("file2_attachANotB.doc"));
        assertTrue("file3_attachBNotA.doc", fNameList.contains("file3_attachBNotA.doc"));
        assertTrue("file4_emptyB.pdf", fNameList.contains("file4_emptyB.pdf"));
        assertTrue("file7_badJson.pdf", fNameList.contains("file7_badJson.pdf"));
    }
    //TODO: lots more testing!

    public void debugTable(TableInfo table) throws Exception {
        Statement st = null;
        try {
            String sql = "select * from "+table.getName();
            st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            int colCount = rs.getMetaData().getColumnCount();
            System.out.println("TABLE: "+table.getName());
            for (int i = 1; i <= colCount; i++) {
                if (i > 1) {
                    System.out.print(" | ");
                }
                System.out.print(rs.getMetaData().getColumnName(i));
            }
            System.out.println("");
            while (rs.next()) {
                for (int i = 1; i <= colCount; i++) {
                    if (i > 1) {
                        System.out.print(" | ");
                    }
                    System.out.print(rs.getString(i));
                }
                System.out.println("");
            }
        } finally {
            if (st != null) {
                st.close();
            }
        }

    }
}
