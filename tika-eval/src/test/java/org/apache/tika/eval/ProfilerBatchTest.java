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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tika.batch.testutils.BatchProcessTestExecutor;
import org.apache.tika.batch.testutils.StreamStrings;
import org.apache.tika.eval.db.H2Util;
import org.junit.BeforeClass;
import org.junit.Test;

public class ProfilerBatchTest {
    public final static String COMPARER_PROCESS_CLASS = "org.apache.tika.batch.fs.FSBatchProcessCLI";

    private static Path dbDir;
    private static Connection conn;

    private final static String mainTable = SingleFileProfiler.MAIN_TABLE;
    private final static String exTable = AbstractProfiler.EXCEPTIONS_TABLE+ FileComparer.thisExtension;
    private final static String fp = AbstractProfiler.HEADERS.FILE_PATH.name();

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

    @Test
    public void testSimpleDBWriteAndRead() throws Exception {

        Statement st = null;
        Set<String> fNameSet = new HashSet<String>();
        List<String> fNameList = new ArrayList<String>();
        try {
            String sql = "select * from "+SingleFileProfiler.MAIN_TABLE;
            st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            while (rs.next()) {
                String fileName = rs.getString(1);
                fNameSet.add(fileName);
                fNameList.add(fileName);
            }
        } finally {
            if (st != null) {
                st.close();
            }
            if (conn != null) {
               conn.close();
            }
        }

        assertEquals(8, fNameSet.size());
        assertEquals(9, fNameList.size());
        assertTrue("file1.pdf.json", fNameSet.contains("file1.pdf.json"));
        assertTrue("file2_attachANotB.doc.json", fNameSet.contains("file2_attachANotB.doc.json"));
        assertTrue("file3_attachBNotA.doc.json", fNameSet.contains("file3_attachBNotA.doc.json"));
        assertTrue("file4_emptyB.pdf.json", fNameSet.contains("file4_emptyB.pdf.json"));
        assertTrue("file7_badJson.pdf.json", fNameSet.contains("file7_badJson.pdf.json"));

    }
}
