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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.tika.MockDBWriter;
import org.apache.tika.TikaTest;
import org.apache.tika.eval.db.Cols;
import org.apache.tika.eval.db.TableInfo;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.RecursiveParserWrapper;
import org.junit.Before;
import org.junit.Test;

//These tests ensure that the comparer is extracting the right information
//into a Map<String,String>.  A full integration test
//should also ensure that the elements are properly being written to the db

public class SimpleComparerTest extends TikaTest {

    private FileComparer comparer = null;
    private MockDBWriter writer = null;

    @Before
    public void setUp() throws Exception {
        writer = new MockDBWriter();
        comparer = new FileComparer(null, new File("testA"), new File("testB"),
                false, writer, -1, -1);
    }

    @Test
    public void testBasic() throws Exception {

        comparer.compareFiles("file1.pdf.json",
                getResourceAsFile("/test-dirs/testA/file1.pdf.json"),
                getResourceAsFile("/test-dirs/testB/file1.pdf.json"));

        List<Map<Cols, String>> tableInfos = writer.getTable(FileComparer.CONTENT_COMPARISONS);
        Map<Cols, String> row = tableInfos.get(0);
        assertEquals("0", row.get(Cols.ID));
        debugPrintRow(row);
        assertTrue(
                row.get(Cols.TOP_10_UNIQUE_TOKEN_DIFFS_A)
                        .startsWith("over: 1"));

    }

    @Test
    public void testEmpty() throws Exception {
        comparer.compareFiles("relPath",
                getResourceAsFile("/test-dirs/testA/file1.pdf.json"),
                getResourceAsFile("/test-dirs/testB/file4_emptyB.pdf.json"));
        List<Map<Cols, String>> table = writer.getTable(FileComparer.ERRORS_B);
        Map<Cols, String> row = table.get(0);
        debugPrintRow(row);
        assertTrue(row.get(Cols.JSON_EX).equals(AbstractProfiler.TRUE));
    }


    @Test
    public void testGetContent() throws Exception {
        Metadata m = new Metadata();
        m.add(RecursiveParserWrapper.TIKA_CONTENT, "0123456789");

        String content = AbstractProfiler.getContent(m, 10);
        assertEquals(10, content.length());

        content = AbstractProfiler.getContent(m, 4);
        assertEquals(4, content.length());

        //test Metadata with no content
        content = AbstractProfiler.getContent(new Metadata(), 10);
        assertEquals(0, content.length());

        //test null Metadata
        content = AbstractProfiler.getContent(null, 10);
        assertEquals(0, content.length());
    }

    @Test
    public void testAccessException() throws Exception {
        comparer.compareFiles("relPath",
                getResourceAsFile("/test-dirs/testA/file6_accessEx.pdf.json"),
                getResourceAsFile("/test-dirs/testB/file6_accessEx.pdf.json"));

        for (TableInfo t : new TableInfo[]{FileComparer.EXCEPTIONS_A, FileComparer.EXCEPTIONS_B}) {
            List<Map<Cols, String>> table = writer.getTable(t);

            Map<Cols, String> rowA = table.get(0);
            debugPrintRow(rowA);
            assertEquals(Integer.toString(AbstractProfiler.EXCEPTION_TYPE.ACCESS_PERMISSION.ordinal()),
                    rowA.get(Cols.EXCEPTION_TYPE_ID));
            assertNull(rowA.get(Cols.ORIG_STACK_TRACE));
            assertNull(rowA.get(Cols.SORT_STACK_TRACE));
        }
    }




    public void testDebug() throws Exception {
        comparer.compareFiles("relPath",
                getResourceAsFile("/test-dirs/testA/file1.pdf.json"),
                getResourceAsFile("/test-dirs/testB/file1.pdf.json"));
        for (TableInfo t : new TableInfo[]{
                FileComparer.COMPARISON_CONTAINERS,
                FileComparer.ERRORS_A,
                FileComparer.ERRORS_B,
                FileComparer.EXCEPTIONS_A,
                FileComparer.EXCEPTIONS_B,
                FileComparer.PROFILES_A,
                FileComparer.PROFILES_B,
                FileComparer.CONTENTS_TABLE_A,
                FileComparer.CONTENTS_TABLE_B,
                FileComparer.CONTENT_COMPARISONS}) {
            debugPrintTable(t);
        }
    }

    private void debugPrintTable(TableInfo tableInfo) {
        List<Map<Cols, String>> table = writer.getTable(tableInfo);
        if (table == null) {
            return;
        }
        int i = 0;
        System.out.println("TABLE: "+tableInfo.getName());
        for (Map<Cols, String> row : table) {
            SortedSet<Cols> keys = new TreeSet<Cols>(row.keySet());
            for (Cols key : keys) {
                System.out.println( i + " :: " + key + " : " + row.get(key));
            }
            i++;
        }
        System.out.println("");
    }

    private void debugPrintRow(Map<Cols, String> row) {
        SortedSet<Cols> keys = new TreeSet<Cols>(row.keySet());
        for (Cols key : keys) {
            System.out.println(key + " : " + row.get(key));
        }
    }
}
