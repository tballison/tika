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

        List<Map<String, String>> table = writer.getTable(FileComparer.COMPARISONS_TABLE);
        Map<String, String> row = table.get(0);
        assertEquals("file1.pdf", row.get(AbstractProfiler.HEADERS.FILE_PATH.name()));

        assertTrue(
                row.get(FileComparer.COMPARISON_HEADERS.TOP_10_UNIQUE_TOKEN_DIFFS + "_A")
                        .startsWith("over: 1"));


    }

    @Test
    public void testEmpty() throws Exception {
        comparer.compareFiles("relPath",
                getResourceAsFile("/test-dirs/testA/file1.pdf.json"),
                getResourceAsFile("/test-dirs/testB/file4_emptyB.pdf.json"));
        List<Map<String, String>> table = writer.getTable(FileComparer.COMPARISONS_TABLE);
        Map<String, String> row = table.get(0);
        debugPrintRow(row);
        assertTrue(row.get("JSON_EX_B").startsWith(AbstractProfiler.JSON_PARSE_EXCEPTION));
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
        List<Map<String, String>> tableA = writer.getTable(FileComparer.EXCEPTIONS_TABLE + "_A");
        List<Map<String, String>> tableB = writer.getTable(FileComparer.EXCEPTIONS_TABLE + "_B");

        Map<String, String> rowA = tableA.get(0);
        debugPrintRow(rowA);
        assertEquals("true", rowA.get(AbstractProfiler.EXCEPTION_HEADERS.ACCESS_PERMISSION_EXCEPTION.toString()));
        assertNull(rowA.get(AbstractProfiler.HEADERS.JSON_EX.toString()));
        assertNull(rowA.get(AbstractProfiler.EXCEPTION_HEADERS.ORIG_STACK_TRACE.toString()));
        assertNull(rowA.get(AbstractProfiler.EXCEPTION_HEADERS.SORT_STACK_TRACE.toString()));

        Map<String, String> rowB = tableB.get(0);
        assertEquals("true", rowB.get(AbstractProfiler.EXCEPTION_HEADERS.ACCESS_PERMISSION_EXCEPTION.toString()));
        assertNull(rowB.get(AbstractProfiler.HEADERS.JSON_EX.toString()));
        assertNull(rowB.get(AbstractProfiler.EXCEPTION_HEADERS.ORIG_STACK_TRACE.toString()));
        assertNull(rowB.get(AbstractProfiler.EXCEPTION_HEADERS.SORT_STACK_TRACE.toString()));
    }



    //every thing below here is intended for dev/debugging

    @Test
    public void testDebug() throws Exception {
        comparer.compareFiles("relPath",
                getResourceAsFile("/test-dirs/testA/file1.pdf.json"),
                getResourceAsFile("/test-dirs/testB/file1.pdf.json"));
        System.out.println("Exceptions A");
        debugPrintTable(FileComparer.EXCEPTIONS_TABLE + "_A");
        System.out.println("");
        System.out.println("Exceptions B");
        debugPrintTable(FileComparer.EXCEPTIONS_TABLE + "_B");
        System.out.println("");
        System.out.println("Comparisons");
        debugPrintTable(FileComparer.COMPARISONS_TABLE);

    }

    private void debugPrintTable(String tableName) {
        List<Map<String, String>> table = writer.getTable(tableName);
        if (table == null) {
            return;
        }
        int i = 0;

        for (Map<String, String> row : table) {
            SortedSet<String> keys = new TreeSet<String>(row.keySet());
            for (String key : keys) {
                System.out.println( i + " :: " + key + " : " + row.get(key));
            }
            i++;
        }
    }

    private void debugPrintRow(Map<String, String> row) {
        SortedSet<String> keys = new TreeSet<String>(row.keySet());
        for (String key : keys) {
            System.out.println(key + " : " + row.get(key));
        }
    }
}
