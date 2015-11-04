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

import static org.apache.tika.eval.AbstractProfiler.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
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
        comparer = new FileComparer(null, null, new File("extractA"), new File("extractB"),
                writer, -1, -1);
    }

    @Test
    public void testBasic() throws Exception {
        EvalFilePaths fpsA = new EvalFilePaths();
        EvalFilePaths fpsB = new EvalFilePaths();
        fpsA.sourceFileName = "file1.pdf.json";
        fpsB.sourceFileName = "file1.pdf.json";
        fpsA.extractFile = getResourceAsFile("/test-dirs/extractA/file1.pdf.json");
        fpsB.extractFile = getResourceAsFile("/test-dirs/extractB/file1.pdf.json");
        comparer.compareFiles(fpsA, fpsB);

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
        EvalFilePaths fpsA = new EvalFilePaths();
        EvalFilePaths fpsB = new EvalFilePaths();
        fpsA.sourceFileName = "file1.pdf";
        fpsB.sourceFileName = "file1.pdf";
        fpsA.extractFile = getResourceAsFile("/test-dirs/extractA/file1.pdf.json");
        fpsB.extractFile = getResourceAsFile("/test-dirs/extractB/file4_emptyB.pdf.json");
        comparer.compareFiles(fpsA, fpsB);
        List<Map<Cols, String>> table = writer.getTable(FileComparer.ERROR_TABLE_B);
        Map<Cols, String> row = table.get(0);
        debugPrintRow(row);
        assertEquals(Integer.toString(EXTRACT_ERROR_TYPE.ZERO_BYTE_EXTRACT_FILE.ordinal()),
                row.get(Cols.EXTRACT_ERROR_TYPE_ID));
    }


    @Test
    public void testGetContent() throws Exception {
        Metadata m = new Metadata();
        m.add(RecursiveParserWrapper.TIKA_CONTENT, "0123456789");

        String content = getContent(m, 10);
        assertEquals(10, content.length());

        content = getContent(m, 4);
        assertEquals(4, content.length());

        //test Metadata with no content
        content = getContent(new Metadata(), 10);
        assertEquals(0, content.length());

        //test null Metadata
        content = getContent(null, 10);
        assertEquals(0, content.length());
    }

    @Test
    public void testAccessException() throws Exception {
        EvalFilePaths fpsA = new EvalFilePaths();
        EvalFilePaths fpsB = new EvalFilePaths();
        fpsA.sourceFileName = "file6_accessEx.pdf.json";
        fpsB.sourceFileName = "file6_accessEx.pdf.json";
        fpsA.extractFile = getResourceAsFile("/test-dirs/extractA/file6_accessEx.pdf.json");
        fpsB.extractFile = getResourceAsFile("/test-dirs/extractB/file6_accessEx.pdf.json");

        comparer.compareFiles(fpsA, fpsB);
        for (TableInfo t : new TableInfo[]{FileComparer.EXCEPTION_TABLE_A, FileComparer.EXCEPTION_TABLE_B}) {
            List<Map<Cols, String>> table = writer.getTable(t);

            Map<Cols, String> rowA = table.get(0);
            debugPrintRow(rowA);
            assertEquals(Integer.toString(EXCEPTION_TYPE.ACCESS_PERMISSION.ordinal()),
                    rowA.get(Cols.PARSE_EXCEPTION_TYPE_ID));
            assertNull(rowA.get(Cols.ORIG_STACK_TRACE));
            assertNull(rowA.get(Cols.SORT_STACK_TRACE));
        }
    }


    @Test
    public void testAttachmentCounts() {
        List<Metadata> list = new ArrayList<>();
        Metadata m0 = new Metadata();
        m0.set(RecursiveParserWrapper.EMBEDDED_RESOURCE_PATH, "dir1/dir2/file.zip");//bad data should be ignored
                                                                                    //in the first metadata object
        list.add(m0);
        Metadata m1 = new Metadata();
        m1.set(RecursiveParserWrapper.EMBEDDED_RESOURCE_PATH, "/f1.docx/f2.zip/text1.txt");
        list.add(m1);
        Metadata m2 = new Metadata();
        m2.set(RecursiveParserWrapper.EMBEDDED_RESOURCE_PATH, "/f1.docx/f2.zip/text2.txt");
        list.add(m2);
        Metadata m3 = new Metadata();
        m3.set(RecursiveParserWrapper.EMBEDDED_RESOURCE_PATH, "/f1.docx/f2.zip");
        list.add(m3);
        Metadata m4 = new Metadata();
        m4.set(RecursiveParserWrapper.EMBEDDED_RESOURCE_PATH, "/f1.docx");
        list.add(m4);
        Metadata m5 = new Metadata();
        m5.set(RecursiveParserWrapper.EMBEDDED_RESOURCE_PATH, "/f1.docx/text3.txt");
        list.add(m5);

        List<Integer> counts = AbstractProfiler.countAttachments(list);

        List<Integer> expected = new ArrayList<>();
        expected.add(5);
        expected.add(0);
        expected.add(0);
        expected.add(2);
        expected.add(4);
        expected.add(0);
        assertEquals(expected, counts);
    }


    public void testDebug() throws Exception {
        EvalFilePaths fpsA = new EvalFilePaths();
        EvalFilePaths fpsB = new EvalFilePaths();
        fpsA.sourceFileName = "file1.pdf.json";
        fpsB.sourceFileName = "file1.pdf.json";
        fpsA.extractFile = getResourceAsFile("/test-dirs/extractA/file1.pdf.json");
        fpsB.extractFile = getResourceAsFile("/test-dirs/extractB/file1.pdf.json");
        for (TableInfo t : new TableInfo[]{
                FileComparer.COMPARISON_CONTAINERS,
                FileComparer.ERROR_TABLE_A,
                FileComparer.ERROR_TABLE_B,
                FileComparer.EXCEPTION_TABLE_A,
                FileComparer.EXCEPTION_TABLE_B,
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