package org.apache.tika.eval;

import org.apache.tika.TikaTest;

//These tests ensure that the comparer is extracting the right information
//into a Map<String,String>.  A full integration test
//should also ensure that the elements are properly being written to the db

public class SimpleComparerTest extends TikaTest {
/*
    @Test
    public void testBasic() throws Exception {
        BasicFileComparer.init(new File("testA"), new File("testB"));
        BasicFileComparer comparer = new BasicFileComparer(null, -1, -1);

        Map<String, String> data = comparer.compareFiles("file1.pdf.json",
                getResourceAsFile("/test-dirs/testA/file1.pdf.json"),
                getResourceAsFile("/test-dirs/testB/file1.pdf.json"));
        assertEquals("file1.pdf", data.get(AbstractProfiler.HEADERS.FILE_PATH.name()));

        assertTrue(
                data.get(BasicFileComparer.COMPARISON_HEADERS.TOP_10_UNIQUE_TOKEN_DIFFS+"_A")
                        .startsWith("over: 1"));


    }

    @Test
    public void testEmpty() throws Exception {
        BasicFileComparer.init(new File("testA"), new File("testB"));
        BasicFileComparer comparer = new BasicFileComparer(null, -1, -1);
        Map<String, String> data = comparer.compareFiles("relPath",
                getResourceAsFile("/test-dirs/testA/file1.pdf.json"),
                getResourceAsFile("/test-dirs/testB/file4_emptyB.pdf.json"));
        assertTrue(data.get("JSON_EX_B").startsWith("Error with json parsing"));
    }

    @Test
    public void testGetContent() throws Exception {
        List<Metadata> list = new ArrayList<Metadata>();
        int len = 25;
        for (int i = 0; i < 10; i++) {
            Metadata m = new Metadata();
            m.add(RecursiveParserWrapper.TIKA_CONTENT, "0123456789");
            list.add(m);
        }
        String content = AbstractProfiler.getContent(list, len);
        assertEquals(25, content.length());

        list.clear();
        Metadata m = new Metadata();
        m.add(RecursiveParserWrapper.TIKA_CONTENT, "0123456789");
        list.add(m);
        content = AbstractProfiler.getContent(list, len);
        assertEquals(10, content.length());

        content = AbstractProfiler.getContent(list, 4);
        assertEquals(4, content.length());

        //now test empty and null list
        list.clear();
        content = AbstractProfiler.getContent(list, len);
        assertEquals(0, content.length());

        content = AbstractProfiler.getContent(null, len);
        assertEquals(0, content.length());

        list.clear();
        Metadata m1 = new Metadata();//empty on purpose
        Metadata m2 = new Metadata();
        m2.add(RecursiveParserWrapper.TIKA_CONTENT, "abcdef");
        list.add(m);
        list.add(m1);
        list.add(m2);

        content = AbstractProfiler.getContent(list, 15);
        assertEquals(15, content.length());
    }

    @Test
    public void testAccessException() throws Exception {
        BasicFileComparer.init(new File("testA"), new File("testB"));
        BasicFileComparer comparer = new BasicFileComparer(null, -1, -1);
        Map<String, String> data = comparer.compareFiles("relPath",
                getResourceAsFile("/test-dirs/testA/file6_accessEx.pdf.json"),
                getResourceAsFile("/test-dirs/testB/file6_accessEx.pdf.json"));
        for (Map.Entry<String, String> e : data.entrySet()) {
            System.out.println(e.getKey() + " : " + e.getValue());
        }
        assertEquals("true", data.get(AbstractProfiler.EXCEPTION_HEADERS.ACCESS_PERMISSION_EXCEPTION.toString() + "_A"));
        assertEquals("true", data.get(AbstractProfiler.EXCEPTION_HEADERS.ACCESS_PERMISSION_EXCEPTION.toString() + "_B"));
        assertNull(data.get(AbstractProfiler.HEADERS.JSON_EX.toString() + "_A"));
        assertNull(data.get(AbstractProfiler.HEADERS.JSON_EX.toString() + "_B"));
        assertNull(data.get(AbstractProfiler.EXCEPTION_HEADERS.ORIG_STACK_TRACE.toString() + "_A"));
        assertNull(data.get(AbstractProfiler.EXCEPTION_HEADERS.ORIG_STACK_TRACE.toString() + "_B"));
        assertNull(data.get(AbstractProfiler.EXCEPTION_HEADERS.SORT_STACK_TRACE.toString() + "_A"));
        assertNull(data.get(AbstractProfiler.EXCEPTION_HEADERS.SORT_STACK_TRACE.toString() + "_B"));
    }

    @Test
    public void testDebug() throws Exception {
        BasicFileComparer.init(new File("testA"), new File("testB"));
        BasicFileComparer comparer = new BasicFileComparer(null, -1, -1);
        Map<String, String> data = comparer.compareFiles("relPath",
                getResourceAsFile("/test-dirs/testA/file1.pdf.json"),
                getResourceAsFile("/test-dirs/testB/file1.pdf.json"));
        for (Map.Entry<String, String> e : data.entrySet()) {
            System.out.println(e.getKey() + " : " + e.getValue());
        }

    }
    */
}
