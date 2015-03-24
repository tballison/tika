package org.apache.tika.eval;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.tika.TikaTest;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.RecursiveParserWrapper;
import org.junit.Test;

//These tests ensure that the comparer is extracting the right information
//into a Map<String,String>.  A full integration test
//should also ensure that the elements are properly being written to the db

public class SimpleComparerTest extends TikaTest {

    @Test
    public void testBasic() throws Exception {
        BasicFileComparer.init(new File("testA"), new File("testB"));
        BasicFileComparer comparer = new BasicFileComparer(null, -1, -1);

        Map<String, String> data = comparer.compareFiles("file1.pdf.json",
                getResourceAsFile("/test-documents/testA/file1.pdf.json"),
                getResourceAsFile("/test-documents/testB/file1.pdf.json"));
        assertEquals("file1.pdf", data.get(AbstractProfiler.HEADERS.FILE_PATH.name()));

        assertTrue(
                data.get(BasicFileComparer.COMPARISON_HEADERS.TOP_10_UNIQUE_TOKEN_DIFFS+"_A")
                        .startsWith("over: 1"));

        assertTrue(
                data.get(BasicFileComparer.COMPARISON_HEADERS.TOP_10_UNIQUE_TOKEN_DIFFS + "_B")
                        .startsWith("aardvark: 3 | bear: 2"));

        assertEquals("aardvark: 3 | bear: 2",
                data.get(BasicFileComparer.COMPARISON_HEADERS.TOP_10_MORE_IN_B.toString()));
        assertEquals("fox: 2 | lazy: 1 | over: 1",
                data.get(BasicFileComparer.COMPARISON_HEADERS.TOP_10_MORE_IN_A.toString()));
        assertEquals("12", data.get(BasicFileComparer.HEADERS.TOKEN_COUNT+"_A"));
        assertEquals("13", data.get(BasicFileComparer.HEADERS.TOKEN_COUNT+"_B"));
        assertEquals("8", data.get(BasicFileComparer.HEADERS.NUM_UNIQUE_TOKENS+"_A"));
        assertEquals("9", data.get(BasicFileComparer.HEADERS.NUM_UNIQUE_TOKENS+"_B"));

        assertEquals(BasicFileComparer.COMPARISON_HEADERS.OVERLAP.name(),
                0.64f, Float.parseFloat(data.get("OVERLAP")), 0.0001f);

        assertEquals(BasicFileComparer.COMPARISON_HEADERS.DICE_COEFFICIENT.name(),
                0.8235294f, Float.parseFloat(data.get("DICE_COEFFICIENT")), 0.0001f);

        assertEquals(BasicFileComparer.HEADERS.TOKEN_LENGTH_MEAN+"_A", 3.83333d,
                Double.parseDouble(
                        data.get(BasicFileComparer.HEADERS.TOKEN_LENGTH_MEAN+"_A")), 0.0001d);

        assertEquals(BasicFileComparer.HEADERS.TOKEN_LENGTH_MEAN+"_B", 4.923d,
                Double.parseDouble(
                        data.get(BasicFileComparer.HEADERS.TOKEN_LENGTH_MEAN+"_B")), 0.0001d);

        assertEquals(BasicFileComparer.HEADERS.TOKEN_LENGTH_STD_DEV+"_A", 1.0298d,
                Double.parseDouble(
                        data.get(BasicFileComparer.HEADERS.TOKEN_LENGTH_STD_DEV+"_A")), 0.0001d);

        assertEquals(BasicFileComparer.HEADERS.TOKEN_LENGTH_STD_DEV+"_B", 1.9774d,
                Double.parseDouble(data.get(BasicFileComparer.HEADERS.TOKEN_LENGTH_STD_DEV+"_B")), 0.0001d);

        assertEquals(BasicFileComparer.HEADERS.TOKEN_LENGTH_SUM+"_A", 46,
                Integer.parseInt(
                        data.get(BasicFileComparer.HEADERS.TOKEN_LENGTH_SUM+"_A")));

        assertEquals(BasicFileComparer.HEADERS.TOKEN_LENGTH_SUM+"_B", 64,
                Integer.parseInt(data.get(BasicFileComparer.HEADERS.TOKEN_LENGTH_SUM+"_B")));

        assertEquals("TOKEN_ENTROPY_RATE_A", 0.237949,
                Double.parseDouble(data.get("TOKEN_ENTROPY_RATE_A")), 0.0001d);

        assertEquals("TOKEN_ENTROPY_RATE_B", 0.232845,
                Double.parseDouble(data.get("TOKEN_ENTROPY_RATE_B")), 0.0001d);

    }

    @Test
    public void testEmpty() throws Exception {
        BasicFileComparer.init(new File("testA"), new File("testB"));
        BasicFileComparer comparer = new BasicFileComparer(null, -1, -1);
        Map<String, String> data = comparer.compareFiles("relPath",
                getResourceAsFile("/test-documents/testA/file1.pdf.json"),
                getResourceAsFile("/test-documents/testB/empty.json"));
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
                getResourceAsFile("/test-documents/testA/file3_accessEx.json"),
                getResourceAsFile("/test-documents/testB/file3_accessEx.json"));
        for (Map.Entry<String, String> e : data.entrySet()) {
            System.out.println(e.getKey() + " : " + e.getValue());
        }
        assertEquals("true", data.get(AbstractProfiler.HEADERS.ACCESS_PERMISSION_EXCEPTION.toString() + "_A"));
        assertEquals("true", data.get(AbstractProfiler.HEADERS.ACCESS_PERMISSION_EXCEPTION.toString() + "_B"));
        assertNull(data.get(AbstractProfiler.HEADERS.JSON_EX.toString() + "_A"));
        assertNull(data.get(AbstractProfiler.HEADERS.JSON_EX.toString() + "_B"));
        assertNull(data.get(AbstractProfiler.HEADERS.ORIG_STACK_TRACE.toString() + "_A"));
        assertNull(data.get(AbstractProfiler.HEADERS.ORIG_STACK_TRACE.toString() + "_B"));
        assertNull(data.get(AbstractProfiler.HEADERS.SORT_STACK_TRACE.toString() + "_A"));
        assertNull(data.get(AbstractProfiler.HEADERS.SORT_STACK_TRACE.toString() + "_B"));
    }

    @Test
    public void testDebug() throws Exception {
        BasicFileComparer.init(new File("testA"), new File("testB"));
        BasicFileComparer comparer = new BasicFileComparer(null, -1, -1);
        Map<String, String> data = comparer.compareFiles("relPath",
                getResourceAsFile("/test-documents/testA/file1.pdf.json"),
                getResourceAsFile("/test-documents/testB/file1.pdf.json"));
        for (Map.Entry<String, String> e : data.entrySet()) {
            System.out.println(e.getKey() + " : " + e.getValue());
        }

    }
}
