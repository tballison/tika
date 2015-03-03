package org.apache.tika.eval;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.tika.TikaTest;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.RecursiveParserWrapper;
import org.junit.Test;

public class SimpleComparerTest extends TikaTest {


    @Test
    public void testBasic() throws Exception {
        BasicFileComparer.init(new File("testA"), new File("testB"));
        BasicFileComparer comparer = new BasicFileComparer(null, -1, -1);
        Map<String, String> data = comparer.compareFiles("relPath",
                getResourceAsFile("/test-documents/testA/file1.json"),
                getResourceAsFile("/test-documents/testB/file1.json"));
        assertTrue(data.get("TOP_10_UNIQUE_TOKEN_DIFFS_A").startsWith("over: 1"));
        assertTrue(data.get("TOP_10_UNIQUE_TOKEN_DIFFS_B").startsWith("aardvark: 3 | bear: 2"));
        assertEquals("aardvark: 3 | bear: 2 | fox: -2 | lazy: -1 | over: -1",
                data.get("TOP_10_TOKEN_DIFFS"));
        assertEquals("13", data.get("TOKEN_COUNT_B"));
        assertEquals("12", data.get("TOKEN_COUNT_A"));
        assertEquals("8", data.get("NUM_UNIQUE_TOKENS_A"));
        assertEquals("9", data.get("NUM_UNIQUE_TOKENS_B"));

        assertEquals("OVERLAP", 0.64f, Float.parseFloat(data.get("OVERLAP")), 0.0001f);
        assertEquals("DICE_COEFFICIENT", 0.8235294f, Float.parseFloat(data.get("DICE_COEFFICIENT")), 0.0001f);

    }

    @Test
    public void testEmpty() throws Exception {
        BasicFileComparer.init(new File("testA"), new File("testB"));
        BasicFileComparer comparer = new BasicFileComparer(null, -1, -1);
        Map<String, String> data = comparer.compareFiles("relPath",
                getResourceAsFile("/test-documents/testA/file1.json"),
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

}