package org.apache.tika.eval;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.fail;

public class TestComparer {

    @Test
    public void testBasic() throws Exception {
        BasicFileComparer.init(new File("testA"), new File("testB"));
        BasicFileComparer comparer = new BasicFileComparer(null);
        Map<String, String> data = comparer.compareFiles("relPath",
                getResourceAsFile("/test-documents/testA/file1.json"),
                getResourceAsFile("/test-documents/testB/file1.json"));

        assertTrue(data.get("TOP_10_UNIQUE_TOKEN_DIFFS_testA").startsWith("over: 1"));
        assertTrue(data.get("TOP_10_UNIQUE_TOKEN_DIFFS_testB").startsWith("aardvark: 3 | bear: 2"));
        assertEquals("aardvark: 3 | bear: 2 | fox: -2 | lazy: -1 | over: -1",
                data.get("TOP_10_TOKEN_DIFFS"));
        assertEquals("13", data.get("TOKEN_COUNT_testB"));
        assertEquals("12", data.get("TOKEN_COUNT_testA"));
        assertEquals("8", data.get("NUM_UNIQUE_TOKENS_testA"));
        assertEquals("9", data.get("NUM_UNIQUE_TOKENS_testB"));
    }

    //TODO: copied from TikaTest...import tika-parsers so as to avoid redundancy
    /**
     * This method will give you back the filename incl. the absolute path name
     * to the resource. If the resource does not exist it will give you back the
     * resource name incl. the path.
     *
     * @param name
     *            The named resource to search for.
     * @return an absolute path incl. the name which is in the same directory as
     *         the the class you've called it from.
     */
    public File getResourceAsFile(String name) throws URISyntaxException {
        URL url = this.getClass().getResource(name);
        if (url != null) {
            return new File(url.toURI());
        } else {
            // We have a file which does not exists
            // We got the path
            url = this.getClass().getResource(".");
            File file = new File(new File(url.toURI()), name);
            if (file == null) {
                fail("Unable to find requested file " + name);
            }
            return file;
        }
    }
}
