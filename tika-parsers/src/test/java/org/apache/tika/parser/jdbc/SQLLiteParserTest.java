package org.apache.tika.parser.jdbc;

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

import org.apache.tika.TikaTest;

public class SQLLiteParserTest extends TikaTest {
/*    @Test
    public void testBasic() throws Exception {
        Parser p = new AutoDetectParser();

        //test different types of input streams
        InputStream[] streams = new InputStream[3];
        streams[0] = getResourceAsStream("/test-documents/testSQLLite3b.db");
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        IOUtils.copy(getResourceAsStream("/test-documents/testSQLLite3b.db"), bos);
        streams[1] = new ByteArrayInputStream(bos.toByteArray());
        streams[2] = TikaInputStream.get(getResourceAsFile("/test-documents/testSQLLite3b.db"));
        int tests = 0;
        for (InputStream stream : streams) {
            Metadata metadata = new Metadata();
            metadata.set(Metadata.RESOURCE_NAME_KEY, "testSQLLite3b.db");
            //getXML closes the stream
            XMLResult result = getXML(stream, p, metadata);
            String x = result.xml;
            //first table name
            assertContains("<table name=\"my_table1\"><tbody><tr>\t\t<td>0</td>", x);
            //non-ascii
            assertContains("<td>普林斯顿大学</td>", x);
            //boolean
            assertContains("<td>true</td>\t\t<td>2015-01-02</td>", x);
            //date test
            assertContains("2015-01-04", x);
            //timestamp test
            assertContains("2015-01-03 15:17:03", x);
            //first embedded doc's image tag
            assertContains("alt=\"image1.png\"", x);
            //second embedded doc's image tag
            assertContains("alt=\"A description...\"", x);
            //second table name
            assertContains("<table name=\"my_table2\"><tbody><tr>\t\t<td>0</td>\t\t<td>sed, ", x);

            Metadata post = result.metadata;
            String[] tableNames = post.getValues(Database.TABLE_NAME);
            assertEquals(2, tableNames.length);
            assertEquals("my_table1", tableNames[0]);
            assertEquals("my_table2", tableNames[1]);
            tests++;
        }
        assertEquals(3, tests);
    }

    //make sure that table cells and rows are properly marked to
    //yield \t and \n at the appropriate places
    @Test
    public void testSpacesInBodyContentHandler()  throws Exception {
        Parser p = new AutoDetectParser();
        InputStream stream = null;
        Metadata metadata = new Metadata();
        metadata.set(Metadata.RESOURCE_NAME_KEY, "testSQLLite3b.db");
        ContentHandler handler = new BodyContentHandler(-1);
        ParseContext ctx = new ParseContext();
        ctx.set(Parser.class, p);
        try {
            stream = getResourceAsStream("/test-documents/testSQLLite3b.db");
            p.parse(stream, handler, metadata, ctx);
        } finally {
            stream.close();
        }
        String s = handler.toString();
        assertContains("0\t\t2.3\t\t2.4\t\tlorem", s);
        assertContains("tempor\n", s);
    }

    //test what happens if the user forgets to pass in a parser via context
    //to handle embedded documents
    @Test
    public void testNotAddingEmbeddedParserToParseContext() throws Exception {
        Parser p = new AutoDetectParser();

        InputStream is = getResourceAsStream("/test-documents/testSQLLite3b.db");
        Metadata metadata = new Metadata();
        metadata.set(Metadata.RESOURCE_NAME_KEY, "testSQLLite3b.db");
        ContentHandler handler = new ToXMLContentHandler();
        p.parse(is, handler, metadata, new ParseContext());
        String xml = handler.toString();
        //just includes headers for embedded documents
        assertContains("<div class=\"package-entry\"><h1>my_table1</h1>", xml);
        assertContains("<div class=\"package-entry\"><h1>my_table2</h1>", xml);
        //but no other content
        assertNotContained("dog", xml);
        assertNotContained("lorem", xml);
        assertNotContained("sed, ", xml);
        assertNotContained("INT_COL", xml);
        assertNotContained("alt=\"image1.png\"", xml);
        //second embedded doc's image tag
        assertNotContained("alt=\"A description...\"", xml);

    }

    @Test
    public void testRecursiveParserWrapper() throws Exception {
        Parser p = new AutoDetectParser();
        RecursiveParserWrapper wrapper =
                new RecursiveParserWrapper(p, new BasicContentHandlerFactory(
                        BasicContentHandlerFactory.HANDLER_TYPE.BODY, -1));
        InputStream is = getResourceAsStream("/test-documents/testSQLLite3b.db");
        Metadata metadata = new Metadata();
        metadata.set(Metadata.RESOURCE_NAME_KEY, "testSQLLite3b.db");
        wrapper.parse(is, new BodyContentHandler(-1), metadata, new ParseContext());
        List<Metadata> metadataList = wrapper.getMetadata();
        int i = 0;
        assertEquals(7, metadataList.size());
        //make sure the \t are inserted
        String table1 = metadataList.get(5).get(RecursiveParserWrapper.TIKA_CONTENT);
        assertContains("0\t2.3\t2.4\tlorem", table1);
        assertContains("普林斯顿大学", table1);

        //make sure the \n is inserted
        String table2 = metadataList.get(6).get(RecursiveParserWrapper.TIKA_CONTENT);
        assertContains("do eiusmod tempor\n", table2);

        assertContains("The quick brown fox", metadataList.get(2).get(RecursiveParserWrapper.TIKA_CONTENT));
        assertContains("The quick brown fox", metadataList.get(4).get(RecursiveParserWrapper.TIKA_CONTENT));

        //confirm .doc was added to blob
        assertEquals("testSQLLite3b.db/my_table1/BYTES_COL_0.doc/image1.png", metadataList.get(1).get(RecursiveParserWrapper.EMBEDDED_RESOURCE_PATH));
    }

    @Test
    public void testParserContainerExtractor() throws Exception {
        //There should be 6 embedded documents:
        //2x tables -- UTF-8 csv representations of the tables
        //2x word files, one doc and one docx
        //2x png files, the same image embedded in each of the doc and docx

        ParserContainerExtractor ex = new ParserContainerExtractor();
        ByteCopyingHandler byteCopier = new ByteCopyingHandler();
        InputStream is = getResourceAsStream("/test-documents/testSQLLite3b.db");
        Metadata metadata = new Metadata();
        metadata.set(Metadata.RESOURCE_NAME_KEY, "testSQLLite3b.db");
        ex.extract(TikaInputStream.get(is), ex, byteCopier);
        assertEquals(6, byteCopier.bytes.size());
        String[] strings = new String[6];
        for (int i = 1; i < byteCopier.bytes.size(); i++) {
            byte[] byteArr = byteCopier.bytes.get(i);
            String s = new String(byteArr, 0, Math.min(byteArr.length,1000), "UTF-8");
            strings[i] = s;
        }
        byte[] oleBytes = new byte[]{
                (byte)-48,
                (byte)-49,
                (byte)17,
                (byte)-32,
                (byte)-95,
                (byte)-79,
                (byte)26,
                (byte)-31,
                (byte)0,
                (byte)0,
        };
        //test OLE
        for (int i = 0; i < 10; i++) {
            assertEquals(oleBytes[i], byteCopier.bytes.get(0)[i]);
        }
        assertContains("PNG", strings[1]);
        assertContains("PK", strings[2]);
        assertContains("PNG", strings[3]);
        //make sure headers are included
        assertTrue(strings[4].startsWith("INT_COL,FLOAT_COL,"));
        assertContains("0,2.3,2.4,lorem,普林斯顿大学,true", strings[4]);
        assertTrue(strings[5].startsWith("INT_COL2,VARCHAR_COL2"));
        assertContains("0,\"sed, do", strings[5]);
    }

    //This confirms that reading the stream twice is not
    //quadrupling the number of attachments.
    @Test
    public void testInputStreamReset() throws Exception {
        //There should be 12 embedded documents:
        //4x tables -- 2x UTF-8 csv representations of the tables
        //4x word files, two docs and two docxs
        //4x png files, the same image embedded in each of the doc and docx

        ParserContainerExtractor ex = new ParserContainerExtractor();
        InputStreamResettingHandler byteCopier = new InputStreamResettingHandler();
        InputStream is = getResourceAsStream("/test-documents/testSQLLite3b.db");
        Metadata metadata = new Metadata();
        metadata.set(Metadata.RESOURCE_NAME_KEY, "testSQLLite3b.db");
        ex.extract(TikaInputStream.get(is), ex, byteCopier);
        is.reset();
        assertEquals(12, byteCopier.bytes.size());
    }



    public static class InputStreamResettingHandler implements EmbeddedResourceHandler {

        public List<byte[]> bytes = new ArrayList<byte[]>();

        @Override
        public void handle(String filename, MediaType mediaType,
                           InputStream stream) {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            if (! stream.markSupported()) {
                stream = TikaInputStream.get(stream);
            }
            stream.mark(1000000);
            try {
                IOUtils.copy(stream, os);
                bytes.add(os.toByteArray());
                stream.reset();
                //now try again
                os.reset();
                IOUtils.copy(stream, os);
                bytes.add(os.toByteArray());
                stream.reset();
            } catch (IOException e) {
                //swallow
            }
        }
    }
*/
    //code used for creating the test file
/*
    private Connection getConnection(String dbFileName) throws Exception {
        File testDirectory = new File(this.getClass().getResource("/test-documents").toURI());
        System.out.println("Writing to: " + testDirectory.getAbsolutePath());
        File testDB = new File(testDirectory, dbFileName);
        Connection c = null;
        try {
            Class.forName("org.sqlite.JDBC");
            c = DriverManager.getConnection("jdbc:sqlite:" + testDB.getAbsolutePath());
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
            System.exit(0);
        }
        return c;
    }

    @Test
    public void testCreateDB() throws Exception {
        Connection c = getConnection("testSQLLite3b.db");
        Statement st = c.createStatement();
        String sql = "DROP TABLE if exists my_table1";
        st.execute(sql);
        sql = "CREATE TABLE my_table1 (" +
                "INT_COL INT PRIMARY KEY, "+
                "FLOAT_COL FLOAT, " +
                "DOUBLE_COL DOUBLE, " +
                "CHAR_COL CHAR(30), "+
                "VARCHAR_COL VARCHAR(30), "+
                "BOOLEAN_COL BOOLEAN,"+
                "DATE_COL DATE,"+
                "TIME_STAMP_COL TIMESTAMP,"+
                "BYTES_COL BYTES" +
        ")";
        st.execute(sql);
        sql = "insert into my_table1 (INT_COL, FLOAT_COL, DOUBLE_COL, CHAR_COL, " +
                "VARCHAR_COL, BOOLEAN_COL, DATE_COL, TIME_STAMP_COL, BYTES_COL) " +
                "values (?,?,?,?,?,?,?,?,?)";
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        java.util.Date d = f.parse("2015-01-03 15:17:03");
        System.out.println(d.getTime());
        long d1Long = 1420229823000L;// 2015-01-02 15:17:03
        long d2Long = 1420316223000L;// 2015-01-03 15:17:03
        PreparedStatement ps = c.prepareStatement(sql);
        ps.setInt(1, 0);
        ps.setFloat(2, 2.3f);
        ps.setDouble(3, 2.4d);
        ps.setString(4, "lorem");
        ps.setString(5, "普林斯顿大学");
        ps.setBoolean(6, true);
        ps.setString(7, "2015-01-02");
        ps.setString(8, "2015-01-03 15:17:03");
//        ps.setClob(9, new StringReader(clobString));
        ps.setBytes(9, getByteArray(this.getClass().getResourceAsStream("/test-documents/testWORD_1img.doc")));//contains "quick brown fox"
        ps.executeUpdate();
        ps.clearParameters();

        ps.setInt(1, 1);
        ps.setFloat(2, 4.6f);
        ps.setDouble(3, 4.8d);
        ps.setString(4, "dolor");
        ps.setString(5, "sit");
        ps.setBoolean(6, false);
        ps.setString(7, "2015-01-04");
        ps.setString(8, "2015-01-03 15:17:03");
        //ps.setClob(9, new StringReader("consectetur adipiscing elit"));
        ps.setBytes(9, getByteArray(this.getClass().getResourceAsStream("/test-documents/testWORD_1img.docx")));//contains "The end!"

        ps.executeUpdate();

        //build table2
        sql = "DROP TABLE if exists my_table2";
        st.execute(sql);

        sql = "CREATE TABLE my_table2 (" +
                "INT_COL2 INT PRIMARY KEY, "+
                "VARCHAR_COL2 VARCHAR(64))";
        st.execute(sql);
        sql = "INSERT INTO my_table2 values(0,'sed, do eiusmod tempor')";
        st.execute(sql);
        sql = "INSERT INTO my_table2 values(1,'incididunt \nut labore')";
        st.execute(sql);

        c.close();
    }

    private byte[] getByteArray(InputStream is) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] buff = new byte[1024];
        for (int bytesRead; (bytesRead = is.read(buff)) != -1;) {
            bos.write(buff, 0, bytesRead);
        }
        return bos.toByteArray();
    }

*/


}
