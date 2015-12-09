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
package org.apache.tika.parser.html;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;

import org.apache.tika.TikaTest;
import org.apache.tika.metadata.Geographic;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import org.junit.Test;
import org.xml.sax.ContentHandler;

public class JsoupParserTest extends TikaTest {

    @Test
    public void testParseAscii() throws Exception {
        Metadata metadata = new Metadata();
        XMLResult r = getXML("testHTML.html", new JsoupParser(), metadata);
        assertEquals(
                "Title : Test Indexation Html", metadata.get(TikaCoreProperties.TITLE));
        assertEquals("Tika Developers", metadata.get("Author"));
        assertEquals("5", metadata.get("refresh"));

        assertEquals("51.2312", metadata.get(Geographic.LATITUDE));
        assertEquals("-5.1987", metadata.get(Geographic.LONGITUDE));


        String content = r.xml;
        System.out.println(r.xml);
        assertTrue(
                "Did not contain expected text:" + "Test Indexation Html",
                content.contains("Test Indexation Html"));
        assertTrue(
                "Did not contain expected text:" + "Indexation</a> du fichier",
                content.contains("Indexation</a> du fichier"));
    }

    @Test
    public void testParseEmpty() throws Exception {
        ContentHandler handler = new BodyContentHandler();
        new JsoupParser().parse(
                new ByteArrayInputStream(new byte[0]),
                handler, new Metadata(), new ParseContext());
        assertEquals("", handler.toString());
    }


}
