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

package org.apache.tika.parser.microsoft;

import java.io.File;
import java.io.InputStream;
import java.util.List;

import org.apache.tika.TikaTest;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.RecursiveParserWrapper;
import org.apache.tika.sax.BasicContentHandlerFactory;
import org.apache.tika.sax.BodyContentHandler;
import org.junit.Test;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class JackcessParserTest extends TikaTest {

    @Test
    public void testBasic() throws Exception {
        Parser p = new AutoDetectParser();
        RecursiveParserWrapper w = new RecursiveParserWrapper(p,
                new BasicContentHandlerFactory(BasicContentHandlerFactory.HANDLER_TYPE.TEXT, -1));
        InputStream is = this.getResourceAsStream("/test-documents/testAccess2.accdb");
//        InputStream is = this.getResourceAsStream("/test-documents/testAccess2_2000.mdb");
 //       InputStream is = this.getResourceAsStream("/test-documents/testAccess2_2002-2003.mdb");
        Metadata meta = new Metadata();
        ParseContext c = new ParseContext();
        w.parse(is, new DefaultHandler(), meta , c);
        List<Metadata> list = w.getMetadata();
        int i = 0;
        for (Metadata m : list) {
            for (String n : m.names()) {
                for (String v : m.getValues(n)) {
                    System.out.println(i + ": " + n + " : " + v);
                }
            }
            i++;
        }
    }

    @Test
    public void runDir() throws Exception {
        File dir = new File("C:\\Users\\tallison\\Desktop\\tmp\\jackcess\\cc_mdb");
        Parser p = new AutoDetectParser();
        int files = 0;
        int ex = 0;
        int notsupported = 0;
        for (File f : dir.listFiles()) {
            if (!f.getName().equals(
                    "com.inetnebr.incolor_pkloepper_Analyst%20Earnings%20Estimates.mdb")){
                //continue;
            }

            files++;
            System.out.println(f.getName() + " : "+f.length());
            try {
                Metadata m = new Metadata();
                ParseContext c = new ParseContext();
                BodyContentHandler h = new BodyContentHandler();
                p.parse(TikaInputStream.get(f, m), h, m, c);
            } catch (TikaException e) {
                if (!e.getMessage().contains("does not support writing") &&
                        !e.getMessage().contains("invalid page number")) {
                    ex++;

                } else {
                    notsupported++;
                    continue;
                }

                    Throwable cause = e.getCause();
                    if (cause != null) {
                        if (cause.getMessage() != null && cause.getMessage().contains("given file does not exist")) {
                            System.out.println(f.getName() + " : "+f.length());
//System.out.println(f.getName());                            throw new RuntimeException(cause);
                        }
                        if (cause.getMessage() == null ||
                         !cause.getMessage().contains("invalid page number")) {
//                            e.printStackTrace();
                        }
  //                      System.out.println("TIKA EXCEPTION: " + cause.getMessage());
                    } else {
    //                    System.out.println("TIKA EXCEPTION:" + e.getMessage());
                    }

                //e.printStackTrace();
            } catch (SAXException e) {
                //e.printStackTrace();
            } finally {
            }
        }
        System.out.println(ex + " exceptions out of "+files + " with "+notsupported);
    }
}
