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

package org.apache.tika.parser.evil;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.tika.batch.BatchNoRestartError;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

public class EvilParser extends AbstractParser {
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    private final static String OOM_STRING = "oom exception";
    private final static String HANG_HEAVY_STRING = "hang heavy";
    private final static String HANG_LIGHT_STRING = "hang light";
    private final static String RUN_TIME_STRING = "run time";
    private final static String ASSERTION_ERROR_STRING = "assertion error";
    private final static String NULL_POINTER_STRING = "null pointer";
    private final static String TIKA_EXCEPTION_STRING = "tika exception";
    private final static String BATCH_NO_RESTART_STRING = "no restart";
    

    @Override
    public Set<MediaType> getSupportedTypes(ParseContext context) {
        Set<MediaType> types = new HashSet<MediaType>();
        MediaType type = MediaType.application("evil");
        types.add(type);
        return types;
    }

    @Override
    public void parse(InputStream stream, ContentHandler handler,
            Metadata metadata, ParseContext context) throws IOException,
            SAXException, TikaException {
        String content = basicAsciiString(stream);
        Matcher sleepMatcher  = Pattern.compile("sleep\\s+(\\d+)").matcher(content);
        if (sleepMatcher.find()) {
            String durationString = sleepMatcher.group(1);
            long duration = -1;
            try{
                duration = Long.parseLong(durationString);
            } catch (NumberFormatException e) {
                //not going to happen unless something goes wrong w regex
                throw new RuntimeException("Problem in regex parsing sleep duration");
            }
            handle(content, handler, duration);
            return;
        } else if(content.contains(OOM_STRING)) {
            kabOOM();
        } else if (content.contains(HANG_HEAVY_STRING)) {
            hangHeavy();
        } else if (content.contains(HANG_LIGHT_STRING)) {
            hangLight();
        } else if (content.contains(RUN_TIME_STRING)){
            throw new RuntimeException(RUN_TIME_STRING);
        } else if (content.contains(ASSERTION_ERROR_STRING)) {
            throw new AssertionError(ASSERTION_ERROR_STRING);
        } else if (content.contains(NULL_POINTER_STRING)) {
            throw new NullPointerException(NULL_POINTER_STRING);
        } else if (content.contains(TIKA_EXCEPTION_STRING)) {
            throw new TikaException(TIKA_EXCEPTION_STRING);
        } else if (content.contains(BATCH_NO_RESTART_STRING)) {
            throw new BatchNoRestartError("Shouldn't restart");
        }
        handle(content, handler, 0);

    }

    private void handle(String content, ContentHandler handler, long sleep) throws SAXException {
        if (sleep > 0) {
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        handler.startDocument();
        Attributes attrs = new AttributesImpl();
        handler.startElement("", "body", "body", attrs);
        handler.startElement("", "p", "p", attrs);
        char[] charArr = content.toCharArray();
        handler.characters(charArr, 0, charArr.length);
        handler.endElement("", "p", "p");
        handler.endElement("", "body", "body");
        handler.endDocument();

    }

    private void kabOOM() {
        List<int[]> ints = new ArrayList<int[]>();
        
        while (true) {
            int[] intArr = new int[32000];
            ints.add(intArr);
        }
    }
    
    private void hangHeavy() {
        while (true) {
            for (int i = 1; i < Integer.MAX_VALUE; i++) {
                for (int j = 1; j < Integer.MAX_VALUE; j++) {
                    double div = (double)i/(double)j;
                }
            }
        }
    }
    
    /**
     * hang forever but don't require heavy cpu load
     */
    private void hangLight() {
        while (true) {
            try {
                Thread.sleep(10000000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
    
    private String basicAsciiString(InputStream is) throws IOException {
        StringBuilder sb = new StringBuilder();
        int c = is.read();
        while (c != -1){
            sb.append((char)c);
            c = is.read();
        }
        return sb.toString();
    }

}
