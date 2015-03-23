package org.apache.tika.eval.io;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.tika.io.IOUtils;

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
public class XMLLogReader {

    private final static Logger logger = Logger.getLogger(XMLLogReader.class);
    //class that wraps a logger's xml output
    //into a single xml parseable input stream.

    public void read(InputStream xmlLogFileIs, XMLLogMsgHandler handler) throws XMLStreamException {
        InputStream is = new LogXMLWrappingInputStream(xmlLogFileIs);
        XMLStreamReader reader = XMLInputFactory.newInstance().createXMLStreamReader(is);

        boolean inEvent = false;
        boolean inMessage = false;
        Level level = null;
        while (reader.hasNext()) {
            reader.next();
            switch (reader.getEventType()) {
                case XMLStreamConstants.START_ELEMENT :
                    if ("event".equals(reader.getLocalName())) {
                        inEvent = true;
                        level = Level.toLevel(reader.getAttributeValue("", "level"), Level.DEBUG);
                    } else if ("message".equals(reader.getLocalName())) {
                        inMessage = true;
                    }
                    System.out.println("Start"+reader.getLocalName());
                    for (int i = 0; i < reader.getAttributeCount(); i++) {
                        System.out.println("ATTR: " + reader.getAttributeName(i) + " : " + reader.getAttributeValue(i));
                    }
                    break;
                case XMLStreamConstants.END_ELEMENT :
                    if ("event".equals(reader.getLocalName())) {
                        inEvent = false;
                        level = null;
                    } else if ("message".equals(reader.getLocalName())) {
                        inMessage = false;
                    }
                    System.out.println("END"+reader.getLocalName());
                    break;
                case XMLStreamConstants.CDATA :
                    try {
                        handler.handleMsg(level, reader.getText());
                    } catch (IOException e) {
                        logger.warn("Error parsing: "+reader.getText());
                    }
                    break;
            };
        }
    }



    class LogXMLWrappingInputStream extends InputStream {
        //plagiarized from log4j's chainsaw
        private final static String HEADER =
                "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>"
                        + "<log4j:eventSet version=\"1.2\" "
                        + "xmlns:log4j=\"http://jakarta.apache.org/log4j/\">";
        private static final String FOOTER = "</log4j:eventSet>";

        private InputStream[] streams;
        int currentStreamIndex = 0;

        private LogXMLWrappingInputStream(InputStream xmlLogFileIs){
            streams = new InputStream[3];
            streams[0] = new ByteArrayInputStream(HEADER.getBytes(IOUtils.UTF_8));
            streams[1] = xmlLogFileIs;
            streams[2] = new ByteArrayInputStream(FOOTER.getBytes(IOUtils.UTF_8));

        }

        @Override
        public int read() throws IOException {
            int c = streams[currentStreamIndex].read();
            if (c < 0) {
                IOUtils.closeQuietly(streams[currentStreamIndex]);
                while (currentStreamIndex < streams.length-1) {
                    currentStreamIndex++;
                    int tmpC = streams[currentStreamIndex].read();
                    if (tmpC < 0) {
                        IOUtils.closeQuietly(streams[currentStreamIndex]);
                    } else {
                        return tmpC;
                    }
                }
                return -1;
            }
            return c;
        }
    }
}
