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

package org.apache.tika.server;

import java.io.Writer;

import org.apache.tika.sax.WriteOutContentHandler;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

class RichTextContentHandler extends WriteOutContentHandler {
    public RichTextContentHandler(Writer writer) {
        super(writer);
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        super.startElement(uri, localName, qName, attributes);

        if ("img".equals(localName) && attributes.getValue("alt") != null) {
            String nfo = "[image: " + attributes.getValue("alt") + ']';

            characters(nfo.toCharArray(), 0, nfo.length());
        }

        if ("a".equals(localName) && attributes.getValue("name") != null) {
            String nfo = "[bookmark: " + attributes.getValue("name") + ']';

            characters(nfo.toCharArray(), 0, nfo.length());
        }
    }
}
