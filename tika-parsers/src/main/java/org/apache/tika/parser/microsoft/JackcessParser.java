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


import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.XHTMLContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

public class JackcessParser extends AbstractParser {

    public static String MDB_PROPERTY_PREFIX = "MDB_PROP"+ Metadata.NAMESPACE_PREFIX_DELIMITER;
    public static String USER_DEFINED_PROPERTY_PREFIX = "MDB_USER_PROP"+Metadata.NAMESPACE_PREFIX_DELIMITER;
    public static Property MDB_PW = Property.externalText("Password");

    public static Property LINKED_DATABASES = Property.externalTextBag("LinkedDatabases");

    private static final long serialVersionUID = -752276948656079347L;

    private static final MediaType MEDIA_TYPE = MediaType.application("x-msaccess");

    private static final Set<MediaType> SUPPORTED_TYPES = Collections.singleton(MEDIA_TYPE);

    private Locale locale = Locale.ROOT;

    @Override
    public Set<MediaType> getSupportedTypes(ParseContext context) {
        return SUPPORTED_TYPES;
    }

    @Override
    public void parse(InputStream stream, ContentHandler handler, Metadata metadata,
                      ParseContext context) throws IOException, SAXException, TikaException {
        TikaInputStream tis = TikaInputStream.get(stream);
        Database db = null;
        XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
        xhtml.startDocument();
        try {
            db = DatabaseBuilder.open(tis.getFile());

            JackcessExtractor ex = new JackcessExtractor(context, locale);
            ex.parse(db, xhtml, metadata);
        } catch (IOException e) {
            //wrap unsupported format exception
            if (e != null) {
                if (e.getMessage().contains("Unsupported newer version")) {
                    throw new TikaException(e.getMessage());
                } else if (e.getMessage().contains("does not support writing for")) {
                    throw new TikaException(e.getMessage());
                }
            }
            throw e;
        } finally {
            try {
                if (db != null) {
                    db.close();
                }
            } catch (IOException e) {
                //swallow
            }
        }
        xhtml.endDocument();
    }
}