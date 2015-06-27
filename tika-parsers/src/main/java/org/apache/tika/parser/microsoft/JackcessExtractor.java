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
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.PropertyMap;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.util.OleBlob;
import org.apache.poi.poifs.filesystem.NPOIFSFileSystem;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.XHTMLContentHandler;
import org.xml.sax.SAXException;

class JackcessExtractor extends AbstractPOIFSExtractor {
    final NumberFormat currencyFormatter;
    final DateFormat shortDateTimeFormatter;

    protected JackcessExtractor(ParseContext context, Locale locale) {
        super(context);
        currencyFormatter = NumberFormat.getCurrencyInstance(locale);
        shortDateTimeFormatter = DateFormat.getDateInstance(DateFormat.SHORT, locale);
    }

    public void parse(Database db, XHTMLContentHandler xhtml, Metadata metadata) throws IOException, SAXException, TikaException {

        for (String n : db.getLinkedDatabases().keySet()) {
            metadata.add(JackcessParser.LINKED_DATABASES, n);
        }

        String pw = db.getDatabasePassword();
        if (pw != null) {
            metadata.set(JackcessParser.MDB_PW, pw);
        }
        //Whether or not you actually want the properties,
        //make sure to call this to find out early that the file
        //is truncated/corrupt. If you don't do this,
        //"LinkedDatabases" won't be populated and
        //there's a chance that getTable() will call a linked file
        //and could reference local file inappropriately.
        PropertyMap dbp = db.getDatabaseProperties();
        for (PropertyMap.Property p : dbp) {
            metadata.add(JackcessParser.MDB_PROPERTY_PREFIX + p.getName(), toString(p.getValue(), p.getType()));
        }

        PropertyMap up = db.getUserDefinedProperties();
        for (PropertyMap.Property p : up) {
            metadata.add(JackcessParser.USER_DEFINED_PROPERTY_PREFIX+ p.getName(), toString(p.getValue(), p.getType()));
        }


        for (String tableName : db.getTableNames()) {
            if (db.getLinkedDatabases().containsKey(tableName)) {
                continue;
            }

            Table table = db.getTable(tableName);

            List<? extends Column> columns = table.getColumns();
            xhtml.startElement("table", "name", tableName);
            addHeaders(columns, xhtml);
            xhtml.startElement("tbody");

            Row r = table.getNextRow();

            if (! db.getCharset().toString().contains("UTF-16LE")){
                System.out.println("CHARSET: " + db.getCharset());
                throw new AssertionError(db.getCharset().toString());
            }
            int i = 0;
            while (r != null) {
                for (Column c : columns) {

                    handleCell(r, c, xhtml);
                }
                r = table.getNextRow();
            }
            xhtml.endElement("tbody");
            xhtml.endElement("table");
        }
    }

    private void addHeaders(List<? extends Column> columns, XHTMLContentHandler xhtml) throws SAXException {
        xhtml.startElement("thead");
        xhtml.startElement("tr");
        for (Column c : columns) {
            xhtml.startElement("th");
            xhtml.characters(c.getName());
            xhtml.endElement("th");
        }
        xhtml.endElement("tr");
        xhtml.endElement("thead");

    }

    private void handleCell(Row r, Column c, XHTMLContentHandler handler)
            throws SAXException, IOException, TikaException {
        handler.startElement("td");
        if (c.getType().equals(DataType.OLE)) {
            handleOLE(r, c.getName(), handler);
            return;
        }
        Object obj = r.get(c.getName());
        String v = toString(obj, c.getType());
        handler.characters(v);
        handler.endElement("td");
    }

    private String toString(Object value, DataType type) {
        if (value == null) {
            return "";
        }
        switch (type) {
            case LONG:
                return Integer.toString((Integer)value);
            case TEXT:
                return (String)value;
            case MONEY:
                return formatCurrency(((BigDecimal)value).doubleValue());
            case SHORT_DATE_TIME:
                return formatShortDateTime((Date)value);
            case BOOLEAN:
                return Boolean.toString((Boolean) value);
            case MEMO:
                return (String)value;

        }
        return "";
    }

    private void handleOLE(Row row, String cName, XHTMLContentHandler xhtml) throws IOException, SAXException, TikaException {
        OleBlob blob = row.getBlob(cName);
        throw new AssertionError("OLE");/*
        //lifted shamelessly from Jackcess's OleBlobTest
        if (blob == null)
            return;

        OleBlob.Content content = blob.getContent();
        if (content == null)
            return;

        switch (content.getType()) {
            case LINK:
                xhtml.characters(((OleBlob.LinkContent) content).getLinkPath());
                break;
            case SIMPLE_PACKAGE:
                OleBlob.SimplePackageContent spc = (OleBlob.SimplePackageContent) content;

                handleEmbeddedResource(
                        TikaInputStream.get(spc.getStream()),
                        spc.getFileName(),//filename
                        null,//relationshipId
                        spc.getTypeName(),//mediatype
                        xhtml, false);
                break;
            case OTHER:
                OleBlob.OtherContent oc = (OleBlob.OtherContent) content;
                handleEmbeddedResource(
                        TikaInputStream.get(oc.getStream()),
                        null,//filename
                        null,//relationshipId
                        oc.getTypeName(),//mediatype
                        xhtml, false);
                break;
            case COMPOUND_STORAGE:
                OleBlob.CompoundContent cc = (OleBlob.CompoundContent) content;
                handleCompoundContent(cc, xhtml);
                break;
        }*/
    }

    private void handleCompoundContent(OleBlob.CompoundContent cc, XHTMLContentHandler xhtml) throws IOException, SAXException, TikaException {
        NPOIFSFileSystem nfs = new NPOIFSFileSystem(cc.getStream());
        handleEmbeddedOfficeDoc(nfs.getRoot(), xhtml);
    }

    String formatCurrency(Double d) {
        if (d == null) {
            return "";
        }
        return currencyFormatter.format(d);
    }

    String formatShortDateTime(Date d) {
        if (d == null) {
            return "";
        }
        return shortDateTimeFormatter.format(d);
    }
}

