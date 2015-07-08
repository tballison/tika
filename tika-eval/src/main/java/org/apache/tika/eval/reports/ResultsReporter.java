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
package org.apache.tika.eval.reports;


import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.tika.eval.db.DBUtil;
import org.apache.tika.eval.db.H2Util;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class ResultsReporter {
    List<String> before = new ArrayList<>();
    List<String> after = new ArrayList<>();
    List<Report> reports = new ArrayList<>();


    private void addBefore(String b) {
        before.add(b);
    }

    private void addAfter(String a) {
        after.add(a);
    }

    private void addReport(Report r) {
        reports.add(r);
    }

    public static ResultsReporter build(Path p) throws Exception {

        ResultsReporter r = new ResultsReporter();
        Document doc = null;
        DocumentBuilderFactory fact = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = null;

        try(InputStream is = Files.newInputStream(p)) {
            docBuilder = fact.newDocumentBuilder();
            doc = docBuilder.parse(is);
        }
        Node docElement = doc.getDocumentElement();
        assert(docElement.getNodeName().equals("reports"));
        NodeList children = docElement.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node n = children.item(i);
            if ("before".equals(n.getNodeName())) {
                for (String before : getSql(n)) {
                    r.addBefore(before);
                }
            } else if ("after".equals(n.getNodeName())) {
                for (String after : getSql(n)) {
                    r.addAfter(after);
                }
            } else if ("report".equals(n.getNodeName())) {
                Report report = buildReport(n);
                r.addReport(report);
            }
        }
        System.out.println(docElement.getNodeName());

        return r;
    }

    private static Report buildReport(Node n) {
        NodeList children = n.getChildNodes();
        Report r = new Report();
        NamedNodeMap attrs = n.getAttributes();
        r.charset = Charset.forName(attrs.getNamedItem("encoding").getNodeValue());
        r.includeSql = Boolean.parseBoolean(attrs.getNamedItem("includeSql").getNodeValue());
        r.reportDirectory = attrs.getNamedItem("reportDirectory").getNodeValue();
        r.reportFilename = attrs.getNamedItem("reportFilename").getNodeValue();
        r.reportName = attrs.getNamedItem("reportName").getNodeValue();
        r.format = getFormat(attrs.getNamedItem("format").getNodeValue());

        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            if (child.getNodeType() != 1) {
                continue;
            }
            System.out.println(child.getNodeName());
            if ("sql".equals(child.getNodeName())) {
                if (r.sql != null) {
                    throw new IllegalArgumentException("Can only have one sql statement per report");
                }
                r.sql = child.getTextContent();
                System.out.println("adding " + r.sql);
            } else if ("colformats".equals(child.getNodeName())) {
                r.colFormats = getColFormats(child);
            } else {
                throw new IllegalArgumentException("Not expecting to see:"+child.getNodeName());
            }
        }
        return r;
    }

    private static Map<String, ColFormatter> getColFormats(Node n) {
        NodeList children = n.getChildNodes();
        Map<String, ColFormatter> ret = new HashMap<>();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            if (child.getNodeType() != 1) {
                continue;
            }
            NamedNodeMap attrs = child.getAttributes();
            String columnName = attrs.getNamedItem("name").getNodeValue();
            assert(!ret.containsKey(columnName));
            String type = attrs.getNamedItem("type").getNodeValue();
            String textFormat = n.getTextContent();
            if ("numberFormatter".equals(type)) {
                ColFormatter f = new NumFormatter(textFormat);
                ret.put(columnName,f);
            }
        }
        return ret;
    }

    private static Report.FORMAT getFormat(String format) {
        assert(format != null);
        if (format.toLowerCase(Locale.ENGLISH).equals("csv")) {
            return Report.FORMAT.CSV;
        } else if (format.toLowerCase(Locale.ENGLISH).equals("html")) {
            return Report.FORMAT.HTML;
        }

        throw new IllegalArgumentException("Format must be 'csv' or 'html'. I don't "+
            "understand: "+format);

    }

    private static List<String> getSql(Node n) {
        List<String> ret = new ArrayList<>();

        NodeList children = n.getChildNodes();

        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            if (child.getNodeType() != 1) {
                continue;
            }
            System.out.println(child.getNodeType() + " : " + child.getNodeName());
//            assert("sql".equals(child.getNodeName()));
            System.out.println("TEXT: " + child.getTextContent());
            ret.add(child.getTextContent());
        }
        return ret;
    }

    public static void main(String[] args) throws Exception {
        File dbFile = new File(args[0]);
        DBUtil dbUtil = new H2Util(dbFile);
        ResultsReporter r = ResultsReporter.build(Paths.get(args[1]));
        try (Connection c = dbUtil.getConnection()) {
            r.execute(c);
        }
    }

    public void execute(Connection c) throws IOException, SQLException {
        Statement st = c.createStatement();
        for (String sql : before) {
            st.execute(sql);
        }
        for (Report r : reports) {
            r.writeReport(c);
        }
        for (String sql : after) {
            st.execute(sql);
        }
    }
}
