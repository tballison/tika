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
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.common.usermodel.Hyperlink;
import org.apache.tika.eval.FileComparer;
import org.apache.tika.eval.SingleFileProfiler;
import org.apache.tika.eval.db.DBUtil;
import org.apache.tika.eval.db.H2Util;
import org.apache.tika.parser.ParseContext;
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

        DocumentBuilder docBuilder = new ParseContext().getDocumentBuilder();
        Document doc;
        try(InputStream is = Files.newInputStream(p)) {
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

        return r;
    }

    private static Report buildReport(Node n) {
        NodeList children = n.getChildNodes();
        Report r = new Report();
        NamedNodeMap attrs = n.getAttributes();

        r.includeSql = Boolean.parseBoolean(attrs.getNamedItem("includeSql").getNodeValue());
        r.reportDirectory = attrs.getNamedItem("reportDirectory").getNodeValue();
        r.reportFilename = attrs.getNamedItem("reportFilename").getNodeValue();
        r.reportName = attrs.getNamedItem("reportName").getNodeValue();

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
            } else if ("colformats".equals(child.getNodeName())) {
                r.cellFormatters = getCellFormatters(child);
            } else {
                throw new IllegalArgumentException("Not expecting to see:"+child.getNodeName());
            }
        }
        return r;
    }

    private static Map<String, XSLXCellFormatter> getCellFormatters(Node n) {
        NodeList children = n.getChildNodes();
        Map<String, XSLXCellFormatter> ret = new HashMap<>();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            if (child.getNodeType() != 1) {
                continue;
            }
            NamedNodeMap attrs = child.getAttributes();
            String columnName = attrs.getNamedItem("name").getNodeValue();
            assert(!ret.containsKey(columnName));
            String type = attrs.getNamedItem("type").getNodeValue();
            if ("numberFormatter".equals(type)) {
                String format = attrs.getNamedItem("format").getNodeValue();
                XSLXCellFormatter f = new XLSXNumFormatter(format);
                ret.put(columnName,f);
            } else if ("urlLink".equals(type)) {
                String base = "";
                Node baseNode = attrs.getNamedItem("base");
                if (baseNode != null) {
                    base = baseNode.getNodeValue();
                }
                XLSXHREFFormatter f = new XLSXHREFFormatter(base, Hyperlink.LINK_URL);
                ret.put(columnName, f);
            } else if ("fileLink".equals(type)) {
                String base = "";
                Node baseNode = attrs.getNamedItem("base");
                if (baseNode != null) {
                    base = baseNode.getNodeValue();
                }
                XLSXHREFFormatter f = new XLSXHREFFormatter(base, Hyperlink.LINK_FILE);
                ret.put(columnName, f);
            }
        }
        return ret;
    }

    private static List<String> getSql(Node n) {
        List<String> ret = new ArrayList<>();

        NodeList children = n.getChildNodes();

        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            if (child.getNodeType() != 1) {
                continue;
            }
            ret.add(child.getTextContent());
        }
        return ret;
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("Must at least specify database file: my_db.mv.db");
        }


        Path dbFile = Paths.get(args[0]);
        DBUtil dbUtil = new H2Util(dbFile);

        try (Connection c = dbUtil.getConnection()) {
            Path tmpReportsFile = null;
            try {
                ResultsReporter resultsReporter = null;
                if (args.length == 1) {
                    tmpReportsFile = getDefaultReportsConfig(c);
                    resultsReporter = ResultsReporter.build(tmpReportsFile);
                } else {
                    resultsReporter = ResultsReporter.build(Paths.get(args[1]));
                }
                resultsReporter.execute(c);
            } finally {
                if (tmpReportsFile != null) {
                    Files.delete(tmpReportsFile);
                }
            }
        }
    }

    private static Path getDefaultReportsConfig(Connection c) throws IOException, SQLException {
        DatabaseMetaData md = c.getMetaData();
        String internalPath = null;
        try (ResultSet rs = md.getTables(null, null, "%", null)) {
            while (rs.next()) {
                String tName = rs.getString(3);
                if (FileComparer.CONTENTS_TABLE_B.getName().equalsIgnoreCase(tName)) {
                    internalPath = "/comparison-reports.xml";
                    break;
                } else if (SingleFileProfiler.PROFILE_TABLE.getName().equalsIgnoreCase(tName)) {
                    internalPath = "/single-dir-profile-reports.xml";
                    break;
                }
            }
        }

        if (internalPath == null) {
            throw new RuntimeException("Couldn't determine if this database was a 'profiler' or 'comparison' db");
        }
        Path tmp = Files.createTempFile("tmp-tika-reports", ".xml");
        Files.copy(ResultsReporter.class.getResourceAsStream(internalPath), tmp);
        return tmp;
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
