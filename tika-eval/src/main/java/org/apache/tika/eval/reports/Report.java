/**
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

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

public class Report {

    enum FORMAT {
        CSV,
        HTML
    }

    Map<String, ColFormatter> colFormats = new HashMap<>();
    String sql;
    String reportFilename;
    String reportDirectory;
    Charset charset;
    FORMAT format;
    //only for html
    boolean includeSql;
    String reportName;

    public void writeReport(Connection c) throws SQLException, IOException {
        if (format.equals(FORMAT.CSV)) {
            dumpCSV(c);
        } else if (format.equals(FORMAT.HTML)) {
            dumpHTML(c);
        } else {
            throw new IllegalArgumentException("Dont' recognize:"+format.toString());
        }
    }

    private void dumpHTML(Connection c) throws IOException, SQLException {

    }

    private void dumpCSV(Connection c) throws IOException, SQLException {
        Statement st = c.createStatement();
        Path out = Paths.get(reportDirectory, reportFilename);
        Files.createDirectories(out.getParent());
        System.out.println("WRITING TO : "+out.toString());
        try (Writer w = Files.newBufferedWriter(out, charset)) {
            ResultSet rs = st.executeQuery(sql);
            CSVPrinter p = CSVFormat.EXCEL.withHeader(rs).print(w);
            ResultSetMetaData m = rs.getMetaData();
            List<String> output = new ArrayList<>();
            while (rs.next()) {
                output.clear();
                for (int i = 1; i <= m.getColumnCount(); i++) {
                    ColFormatter formatter = colFormats.get(m.getColumnName(i));
                    String val;
                    if (formatter == null) {
                        val = rs.getString(i);
                    } else {
                        val = formatter.getString(i, rs);
                        System.out.println("FORMATTED_VAL: "+val);
                    }
                    output.add(val);
                }
                p.printRecord(output);
            }
            p.flush();
            w.flush();
            p.close();
        }
    }
}
