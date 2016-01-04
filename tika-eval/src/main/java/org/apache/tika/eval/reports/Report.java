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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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
import org.apache.commons.io.ByteOrderMark;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.tika.io.IOUtils;

public class Report {

    enum FORMAT {
        CSV,
        HTML,
        XLSX
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
        } else if (format.equals(FORMAT.XLSX)) {
            dumpXLSX(c);
        } else {
            throw new IllegalArgumentException("Dont' recognize:"+format.toString());
        }
    }

    private void dumpXLSX(Connection c) throws IOException, SQLException {
        Statement st = c.createStatement();
        Path out = Paths.get(reportDirectory, reportFilename);
        Files.createDirectories(out.getParent());

        SXSSFWorkbook wb = new SXSSFWorkbook(new XSSFWorkbook(), 100, true, true);
        wb.setCompressTempFiles(true);
        try {
            dumpReportToWorkbook(st, wb);
        } finally {
            System.out.println("about to write file: "+out);
            try (OutputStream os = Files.newOutputStream(out)) {
                wb.write(os);
                System.out.println("Finished writing file");
            } finally {
                wb.dispose();
                System.out.println("disposed");
            }
        }
    }

    private void dumpReportToWorkbook(Statement st, SXSSFWorkbook wb) throws IOException, SQLException {
        ResultSet rs = st.executeQuery(sql);

        ResultSetMetaData m = rs.getMetaData();
        Sheet sheet = wb.createSheet("tika-eval Report");
        int rowCount = 0;
        ResultSetMetaData meta = rs.getMetaData();
        Row xssfRow = sheet.createRow(rowCount++);
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            Cell cell = xssfRow.createCell(i-1);
            cell.setCellValue(meta.getColumnLabel(i));
        }

        while (rs.next()) {
            xssfRow = sheet.createRow(rowCount++);
            for (int i = 1; i <= m.getColumnCount(); i++) {
                ColFormatter formatter = colFormats.get(m.getColumnName(i));
                String val;
                if (formatter == null) {
                    val = rs.getString(i);
                } else {
                    val = formatter.getString(i, rs);
                    System.out.println("FORMATTED_VAL: "+val);
                }
                Cell cell = xssfRow.createCell(i-1);
                cell.setCellValue(val);
            }
        }
    }

    private void dumpHTML(Connection c) throws IOException, SQLException {
        throw new IllegalArgumentException("HTML writer not available yet");
    }

    private void dumpCSV(Connection c) throws IOException, SQLException {
        Statement st = c.createStatement();
        Path out = Paths.get(reportDirectory, reportFilename);
        Files.createDirectories(out.getParent());
        System.out.println("WRITING TO : "+out.toString());
        OutputStream os = null;
        Writer w = null;
        //can't use closeable because of bom
        try {
            os = Files.newOutputStream(out);
            if (charset.equals(StandardCharsets.UTF_16LE)) {
                os.write(ByteOrderMark.UTF_16LE.getBytes());
            } else if (charset.equals(StandardCharsets.UTF_16BE)) {
                os.write(ByteOrderMark.UTF_16BE.getBytes());
            }
            w = new BufferedWriter(new OutputStreamWriter(os, charset));
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
        } catch (IOException e) {
            IOUtils.closeQuietly(w);
        }
    }
}
