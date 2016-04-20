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
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.ByteOrderMark;
import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.tika.io.IOUtils;

public class Report {

    static final Logger logger = Logger.getLogger(Report.class);

    final String NULL_VALUE = "";//TODO: make this configurable!!!
    Map<String, ColFormatter> colFormats = new HashMap<>();
    String sql;
    String reportFilename;
    String reportDirectory;
    Charset charset;
    //only for html
    boolean includeSql;
    String reportName;

    public void writeReport(Connection c) throws SQLException, IOException {
         dumpXLSX(c);
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

        Sheet sheet = wb.createSheet("tika-eval Report");
        int rowCount = 0;
        ResultSetMetaData meta = rs.getMetaData();
        Set<String> colNames = new HashSet<>();

        Row xssfRow = sheet.createRow(rowCount++);
        //write headers and cache them to check against styles
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            Cell cell = xssfRow.createCell(i-1);
            cell.setCellValue(meta.getColumnLabel(i));
            colNames.add(meta.getColumnLabel(i));
        }
        Map<String, CellStyle> styles = new HashMap<>();
        for (Map.Entry<String, ColFormatter> e : colFormats.entrySet()) {
            if (! colNames.contains(e.getKey())) {
                logger.warn("Format for non-existing column label: "+e.getKey() + " in "+reportName);
                continue;
            }
            CellStyle style = wb.createCellStyle();
            style.setDataFormat(wb.getCreationHelper()
                    .createDataFormat().getFormat(e.getValue().getFormatString()));
            styles.put(e.getKey(), style);
        }


        while (rs.next()) {
            xssfRow = sheet.createRow(rowCount++);
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                CellStyle style = styles.get(meta.getColumnLabel(i));
                Cell cell = xssfRow.createCell(i-1);
                writeCell(meta, rs, i, cell, style);
            }
        }
    }

    private void writeCell(ResultSetMetaData meta, ResultSet rs, int i,
                           Cell cell, CellStyle style) throws SQLException {

        switch(meta.getColumnType(i)) {
            //fall through on numerics
            case Types.BIGINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.DECIMAL:
            case Types.NUMERIC:
                if (rs.wasNull()) {
                    cell.setCellValue(NULL_VALUE);
                } else {
                    cell.setCellValue(rs.getDouble(i));
                }
                break;
            //fall through strings
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGNVARCHAR:
                if (rs.wasNull()) {
                    cell.setCellValue(NULL_VALUE);
                } else {
                    cell.setCellValue(rs.getString(i));
                }
                break;
            default:
                if (rs.wasNull()) {
                    cell.setCellValue(NULL_VALUE);
                } else {
                    cell.setCellValue(rs.getString(i));
                }
                logger.warn("Couldn't find type for: " + meta.getColumnType(i) +
                        ". Defaulting to String");
        }
        if (style != null) {
            cell.setCellStyle(style);
        }
    }

}
