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
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

/**
 * This class represents a single report.
 */
public class Report {

    static final Logger logger = Logger.getLogger(Report.class);

    final String NULL_VALUE = "";//TODO: make this configurable!!!
    Map<String, XSLXCellFormatter> cellFormatters = new HashMap<>();
    private XLSXNumFormatter defaultDoubleFormatter = new XLSXNumFormatter("0.000");
    private XLSXNumFormatter defaultIntegerFormatter = new XLSXNumFormatter("0");
    String sql;
    String reportFilename;
    String reportDirectory;
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
        defaultIntegerFormatter.reset(wb.getXSSFWorkbook());
        defaultDoubleFormatter.reset(wb.getXSSFWorkbook());

        try {
            dumpReportToWorkbook(st, wb);
        } finally {
            try (OutputStream os = Files.newOutputStream(out)) {
                wb.write(os);
            } finally {
                wb.dispose();
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

        ResultSetMetaData resultSetMetaData = rs.getMetaData();
        while (rs.next()) {
            xssfRow = sheet.createRow(rowCount++);
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                Cell cell = xssfRow.createCell(i-1);
                XSLXCellFormatter formatter = cellFormatters.get(meta.getColumnLabel(i));
                if (formatter == null) {
                    formatter = getDefaultFormatter(resultSetMetaData.getColumnType(i));
                }
                if (formatter != null) {
                    formatter.applyStyleAndValue(i, rs, cell);
                } else {
                    writeCell(meta, i, rs, cell);
                }
            }
        }
    }

    private XSLXCellFormatter getDefaultFormatter(int columnType) {
        switch (columnType) {
            case Types.INTEGER :
                return defaultIntegerFormatter;
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.DECIMAL:
                return defaultDoubleFormatter;
            default:
                return null;
        }
    }

    private void writeCell(ResultSetMetaData meta, int colIndex, ResultSet rs,
                           Cell cell) throws SQLException {

        switch(meta.getColumnType(colIndex)) {
            //fall through on numerics
            case Types.BIGINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.DECIMAL:
            case Types.NUMERIC:
                double dbl = rs.getDouble(colIndex);
                if (rs.wasNull()) {
                    cell.setCellValue(NULL_VALUE);
                } else {
                    cell.setCellValue(dbl);
                }
                break;
            //fall through strings
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGNVARCHAR:
                String val = rs.getString(colIndex);
                if (rs.wasNull()) {
                    cell.setCellValue(NULL_VALUE);
                } else {
                    cell.setCellValue(val);
                }
                break;
            default:
                if (rs.wasNull()) {
                    cell.setCellValue(NULL_VALUE);
                } else {
                    cell.setCellValue(rs.getString(colIndex));
                }
                logger.warn("Couldn't find type for: " + meta.getColumnType(colIndex) +
                        ". Defaulting to String");
        }
    }

}
