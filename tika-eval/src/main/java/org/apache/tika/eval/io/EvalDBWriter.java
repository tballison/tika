package org.apache.tika.eval.io;

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


import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.apache.tika.eval.db.ColInfo;
import org.apache.tika.eval.db.DBUtil;
import org.apache.tika.io.IOExceptionWithCause;

/**
 * This is still in its early stages.  The idea is to
 * get something working with sqlite and then add to that
 * as necessary.
 *
 * Beware, this deletes the db file with each initialization.
 */
public class EvalDBWriter {


    private static Logger logger = Logger.getLogger(EvalDBWriter.class);
    private final AtomicLong insertedRows = new AtomicLong();
    private final DBUtil dbUtil;
    private final Long commitEveryX = 1000L;
    private final Connection conn;
    //map of table names with a map of colname/colInfo
    private final Map<String, Map<String, ColInfo>> tableInfo;
    //<tableName, preparedStatement>
    private final Map<String, PreparedStatement> inserts = new HashMap<String, PreparedStatement>();

    public EvalDBWriter(Map<String, Map<String, ColInfo>> tableInfo, DBUtil dbUtil,
                        File dbFile, boolean append) throws Exception {
        this.tableInfo = tableInfo;
        this.dbUtil = dbUtil;

        for (Map.Entry<String, Map<String, ColInfo>> table : tableInfo.entrySet()) {
            Map<String, ColInfo> cols = table.getValue();
            Map<String, ColInfo> sorted = new TreeMap<String,ColInfo>(new ValueComparator(cols));
            sorted.putAll(cols);
            tableInfo.put(table.getKey(), sorted);
        }
        conn = createDB(dbFile, append);
        for (Map.Entry<String, Map<String, ColInfo>> table : tableInfo.entrySet()) {
            List<String> colNames = new ArrayList<String>();
            colNames.addAll(table.getValue().keySet());
            PreparedStatement st = createPreparedInsert(table.getKey(), colNames);
            inserts.put(table.getKey(), st);
        }

    }

    private PreparedStatement createPreparedInsert(String tableName, List<String> colNames) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(tableName);
        sb.append("(");
        int i = 0;
        for (String c : colNames) {
            if (i++ > 0) {
                sb.append(", ");
            }
            sb.append(c);
        }
        sb.append(") ");

        sb.append("VALUES");
        sb.append("(");
        for (int j = 0; j < i; j++) {
            if (j > 0) {
                sb.append(", ");
            }
            sb.append("?");
        }
        sb.append(")");

        return conn.prepareStatement(sb.toString());
    }

    private Connection createDB(File dbFile, boolean append) throws Exception {
        Class.forName(dbUtil.getJDBCDriverClass());

        //if this is a single file type db, first
        //try to delete the actual file.
        if (! append && dbFile.exists() && ! dbFile.isDirectory()) {
            dbFile.delete();
        }
        Connection c = dbUtil.getConnection(dbFile);

        Set<String> tables = dbUtil.getTables(c);

        for (Map.Entry<String, Map<String, ColInfo>> table : tableInfo.entrySet()) {

            if (append && tables.contains(table.getKey().toUpperCase(Locale.ROOT))) {
                continue;
            }
            if (! append) {
                dbUtil.dropTableIfExists(c, table.getKey());
            }
            createTable(c, table.getKey(), table.getValue());

        }


        return c;

    }

    private void createTable(Connection conn, String tableName,
                             Map<String, ColInfo> sortedHeaders) throws SQLException {
        StringBuilder createSql = new StringBuilder();
        createSql.append("CREATE TABLE "+tableName);
        createSql.append("(");
        int i = 0;

        int last = 0;
        for (Map.Entry<String, ColInfo> col : sortedHeaders.entrySet()) {
            if (col.getValue().getDBColOffset()-last != 1) {
                throw new IllegalArgumentException("Columns must be consecutive:" + last + " : " + col.getValue().getDBColOffset());
            }
            last++;
            if (last > 1) {
                createSql.append(", ");
            }
            createSql.append(col.getKey());
            createSql.append(" ");
            createSql.append(col.getValue().getSqlDef());
        }
        createSql.append(")");

        Statement st = conn.createStatement();
        st.execute(createSql.toString());

        st.close();
        conn.commit();

    }

    public void writeRow(String tableName, Map<String, String> data) throws IOException {
        try {
            PreparedStatement p = inserts.get(tableName);
            if (p == null) {
                throw new RuntimeException("Failed to create prepared statement for: "+tableName);
            }
            DBUtil.insert(p, tableInfo.get(tableName), data);
            long rows = insertedRows.incrementAndGet();
            if (rows % commitEveryX == 0) {
                logger.info("writer is committing after "+ rows + " rows");
                conn.commit();
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    public void close() throws IOException {
        if (conn != null) {
            try {
                conn.commit();
            } catch (SQLException e) {
                throw new IOExceptionWithCause(e);
            }

            try {
                conn.close();
            } catch (SQLException e) {
                throw new IOExceptionWithCause(e);
            }

        }
    }



    class ValueComparator implements Comparator<String> {

        Map<String, ColInfo> map;

        public ValueComparator(Map<String, ColInfo> base) {
            this.map = base;
        }

        public int compare(String a, String b) {
            Integer aVal = map.get(a).getDBColOffset();
            Integer bVal = map.get(b).getDBColOffset();
            if (aVal == null || bVal == null) {
                throw new IllegalArgumentException("Column offset must be specified!");
            }
            if (aVal == bVal && ! map.get(a).equals(map.get(b))) {
                throw new IllegalArgumentException("Column offsets must be unique: " + a + " and " + b + " both have: "+aVal);
            }
            if (aVal < bVal) {
                return -1;
            } else {
                return 1;
            }
        }
    }

}
