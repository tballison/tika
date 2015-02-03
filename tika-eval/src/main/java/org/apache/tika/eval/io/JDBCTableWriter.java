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
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.tika.eval.db.ColInfo;
import org.apache.tika.eval.db.DBUtil;
import org.apache.tika.io.IOExceptionWithCause;

/**
 * This is still in its early stages.  The idea is to
 * get something working with sqlite/derby and then add to that
 * as necessary.
 */
public class JDBCTableWriter implements TableWriter {

    public static final String PAIR_NAMES_TABLE = "pair_names";

    private final AtomicLong insertedRows = new AtomicLong();
    private final DBUtil dbUtil;
    private final Long commitEveryX = 1000L;
    private final Connection conn;
    private final Map<String, ColInfo> sortedHeaders;
    private final String tableName;

    public JDBCTableWriter(Map<String, ColInfo> headers, DBUtil dbUtil,
                           File dbFile, String tableName) throws Exception {
        this.sortedHeaders = new TreeMap<String, ColInfo>(new ValueComparator(headers));
        sortedHeaders.putAll(headers);
        this.dbUtil = dbUtil;
        conn = createDB(dbFile, tableName);
        this.tableName = tableName;
    }

    private PreparedStatement createPreparedInsert(String tableName) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(tableName);
        sb.append("(");
        int i = 0;
        for (String k : sortedHeaders.keySet()) {
            if (i++ > 0) {
                sb.append(", ");
            }
            sb.append(k);
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

    private Connection createDB(File dbFile, String tableName) throws Exception {
        Class.forName(dbUtil.getJDBCDriverClass());
        Connection c = dbUtil.getConnection(dbFile);
        dbUtil.dropTableIfExists(c, tableName);


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

        Statement st = c.createStatement();
        st.execute(createSql.toString());
        st.close();
        c.commit();
        return c;

    }

    @Override
    public void writeHeaders() {
        //no-op
    }

    @Override
    public void init() throws IOException {
        //no-op for now
    }

    @Override
    public void writeRow(Map<String, String> data) throws IOException {
        try {
            PreparedStatement p = createPreparedInsert(tableName);
            DBUtil.insert(p, sortedHeaders, data);
            long rows = insertedRows.incrementAndGet();
            if (rows % commitEveryX == 0) {
                //System.out.println("Committing: "+rows);
                conn.commit();
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (conn != null) {
            try {
                conn.commit();
            } catch (SQLException e) {
                throw new IOExceptionWithCause(e);
            }
            dbUtil.shutDownDB(conn);
        }
    }

    @Override
    public void shutdown() {
        //no-op
    }

    public void addPairTable(String thisDir, String thatDir) throws IOException {
        String sql = "DROP TABLE IF EXISTS "+PAIR_NAMES_TABLE;

        try {
            Statement st = conn.createStatement();
            st.execute(sql);
            sql = "CREATE table " +PAIR_NAMES_TABLE +" (" +
                    "DIR_NAME_A VARCHAR(128)," +
                    "DIR_NAME_B VARCHAR(128));";

            st.execute(sql);
            conn.commit();
            sql = "INSERT INTO "+PAIR_NAMES_TABLE+
                    " VALUES ('"+thisDir+"', '"+thatDir+"');";
            st.execute(sql);
            conn.commit();
            st.close();
        } catch (SQLException e) {
            throw new IOExceptionWithCause(e);
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
