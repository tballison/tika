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

package org.apache.tika.eval.db;


import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.tika.io.IOExceptionWithCause;

public abstract class DBUtil {

    public static Logger logger = Logger.getLogger(DBUtil.class);
    public abstract String getJDBCDriverClass();
    public abstract boolean dropTableIfExists(Connection conn, String tableName) throws SQLException;
    private final File dbFile;
    public DBUtil(File dbFile) {
        this.dbFile = dbFile;
    }

    /**
     * This is intended for a file/directory based db.
     * <p>
     * Override this any optimizations you want to do on the db
     * before writing/reading.
     *
     * @return
     * @throws IOException
     */
    public Connection getConnection() throws IOException {
        String connectionString = getConnectionString(dbFile);
        Connection conn = null;
        try {
            try {
                Class.forName(getJDBCDriverClass());
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            conn = DriverManager.getConnection(connectionString);
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw new IOExceptionWithCause(e);
        }
        return conn;
    }

    abstract public String getConnectionString(File dbFile);

    /**
     *
     * @param connection
     * @return a list of uppercased table names
     * @throws SQLException
     */
    abstract public Set<String> getTables(Connection connection) throws SQLException;

    public static int insert(PreparedStatement insertStatement,
                              Map<String, ColInfo> columns,
                              Map<String, String> data) throws SQLException {

        //clear parameters before setting
        insertStatement.clearParameters();
        try {
            for (Map.Entry<String, ColInfo> e : columns.entrySet()) {

                updateInsertStatement(insertStatement, e.getKey(), e.getValue(), data.get(e.getKey()));
            }
            return insertStatement.executeUpdate();
        } catch (SQLException e) {
            logger.warn("couldn't insert data for this row: "+e.getMessage());
            return -1;
        }
    }

    public static void updateInsertStatement(PreparedStatement st, String colName,
                                                ColInfo colInfo, String value ) throws SQLException {
        if (value == null) {
            st.setNull(colInfo.getDBColOffset(), colInfo.getType());
            return;
        }
        try {
            switch (colInfo.getType()) {
                case Types.VARCHAR:
                    if (value != null && value.length() > colInfo.getPrecision()) {
                        value = value.substring(0, colInfo.getPrecision());
                        logger.info("truncated varchar value in " + colName);
                    }
                    st.setString(colInfo.getDBColOffset(), value);
                    break;
                case Types.DOUBLE:
                    st.setDouble(colInfo.getDBColOffset(), Double.parseDouble(value));
                    break;
                case Types.FLOAT:
                    st.setDouble(colInfo.getDBColOffset(), Float.parseFloat(value));
                    break;
                case Types.INTEGER:
                    st.setDouble(colInfo.getDBColOffset(), Integer.parseInt(value));
                    break;
                case Types.BIGINT:
                    st.setLong(colInfo.getDBColOffset(), Long.parseLong(value));
                    break;
                case Types.BOOLEAN:
                    st.setBoolean(colInfo.getDBColOffset(), Boolean.parseBoolean(value));
                    break;
                default:
                    throw new UnsupportedOperationException("Don't yet support type: " + colInfo.getType());
            }
        } catch (NumberFormatException e) {
            logger.warn("number format exception: "+colName+ " : " + value);
            st.setNull(colInfo.getDBColOffset(), colInfo.getType());
        } catch (SQLException e) {
            logger.warn("sqlexception: "+colName+ " : " + value);
            st.setNull(colInfo.getDBColOffset(), colInfo.getType());
        }
    }

    public void createDB(Map<String, Map<String, ColInfo>> tableInfo,
                         Map<String, String> indexInfo, boolean append) throws SQLException, IOException {
        Connection conn = getConnection();
        Set<String> tables = getTables(conn);

        for (Map.Entry<String, Map<String, ColInfo>> table : tableInfo.entrySet()) {

            if (append && tables.contains(table.getKey().toUpperCase(Locale.ROOT))) {
                continue;
            }
            if (! append) {
                dropTableIfExists(conn, table.getKey());
            }
            createTable(conn, table.getKey(), table.getValue());
        }
        if (indexInfo != null) {
            Statement st = conn.createStatement();
            for (Map.Entry<String, String> e : indexInfo.entrySet()) {
                String sql = "CREATE INDEX "+e.getKey()+"_idx " +
                        "on " + e.getKey() + "("+e.getValue()+")";
                st.execute(sql);
            }
        }
        conn.commit();
        conn.close();
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
            String constraints = col.getValue().getConstraints();
            if (constraints != null) {
                createSql.append(" ");
                createSql.append(constraints);
            }
        }
        createSql.append(")");
        Statement st = conn.createStatement();
        st.execute(createSql.toString());

        st.close();
        conn.commit();
    }


}
