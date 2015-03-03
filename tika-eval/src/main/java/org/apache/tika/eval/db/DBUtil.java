package org.apache.tika.eval.db;
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
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.tika.io.IOExceptionWithCause;

public abstract class DBUtil {

    public static Logger logger = Logger.getLogger(DBUtil.class);
    public abstract String getJDBCDriverClass();
    public abstract boolean dropTableIfExists(Connection conn, String tableName) throws SQLException;
    public abstract void shutDownDB(Connection conn) throws IOException;

    /**
     * This is intended for a file/directory based db.
     * <p>
     * Override this any optimizations you want to do on the db
     * before writing/reading.
     *
     * @param dbFile
     * @return
     * @throws IOException
     */
    public Connection getConnection(File dbFile) throws IOException {
        String connectionString = getConnectionString(dbFile);
        Connection conn = null;
        try {
            try {
                Class.forName("org.h2.Driver");
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

}