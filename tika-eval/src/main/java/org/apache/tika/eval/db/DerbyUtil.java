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
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

/**
 * Built during development.  This will likely go away soon.
 */
public class DerbyUtil extends DBUtil {

    @Override
    public String getJDBCDriverClass() {
        return "org.apache.derby.jdbc.EmbeddedDriver";

    }

    @Override
    public String getConnectionString(File dbFile) {
        return "jdbc:derby:"+dbFile.getPath()+";create=true";
    }

    @Override
    public boolean dropTableIfExists(Connection conn, String tableName) throws SQLException {
        if (!tableExists(conn, tableName)) {
            return false;
        }
        StringBuilder sql = new StringBuilder();
        sql.append("DROP TABLE ").append(tableName);
        Statement st = conn.createStatement();
        st.execute(sql.toString());
        st.close();
        conn.commit();
        return true;
    }


    private boolean tableExists(Connection conn, String tableName) throws SQLException {
        Statement st = conn.createStatement();
        DatabaseMetaData dbmeta = conn.getMetaData();
        int numRows = 0;

        ResultSet rs = dbmeta.getTables( null, "APP", tableName.toUpperCase(Locale.ROOT), null);
        while( rs.next() ) ++numRows;
        rs.close();
        st.close();
        return numRows > 0;

    }

    @Override
    public void shutDownDB(Connection conn) throws IOException {
        try {
            conn.close();
            DriverManager.getConnection("jdbc:derby:;shutdown=true");
        } catch (SQLException e) {
            Throwable cause = (Throwable)e;
            while (cause != null){
                if (cause.toString().startsWith("ERROR XJ015")) {//derby code for clean shutdown, naturally
                    return;
                }
                cause = cause.getCause();
            }
            throw new IOException(e);
        }

    }
}
