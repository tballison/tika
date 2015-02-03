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
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.tika.io.IOExceptionWithCause;

public class SqliteUtil extends DBUtil {

    @Override
    public String getJDBCDriverClass() {
        return "org.sqlite.JDBC";
    }

    @Override
    public boolean dropTableIfExists(Connection conn, String tableName) throws SQLException {
        String sql = "drop table if exists "+tableName;
        Statement st = null;
        try {
            st = conn.createStatement();
            return st.execute(sql);
        } finally {
            if (st != null) {
                st.close();
            }
        }
    }

    @Override
    public void shutDownDB(Connection conn) throws IOException {
        System.out.println("SHUTTING DOWN DB in SQLITE UTIL");
        try {
            conn.close();
        } catch (SQLException e) {
            throw new IOExceptionWithCause(e);
        }
    }

    @Override
    public String getConnectionString(File dbFile) {
        return "jdbc:sqlite:"+dbFile.getAbsolutePath();
    }
}
