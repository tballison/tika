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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FilenameUtils;


public class H2Util extends DBUtil {

    public H2Util(File dbFile) {
        super(dbFile);
    }

    @Override
    public String getJDBCDriverClass() {
        return "org.h2.Driver";
    }

    @Override
    public boolean dropTableIfExists(Connection conn, String tableName) throws SQLException {
        Statement st = conn.createStatement();
        String sql = "drop table if exists "+tableName;
        boolean success = st.execute(sql);
        st.close();
        return success;
    }

    @Override
    public String getConnectionString(File dbFile) {
        return "jdbc:h2:"+ FilenameUtils.separatorsToUnix(dbFile.getAbsolutePath());
    }

    @Override
    public Set<String> getTables(Connection connection) throws SQLException {
        String sql = "SHOW TABLES";
        Statement st = connection.createStatement();
        ResultSet rs = st.executeQuery(sql);
        Set<String> tables = new HashSet<String>();
        while (rs.next()) {
            String table = rs.getString(1);
            tables.add(table);
        }
        return tables;
    }
}
