package org.apache.tika.eval.db;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FilenameUtils;

/**
 * Created by tallison on 2/4/2015.
 */
public class H2Util extends DBUtil {
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
