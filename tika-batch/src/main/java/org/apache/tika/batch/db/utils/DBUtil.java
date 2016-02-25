package org.apache.tika.batch.db.utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by TALLISON on 2/17/2016.
 */
public class DBUtil {
    public static void createTable(Connection connection, TableDef tableDef)
            throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute(tableDef.getCreateSql());
    }
}
