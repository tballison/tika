package org.apache.tika.batch.db;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Date;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.tika.batch.db.utils.DBUtil;
import org.junit.Test;

/**
 * Created by TALLISON on 2/17/2016.
 */
public class FSToDBFileInserterTest {
    @Test
    public void simpleIntegrationTest() throws Exception {
        Path root = Paths.get("C:\\data\\USDA\\FSAs\\fsa_batches");
        String connectionString = "jdbc:h2:mem:testdb";
        Class.forName("org.h2.Driver");
        Connection connection = DriverManager.
                getConnection(connectionString, "sa", "");

        DBUtil.createTable(connection, Tables.ROOTS);
        DBUtil.createTable(connection, Tables.REL_PATHS);
        DBUtil.createTable(connection, Tables.FILE_STATUS);
        DBUtil.createTable(connection, Tables.FLAT_TABLE);
        long started = new Date().getTime();
        FSToDBFileInserterC inserter = new FSToDBFileInserterC(root, connection, false);
        ExecutorService pool = Executors.newFixedThreadPool(1);
        ExecutorCompletionService service = new ExecutorCompletionService(pool);
        service.submit(inserter);

        service.take();
        System.out.println(new Date().getTime()-started + " ms");
        System.out.println("done");
        String sql = "select * from "+Tables.FLAT_TABLE.getTableName();
        ResultSet rs = connection.prepareStatement(sql).executeQuery();
        ResultSetMetaData m = rs.getMetaData();
        int c = 0;
        while (rs.next()) {
            for (int i = 1; i <= m.getColumnCount(); i++) {
                //System.out.print(rs.getString(i)+"\t");
            }
            c++;//
//            System.out.println("");
        }
        System.out.println(c + " records");

    }
}
