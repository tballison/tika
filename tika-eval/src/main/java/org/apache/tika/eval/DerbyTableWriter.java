package org.apache.tika.eval;

import org.apache.tika.eval.db.ColInfo;
import org.apache.tika.eval.db.DBUtil;
import org.apache.tika.eval.db.DerbyUtil;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

public class DerbyTableWriter implements TableWriter {
    private final AtomicLong insertedRows = new AtomicLong();
    private final Long commitEveryX = 1000L;
    private final Connection conn;
    private final PreparedStatement preparedInsert;
    private final Map<String, ColInfo> sortedHeaders;

    public DerbyTableWriter(Map<String, ColInfo> headers, File dbFile, String tableName) throws Exception {
        this.sortedHeaders = new TreeMap<String, ColInfo>(new ValueComparator(headers));
        sortedHeaders.putAll(headers);

        conn = createDB(dbFile, tableName);
        preparedInsert = createPreparedInsert(tableName);
    }

    private PreparedStatement createPreparedInsert(String tableName) throws Exception {
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
        String driver = "org.apache.derby.jdbc.EmbeddedDriver";
        Class.forName(driver);
        String url = "jdbc:derby:"+dbFile.getPath()+";create=true";
        Connection c = DriverManager.getConnection(url);
        DerbyUtil.dropTableIfExists(c, tableName);


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

        System.out.println(createSql);
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
            DBUtil.insert(preparedInsert, sortedHeaders, data);
            long rows = insertedRows.incrementAndGet();
            if (rows % commitEveryX == 0) {
                System.out.println("Committing: "+rows);
                conn.commit();
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        DerbyUtil.shutDownDB(conn);
    }

    @Override
    public void shutdown() {
        //no-op
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
