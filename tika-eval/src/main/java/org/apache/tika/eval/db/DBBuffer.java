package org.apache.tika.eval.db;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DBBuffer extends AbstractDBBuffer {

    private final PreparedStatement st;

    public DBBuffer(Connection connection, String tableName,
                    String idColumnName, String valueColumnName) throws SQLException {
        st = connection.prepareStatement("insert into "+tableName+ "( "+
                idColumnName + ", " + valueColumnName+") values (?,?);");
    }

    @Override
    public void write(int id, String value) throws RuntimeException {
        try {
            st.clearParameters();
            st.setInt(1, id);
            st.setString(2, value);
            boolean ex = st.execute();
            if (ex == false) {
                throw new RuntimeException("failed to write in DBBuffer");
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws SQLException {
        st.close();

    }


}
