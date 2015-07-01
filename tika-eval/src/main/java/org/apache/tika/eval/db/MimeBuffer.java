package org.apache.tika.eval.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.eval.AbstractProfiler;
import org.apache.tika.eval.util.MimeUtil;
import org.apache.tika.mime.MimeTypeException;


public class MimeBuffer extends AbstractDBBuffer {

    private final static String EXTENSION_COLUMN_NAME = "EXTENSION";

    private final PreparedStatement st;
    private final TikaConfig config;

    public MimeBuffer(Connection connection, TikaConfig config) throws SQLException {
        st = connection.prepareStatement("insert into "+ AbstractProfiler.MIME_TABLE.getName() + "( "+
                Cols.MIME_TYPE_ID.name() + ", " +
                Cols.MIME_STRING+", "+
                Cols.FILE_EXTENSION+") values (?,?,?);");
        this.config = config;
    }

    @Override
    public void write(int id, String value) throws RuntimeException {
        try {
            st.clearParameters();
            st.setInt(1, id);
            st.setString(2, value);
            try {
                String ext = MimeUtil.getExtension(value, config);
                if (ext == null || ext.length() == 0) {
                    st.setNull(3, Types.VARCHAR);
                } else {
                    st.setString(3, ext);
                }
            } catch (MimeTypeException e) {
                st.setNull(3, Types.VARCHAR);
            }
            st.execute();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws SQLException {
        st.close();
    }


}
