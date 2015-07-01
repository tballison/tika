package org.apache.tika.eval.io;


import java.io.IOException;
import java.util.Map;

import org.apache.tika.eval.db.Cols;
import org.apache.tika.eval.db.TableInfo;

public interface IDBWriter {
    public void writeRow(TableInfo table, Map<Cols, String> data) throws IOException;
    public void close() throws IOException;
    public int getMimeId(String mimeString);
}
