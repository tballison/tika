package org.apache.tika.eval.io;


import java.io.IOException;
import java.util.Map;

import org.apache.tika.eval.db.TableInfo;

public interface IDBWriter {
    public void writeRow(TableInfo table, Map<String, String> data);
    public void close() throws IOException;
}
