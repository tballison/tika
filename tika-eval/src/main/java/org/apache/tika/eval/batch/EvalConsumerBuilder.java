package org.apache.tika.eval.batch;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.tika.batch.FileResource;
import org.apache.tika.batch.FileResourceConsumer;
import org.apache.tika.eval.AbstractProfiler;
import org.apache.tika.eval.db.Cols;
import org.apache.tika.eval.db.DBUtil;
import org.apache.tika.eval.db.TableInfo;
import org.apache.tika.eval.io.IDBWriter;

public abstract class EvalConsumerBuilder {

    protected ArrayBlockingQueue<FileResource> queue;
    Map<String, String> localAttrs;
    DBUtil dbUtil;

    public void init(ArrayBlockingQueue<FileResource> queue, Map<String, String> localAttrs,
                     DBUtil dbUtil) {
        this.queue = queue;
        this.localAttrs = localAttrs;
        this.dbUtil = dbUtil;
    }

    public abstract FileResourceConsumer build() throws IOException, SQLException;

    protected abstract List<TableInfo> getTableInfo();

    protected abstract IDBWriter getDBWriter() throws IOException, SQLException;

    public void populateRefTables() throws IOException, SQLException {
        IDBWriter writer = getDBWriter();
        Map<Cols, String> m = new HashMap<Cols, String>();
        for (AbstractProfiler.ERROR_TYPE t : AbstractProfiler.ERROR_TYPE.values()) {
            m.clear();
            m.put(Cols.ERROR_TYPE_ID, Integer.toString(t.ordinal()));
            m.put(Cols.ERROR_DESCRIPTION, t.name());
            writer.writeRow(AbstractProfiler.REF_ERROR_TYPES, m);
        }

        for (AbstractProfiler.EXCEPTION_TYPE t : AbstractProfiler.EXCEPTION_TYPE.values()) {
            m.clear();
            m.put(Cols.EXCEPTION_TYPE_ID, Integer.toString(t.ordinal()));
            m.put(Cols.EXCEPTION_DESCRIPTION, t.name());
            writer.writeRow(AbstractProfiler.REF_EXCEPTION_TYPES, m);
        }
    }

/*
    public abstract Map<String, String> getIndexInfo();

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
*/
}
