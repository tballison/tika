package org.apache.tika.eval.batch;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.tika.batch.FileResource;
import org.apache.tika.batch.FileResourceConsumer;
import org.apache.tika.eval.db.ColInfo;
import org.apache.tika.eval.db.DBUtil;

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

    public abstract FileResourceConsumer build() throws IOException;

    protected abstract Map<String, Map<String, ColInfo>> getTableInfo();

    public Map<String, Map<String, ColInfo>> getSortedTableInfo() {
        Map<String, Map<String, ColInfo>> tableInfo = getTableInfo();
        for (Map.Entry<String, Map<String, ColInfo>> table : tableInfo.entrySet()) {
            Map<String, ColInfo> cols = table.getValue();
            TreeMap<String, ColInfo> sorted = new TreeMap<String,ColInfo>(new ValueComparator(cols));
            sorted.putAll(cols);
            tableInfo.put(table.getKey(), Collections.unmodifiableSortedMap(sorted));
        }
        return Collections.unmodifiableMap(tableInfo);
    }

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

}
