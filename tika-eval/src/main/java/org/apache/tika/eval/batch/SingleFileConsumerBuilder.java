package org.apache.tika.eval.batch;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.tika.batch.FileResourceConsumer;
import org.apache.tika.eval.AbstractProfiler;
import org.apache.tika.eval.SingleFileProfiler;
import org.apache.tika.eval.db.ColInfo;
import org.apache.tika.eval.io.DBWriter;
import org.apache.tika.eval.io.IDBWriter;
import org.apache.tika.util.PropsUtil;


public class SingleFileConsumerBuilder extends EvalConsumerBuilder {

    @Override
    public FileResourceConsumer build() throws IOException {
        boolean crawlingInputDir = PropsUtil.getBoolean(localAttrs.get("crawlingInputDir"), false);
        File rootDir = PropsUtil.getFile(localAttrs.get("thisDir"), null);
        if (rootDir == null) {
            throw new RuntimeException("Must specify \"thisDir\" -- directory to crawl");
        }
        if (!rootDir.isDirectory()) {
            throw new RuntimeException("ROOT DIRECTORY DOES NOT EXIST: " + rootDir.getAbsolutePath());
        }
        IDBWriter writer = new DBWriter(getSortedTableInfo(), dbUtil);
        return new SingleFileProfiler(queue, crawlingInputDir, rootDir, writer);
    }

    @Override
    protected Map<String, Map<String, ColInfo>> getTableInfo() {
        Map<String, Map<String, ColInfo>> tableInfo = new HashMap<String, Map<String, ColInfo>>();
        tableInfo.put(SingleFileProfiler.MAIN_TABLE, SingleFileProfiler.getHeaders());
        tableInfo.put(AbstractProfiler.EXCEPTIONS_TABLE, AbstractProfiler.getExceptionHeaders());
        return tableInfo;
    }

    @Override
    public Map<String, String> getIndexInfo() {
        Map<String, String> indices = new HashMap<String, String>();
        indices.put(SingleFileProfiler.MAIN_TABLE, AbstractProfiler.HEADERS.ID.name());
        indices.put(AbstractProfiler.EXCEPTIONS_TABLE, AbstractProfiler.HEADERS.ID.name());
        return indices;

    }

}
