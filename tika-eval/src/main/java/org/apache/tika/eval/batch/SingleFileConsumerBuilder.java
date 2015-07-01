package org.apache.tika.eval.batch;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.tika.batch.FileResourceConsumer;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.eval.AbstractProfiler;
import org.apache.tika.eval.SingleFileProfiler;
import org.apache.tika.eval.db.TableInfo;
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
        IDBWriter writer = null;
        try {
            writer = getDBWriter();
        } catch (SQLException ex) {
            throw new IOException(ex);
        }
        try {
            populateRefTables();
        } catch (SQLException e) {
            throw new RuntimeException("Can't populate ref tables", e);
        }

        return new SingleFileProfiler(queue, crawlingInputDir, rootDir, writer);
    }

    @Override
    protected List<TableInfo> getTableInfo() {
        List<TableInfo> tableInfos = new ArrayList<TableInfo>();
        tableInfos.add(AbstractProfiler.MIME_TABLE);
        tableInfos.add(AbstractProfiler.REF_ERROR_TYPES);
        tableInfos.add(AbstractProfiler.REF_EXCEPTION_TYPES);
        tableInfos.add(SingleFileProfiler.CONTAINER_TABLE);
        tableInfos.add(SingleFileProfiler.PROFILE_TABLE);
        tableInfos.add(SingleFileProfiler.EXCEPTION_TABLE);
        tableInfos.add(SingleFileProfiler.ERROR_TABLE);
        tableInfos.add(SingleFileProfiler.CONTENTS_TABLE);
        return tableInfos;
    }

    @Override
    protected IDBWriter getDBWriter() throws IOException, SQLException {
        return new DBWriter(getTableInfo(), TikaConfig.getDefaultConfig(), dbUtil);
    }
}
