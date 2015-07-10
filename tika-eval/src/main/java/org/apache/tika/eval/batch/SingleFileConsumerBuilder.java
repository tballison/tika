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
        File extractDir = PropsUtil.getFile(localAttrs.get("extractDir"), null);
        if (extractDir == null) {
            throw new RuntimeException("Must specify \"extractDir\" -- directory to crawl");
        }
        if (!extractDir.isDirectory()) {
            throw new RuntimeException("ROOT DIRECTORY DOES NOT EXIST: " + extractDir.getAbsolutePath());
        }

        File inputDir = PropsUtil.getFile(localAttrs.get("inputDir"), null);

        IDBWriter writer = null;
        try {
            writer = getDBWriter();
        } catch (SQLException ex) {
            throw new IOException(ex);
        }

        //TODO: clean up the writing of the ref tables!!!
        try {
            populateRefTables(writer);
        } catch (SQLException e) {
            throw new RuntimeException("Can't populate ref tables", e);
        }
        return new SingleFileProfiler(queue, inputDir, extractDir, writer);
    }

    @Override
    protected List<TableInfo> getTableInfo() {
        List<TableInfo> tableInfos = new ArrayList<TableInfo>();
        tableInfos.add(AbstractProfiler.MIME_TABLE);
        tableInfos.add(AbstractProfiler.REF_PARSE_ERROR_TYPES);
        tableInfos.add(AbstractProfiler.REF_PARSE_EXCEPTION_TYPES);
        tableInfos.add(AbstractProfiler.REF_EXTRACT_ERROR_TYPES);
        tableInfos.add(SingleFileProfiler.CONTAINER_TABLE);
        tableInfos.add(SingleFileProfiler.PROFILE_TABLE);
        tableInfos.add(SingleFileProfiler.ERROR_TABLE);
        tableInfos.add(SingleFileProfiler.EXCEPTION_TABLE);
        tableInfos.add(SingleFileProfiler.CONTENTS_TABLE);
        tableInfos.add(SingleFileProfiler.EMBEDDED_FILE_PATH_TABLE);
        return tableInfos;
    }

    @Override
    protected IDBWriter getDBWriter() throws IOException, SQLException {
        return new DBWriter(getTableInfo(), TikaConfig.getDefaultConfig(), dbUtil);
    }

    @Override
    protected void addErrorLogTablePairs(DBConsumersManager manager) {
        File errorLog = PropsUtil.getFile(localAttrs.get("errorLogFile"), null);
        System.err.println("ADDING ERROR LOG:"+errorLog);
        if (errorLog == null) {
            return;
        }
        manager.addErrorLogTablePair(errorLog, SingleFileProfiler.ERROR_TABLE);
    }
}
