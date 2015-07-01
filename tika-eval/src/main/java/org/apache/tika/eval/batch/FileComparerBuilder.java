package org.apache.tika.eval.batch;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.tika.batch.FileResourceConsumer;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.eval.AbstractProfiler;
import org.apache.tika.eval.FileComparer;
import org.apache.tika.eval.db.TableInfo;
import org.apache.tika.eval.io.DBWriter;
import org.apache.tika.eval.io.IDBWriter;
import org.apache.tika.util.PropsUtil;

public class FileComparerBuilder extends EvalConsumerBuilder {
    private final static String WHICH_DB = "h2";//TODO: allow flexibility


    @Override
    public FileResourceConsumer build() throws IOException, SQLException {
        File thisRootDir = PropsUtil.getFile(localAttrs.get("thisDir"), null);
        if (thisRootDir == null) {
            throw new RuntimeException("Must specify \"thisDir\" -- directory to crawl");
        }
        File thatRootDir = PropsUtil.getFile(localAttrs.get("thatDir"), null);
        if (thatRootDir == null) {
            throw new RuntimeException("Must specify \"thatDir\" -- directory to crawl");
        }

        boolean crawlingInputDir = PropsUtil.getBoolean(localAttrs.get("crawlingInputDir"), false);

        long minJsonLength = PropsUtil.getLong(localAttrs.get("minJsonFileSizeBytes"), -1L);
        long maxJsonLength = PropsUtil.getLong(localAttrs.get("maxJsonFileSizeBytes"), -1L);
        IDBWriter writer = getDBWriter();
        return new FileComparer(queue, thisRootDir, thatRootDir, crawlingInputDir, writer,
                minJsonLength, maxJsonLength);
    }

    @Override
    protected List<TableInfo> getTableInfo() {
        List<TableInfo> tableInfo = new ArrayList<TableInfo>();
        tableInfo.add(FileComparer.COMPARISON_CONTAINERS);
        tableInfo.add(FileComparer.PROFILES_A);
        tableInfo.add(FileComparer.PROFILES_B);
        tableInfo.add(FileComparer.ERRORS_A);
        tableInfo.add(FileComparer.ERRORS_B);
        tableInfo.add(FileComparer.EXCEPTIONS_A);
        tableInfo.add(FileComparer.EXCEPTIONS_B);
        tableInfo.add(FileComparer.CONTENTS_TABLE_A);
        tableInfo.add(FileComparer.CONTENTS_TABLE_B);
        tableInfo.add(FileComparer.EMBEDDED_FILE_PATH_TABLE_A);
        tableInfo.add(FileComparer.EMBEDDED_FILE_PATH_TABLE_B);

        tableInfo.add(FileComparer.CONTENT_COMPARISONS);
        tableInfo.add(FileComparer.REF_PAIR_NAMES);
        tableInfo.add(AbstractProfiler.MIME_TABLE);
        tableInfo.add(AbstractProfiler.REF_ERROR_TYPES);
        tableInfo.add(AbstractProfiler.REF_EXCEPTION_TYPES);
        return tableInfo;
    }

    @Override
    protected IDBWriter getDBWriter() throws IOException, SQLException {
        return new DBWriter(getTableInfo(), TikaConfig.getDefaultConfig(), dbUtil);
    }
}
