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
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.apache.tika.batch.FileResourceConsumer;
import org.apache.tika.eval.AbstractProfiler;
import org.apache.tika.eval.FileComparer;
import org.apache.tika.eval.db.ColInfo;
import org.apache.tika.eval.io.DBWriter;
import org.apache.tika.eval.io.IDBWriter;
import org.apache.tika.util.PropsUtil;

public class FileComparerBuilder extends EvalConsumerBuilder {
    private final static String WHICH_DB = "h2";//TODO: allow flexibility


    @Override
    public FileResourceConsumer build() throws IOException {
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
        IDBWriter writer = new DBWriter(getSortedTableInfo(), dbUtil);
        return new FileComparer(queue, thisRootDir, thatRootDir, crawlingInputDir, writer,
                minJsonLength, maxJsonLength);
    }

    @Override
    protected Map<String, Map<String, ColInfo>> getTableInfo() {
        Map<String, Map<String, ColInfo>> tableInfo = new HashMap<String, Map<String, ColInfo>>();
        tableInfo.put(FileComparer.COMPARISONS_TABLE, FileComparer.getHeaders());
        tableInfo.put(FileComparer.PAIR_NAMES_TABLE, getPairNamesCols());
        tableInfo.put(FileComparer.EXCEPTIONS_TABLE + FileComparer.thisExtension,
                AbstractProfiler.getExceptionHeaders());
        tableInfo.put(FileComparer.EXCEPTIONS_TABLE + FileComparer.thatExtension,
                AbstractProfiler.getExceptionHeaders());
        return tableInfo;
    }

    @Override
    public Map<String, String> getIndexInfo() {
        Map<String, String> indices = new HashMap<String, String>();
        indices.put(FileComparer.COMPARISONS_TABLE, AbstractProfiler.HEADERS.ID.name());
        indices.put(FileComparer.EXCEPTIONS_TABLE + FileComparer.thisExtension,
                AbstractProfiler.HEADERS.ID.name());
        indices.put(FileComparer.EXCEPTIONS_TABLE+ FileComparer.thatExtension,
                AbstractProfiler.HEADERS.ID.name());
        return indices;
    }


    public Map<String, ColInfo> getPairNamesCols() {
        Map<String, ColInfo> m = new HashMap<String, ColInfo>();
        m.put("DIR_NAME_A", new ColInfo(1, Types.VARCHAR, 128));
        m.put("DIR_NAME_B", new ColInfo(2, Types.VARCHAR, 128));
        return m;
    }

}
