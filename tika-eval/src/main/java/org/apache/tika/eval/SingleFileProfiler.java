package org.apache.tika.eval;

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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.lucene.util.mutable.MutableValueInt;
import org.apache.tika.batch.FileResource;
import org.apache.tika.batch.fs.FSProperties;
import org.apache.tika.eval.db.ColInfo;
import org.apache.tika.eval.db.Cols;
import org.apache.tika.eval.db.TableInfo;
import org.apache.tika.eval.io.IDBWriter;
import org.apache.tika.eval.tokens.TokenCounter;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.RecursiveParserWrapper;

public class SingleFileProfiler extends AbstractProfiler {


    public static TableInfo ERROR_TABLE = new TableInfo("errors",
            new ColInfo(Cols.CONTAINER_ID, Types.INTEGER, "PRIMARY KEY"),
            new ColInfo(Cols.ERROR_TYPE_ID, Types.INTEGER),
            new ColInfo(Cols.JSON_EX, Types.BOOLEAN)
    );

    public static TableInfo EXCEPTION_TABLE = new TableInfo("exceptions",
            new ColInfo(Cols.ID, Types.INTEGER, "PRIMARY KEY"),
            new ColInfo(Cols.ORIG_STACK_TRACE, Types.VARCHAR, 8192),
            new ColInfo(Cols.SORT_STACK_TRACE, Types.VARCHAR, 8192),
            new ColInfo(Cols.EXCEPTION_TYPE_ID, Types.INTEGER)
    );


    public static TableInfo CONTAINER_TABLE = new TableInfo("containers",
            new ColInfo(Cols.CONTAINER_ID, Types.INTEGER, "PRIMARY KEY"),
            new ColInfo(Cols.FILE_PATH, Types.VARCHAR, 512),
            new ColInfo(Cols.LENGTH, Types.BIGINT),
            new ColInfo(Cols.EXTRACT_FILE_LENGTH, Types.BIGINT)
    );

    public static TableInfo PROFILE_TABLE = new TableInfo("profiles",
            new ColInfo(Cols.ID, Types.INTEGER, "PRIMARY KEY"),
            new ColInfo(Cols.CONTAINER_ID, Types.INTEGER),//, "FOREIGN KEY"),
            new ColInfo(Cols.MD5, Types.CHAR, 32),
            new ColInfo(Cols.LENGTH, Types.BIGINT),
            new ColInfo(Cols.IS_EMBEDDED, Types.BOOLEAN),
            new ColInfo(Cols.FILE_EXTENSION, Types.VARCHAR, 12),
            new ColInfo(Cols.MIME_TYPE_ID, Types.INTEGER),
            new ColInfo(Cols.ELAPSED_TIME_MILLIS, Types.INTEGER),
            new ColInfo(Cols.NUM_ATTACHMENTS, Types.INTEGER),
            new ColInfo(Cols.NUM_METADATA_VALUES, Types.INTEGER),
            new ColInfo(Cols.HAS_CONTENT, Types.BOOLEAN)
    );

    public static TableInfo EMBEDDED_FILE_PATH_TABLE = new TableInfo("emb_file_names",
            new ColInfo(Cols.ID, Types.INTEGER, "PRIMARY KEY"),
            new ColInfo(Cols.EMBEDDED_FILE_PATH, Types.VARCHAR, 1024)
    );

    public static TableInfo CONTENTS_TABLE = new TableInfo("contents",
            new ColInfo(Cols.ID, Types.INTEGER, "PRIMARY KEY"),
            new ColInfo(Cols.CONTENT_LENGTH, Types.INTEGER),
            new ColInfo(Cols.TOKEN_COUNT, Types.INTEGER),
            new ColInfo(Cols.UNIQUE_TOKEN_COUNT, Types.INTEGER),
            new ColInfo(Cols.TOP_N_WORDS, Types.VARCHAR, 1024),
            new ColInfo(Cols.NUM_EN_STOPS_TOP_N, Types.INTEGER),
            new ColInfo(Cols.LANG_ID_1, Types.VARCHAR, 12),
            new ColInfo(Cols.LANG_ID_PROB_1, Types.FLOAT),
            new ColInfo(Cols.LANG_ID_2, Types.VARCHAR, 12),
            new ColInfo(Cols.LANG_ID_PROB_2, Types.FLOAT),
            new ColInfo(Cols.TOKEN_ENTROPY_RATE, Types.FLOAT),
            new ColInfo(Cols.TOKEN_LENGTH_SUM, Types.INTEGER),
            new ColInfo(Cols.TOKEN_LENGTH_MEAN, Types.FLOAT),
            new ColInfo(Cols.TOKEN_LENGTH_STD_DEV, Types.FLOAT)
    );

    private final File rootDir;

    public SingleFileProfiler(ArrayBlockingQueue<FileResource> queue,
                              boolean crawlingInputDir, File rootDir,
                              IDBWriter dbWriter) {
        super(queue, crawlingInputDir, dbWriter);
        this.rootDir = rootDir;
    }

    @Override
    public boolean processFileResource(FileResource fileResource) {
        Metadata metadata = fileResource.getMetadata();
        String relativePath = metadata.get(FSProperties.FS_REL_PATH);
        File thisFile = new File(rootDir, relativePath);
        System.err.println("trying to get: "+thisFile.getAbsolutePath());
        List<Metadata> metadataList = getMetadata(thisFile);

        Map<Cols, String> contOutput = new HashMap<Cols, String>();
        String containerId = Integer.toString(CONTAINER_ID.incrementAndGet());

        contOutput.put(Cols.CONTAINER_ID, containerId);
        contOutput.put(Cols.FILE_PATH, relativePath);
        contOutput.put(Cols.EXTRACT_FILE_LENGTH,
                Long.toString(thisFile.length()));
        try {
            writer.writeRow(CONTAINER_TABLE, contOutput);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (metadataList == null) {
            Map<Cols, String> errOutput = new HashMap<Cols,String>();
            errOutput.put(Cols.CONTAINER_ID, containerId);
            errOutput.put(Cols.JSON_EX, "TRUE");
            try {
                writer.writeRow(ERROR_TABLE, errOutput);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return true;
        }

        //TODO: calculate num_attachments, add to profile table
        int i = 0;
        for (Metadata m : metadataList) {
            String fileId = Integer.toString(ID.incrementAndGet());
            writeProfileData(thisFile, i, fileId, containerId, m, PROFILE_TABLE);
            writeEmbeddedPathData(i, fileId, m, EMBEDDED_FILE_PATH_TABLE);
            writeExceptionData(fileId, m, EXCEPTION_TABLE);
            SingleFileTokenCounter counter = new SingleFileTokenCounter();
            writeContentData(fileId, m, counter, CONTENTS_TABLE);
            i++;
        }
        return true;
    }

    private void writeEmbeddedPathData(int i, String fileId, Metadata m,
                                       TableInfo embeddedFilePathTable) {
        if (i == 0) {
            return;
        }
        Map<Cols, String> data = new HashMap<Cols, String>();
        data.put(Cols.ID, fileId);
        data.put(Cols.EMBEDDED_FILE_PATH,
                m.get(RecursiveParserWrapper.EMBEDDED_RESOURCE_PATH));
        try {
            writer.writeRow(embeddedFilePathTable, data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    class SingleFileTokenCounter extends TokenCounter {
        private final Map<String, MutableValueInt> m = new HashMap<String, MutableValueInt>();


        @Override
        public void increment (String s){
            MutableValueInt i = m.get(s);
            if (i == null) {
                i = new MutableValueInt();
                i.value = 0;
            }
            incrementOverallCounts(i.value);
            i.value++;
            m.put(s, i);
        }

        @Override
        public Collection<String> getTokens() {
            return m.keySet();
        }

        @Override
        public int getCount(String token) {
            MutableValueInt i = m.get(token);
            if (i == null) {
                return 0;
            }
            return i.value;
        }
    }

}
