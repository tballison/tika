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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.util.mutable.MutableValueInt;
import org.apache.tika.batch.FileResource;
import org.apache.tika.batch.fs.FSProperties;
import org.apache.tika.eval.db.Cols;
import org.apache.tika.eval.io.DBWriter;
import org.apache.tika.eval.tokens.TokenCounter;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.RecursiveParserWrapper;

public class SingleFileProfiler extends AbstractProfiler {

    private final File rootDir;

    public SingleFileProfiler(ArrayBlockingQueue<FileResource> queue,
                              boolean crawlingInputDir, File rootDir,
                              DBWriter dbWriter) {
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

        Map<Cols, String> output = new HashMap<Cols, String>();
        Map<Cols, String> excOutput = new HashMap<Cols, String>();

        int i = 0;
        for (Metadata m : metadataList) {
            output.clear();
            excOutput.clear();
            String fileId = Integer.toString(ID.incrementAndGet());

            output.put(Cols.ID, fileId);
            output.put(Cols.CONTAINER_ID, containerId);

            output.put(Cols.ELAPSED_TIME_MILLIS, getTime(m));
            output.put(Cols.NUM_METADATA_VALUES,
                    Integer.toString(countMetadataValues(m)));

            //if the outer wrapper document
            if (i == 0) {
                output.put(Cols.IS_EMBEDDED, FALSE);
                output.put(Cols.FILE_EXTENSION,
                        getOriginalFileExtension(thisFile.getName()));
            } else {
                output.put(Cols.IS_EMBEDDED, TRUE);
                output.put(Cols.EMBEDDED_FILE_PATH,
                        m.get(RecursiveParserWrapper.EMBEDDED_RESOURCE_PATH));
                output.put(Cols.FILE_EXTENSION,
                        FilenameUtils.getExtension(m.get(RecursiveParserWrapper.EMBEDDED_RESOURCE_PATH)));
            }
            getFileTypes(m, output);

            getExceptionStrings(m, excOutput);
            langid(m, output);
            SingleFileTokenCounter counter = new SingleFileTokenCounter();
            try {
                countTokens(m, counter);
                handleWordCounts(output, counter);
                output.put(Cols.NUM_UNIQUE_TOKENS,
                        Integer.toString(counter.getUniqueTokenCount()));
                output.put(Cols.TOKEN_COUNT,
                        Integer.toString(counter.getTokenCount()));
            } catch (IOException e) {
                //should log
                e.printStackTrace();
            }
            try {
                writer.writeRow(PROFILE_TABLE, output);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            i++;
        }
        return true;
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
