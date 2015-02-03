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

import org.apache.lucene.util.mutable.MutableValueInt;
import org.apache.tika.batch.FileResource;
import org.apache.tika.batch.fs.FSProperties;
import org.apache.tika.eval.db.ColInfo;
import org.apache.tika.eval.tokens.TokenCounter;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadataList;

public class SingleFileProfiler extends AbstractProfiler {
    static Map<String, ColInfo> headers = new HashMap<String, ColInfo>();

    private final File rootDir;
    private final JsonMetadataList serializer = new JsonMetadataList();

    static {
        for (HEADERS header : AbstractProfiler.HEADERS.values()) {
            addHeader(headers, header);
        }
    }

    public SingleFileProfiler(ArrayBlockingQueue<FileResource> queue, File rootDir) {
        super(queue);
        this.rootDir = rootDir;
    }

    @Override
    public boolean processFileResource(FileResource fileResource) {
        Metadata metadata = fileResource.getMetadata();
        String relativePath = metadata.get(FSProperties.FS_REL_PATH);
        File thisFile = new File(rootDir, relativePath);

        List<Metadata> metadataList = getMetadata(thisFile);
        Map<String, String> output = new HashMap<String, String>();
        output.put(HEADERS.FILE_PATH.name(), relativePath);

        if (metadataList == null) {
            output.put(HEADERS.JSON_EX.name(), "JSON_EXCEPTION");
        }

        output.put(HEADERS.FILE_EXTENSION.name(), getOriginalFileExtension(thisFile.getName()));
        output.put(HEADERS.ELAPSED_TIME_MILLIS.name(), getTime(metadataList));
        output.put(HEADERS.NUM_METADATA_VALUES.name(),
                Integer.toString(countMetadataValues(metadataList)));
        output.put(HEADERS.NUM_ATTACHMENTS.name(), Integer.toString(metadataList.size()-1));
        getFileTypes(metadataList, "", output);

        getExceptionStrings(metadataList, "", output);
        langid(metadataList, "", output);
        SingleFileTokenCounter counter = new SingleFileTokenCounter();
        try {
            countTokens(metadataList, counter);
            handleWordCounts(output, counter, "");
            output.put(HEADERS.NUM_UNIQUE_TOKENS.name(),
                    Integer.toString(counter.getUniqueTokenCount()));
            output.put(HEADERS.TOKEN_COUNT.name(),
                    Integer.toString(counter.getTokenCount()));


        } catch (IOException e) {
            //should log
            e.printStackTrace();
        }
        try {
            writer.writeRow(output);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    public static Map<String, ColInfo> getHeaders() {
        return headers;
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
