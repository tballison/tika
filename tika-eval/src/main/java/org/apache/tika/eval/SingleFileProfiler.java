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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.lucene.util.mutable.MutableValueInt;
import org.apache.tika.batch.FileResource;
import org.apache.tika.batch.fs.FSProperties;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadataList;

public class SingleFileProfiler extends AbstractProfiler {

    private final File rootDir;
    private final JsonMetadataList serializer = new JsonMetadataList();
    TableWriter writer = null;
    private List<String> localHeaders = null;

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
        if (metadataList == null) {
            output.put(HEADERS.JSON_EX.name(), "JSON_EXCEPTION");
        }

        String content = getContent(metadataList);
        output.put(HEADERS.FILE_EXTENSION.name(), getOriginalFileExtension(thisFile.getName()));
        output.put(HEADERS.ELAPSED_TIME_MILLIS.name(), getTime(metadataList));
        output.put(HEADERS.NUM_METADATA_VALUES.name(),
                Integer.toString(countMetadataValues(metadataList)));
        output.put(HEADERS.NUM_ATTACHMENTS.name(), Integer.toString(metadataList.size()-1));
        getExceptionStrings(metadataList, "", output);
        langid(content, "", output);
        Map<String, MutableValueInt> tokens = null;
        try {
            tokens = getTokens(content);
            handleWordCounts(output, tokens, "");
        } catch (IOException e) {
            //log
            e.printStackTrace();
        }
        writer.writeRow(output, getHeaders());
        return true;
    }

    @Override
    public Iterable<String> getHeaders() {
        if (localHeaders != null) {
            return localHeaders;
        }
        localHeaders = new ArrayList<String>();
        for (HEADERS h : HEADERS.values()) {
            localHeaders.add(h.name());
        }
        return localHeaders;
    }
}
