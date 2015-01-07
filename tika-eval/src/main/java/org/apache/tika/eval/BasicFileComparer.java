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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;

import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import org.apache.lucene.util.mutable.MutableValueInt;
import org.apache.tika.batch.BatchNoRestartError;
import org.apache.tika.batch.FileResource;
import org.apache.tika.batch.fs.FSProperties;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.eval.db.ColInfo;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MimeType;
import org.apache.tika.mime.MimeTypeException;
import org.apache.tika.mime.MimeTypes;

public class BasicFileComparer extends AbstractProfiler {

    public enum COMPARISON_HEADERS {
        DIFF_NUM_ATTACHMENTS(new ColInfo(-1, Types.INTEGER)),
        DIFF_NUM_METADATA_VALUES(new ColInfo(-1, Types.INTEGER)),
        TOP_10_UNIQUE_TOKEN_DIFFS(new ColInfo(-1, Types.VARCHAR, 1024)),
        TOP_10_TOKEN_DIFFS(new ColInfo(-1, Types.VARCHAR, 1024)),
        DICE_COEFFICIENT(new ColInfo(-1, Types.FLOAT)),
        OVERLAP(new ColInfo(-1, Types.FLOAT));

        private final ColInfo colInfo;

        COMPARISON_HEADERS(ColInfo colInfo) {
            this.colInfo = colInfo;
        }

        protected ColInfo getColInfo() {
            return colInfo;
        }

    }

    public static Map<String, ColInfo> headers;


    //good enough? or do we need to parameterize?
    private final TikaConfig config = TikaConfig.getDefaultConfig();

    private static File thisRootDir;
    private static File thatRootDir;
    private static String thisExtension;
    private static String thatExtension;
    private static int thisDirLen;

    public static void init(File thsRootDir, File thtRootDir) {
        thisRootDir = thsRootDir;
        thatRootDir = thtRootDir;
        thisDirLen = thsRootDir.getAbsolutePath().length() + 1;
        thisExtension = "_" + thsRootDir.getName();
        thatExtension = "_" + thtRootDir.getName();
        headers = new HashMap<String, ColInfo>();
        addHeader(headers, HEADERS.FILE_PATH);
        addHeaders(headers, HEADERS.JSON_EX, thisExtension, thatExtension);
        addHeaders(headers, HEADERS.ORIG_STACK_TRACE, thisExtension, thatExtension);
        addHeaders(headers, HEADERS.SORT_STACK_TRACE, thisExtension, thatExtension);
        addHeader(headers, HEADERS.FILE_EXTENSION);
        addHeaders(headers, HEADERS.DETECTED_CONTENT_TYPE, thisExtension, thatExtension);
        addHeaders(headers, HEADERS.DETECTED_FILE_EXTENSION, thisExtension, thatExtension);
        addHeaders(headers, HEADERS.NUM_ATTACHMENTS, thisExtension, thatExtension);
        addHeader(headers, COMPARISON_HEADERS.DIFF_NUM_ATTACHMENTS);
        addHeaders(headers, HEADERS.NUM_METADATA_VALUES, thisExtension, thatExtension);
        addHeader(headers, COMPARISON_HEADERS.DIFF_NUM_METADATA_VALUES);
        addHeaders(headers, HEADERS.ELAPSED_TIME_MILLIS, thisExtension, thatExtension);
        addHeaders(headers, HEADERS.NUM_UNIQUE_TOKENS, thisExtension, thatExtension);
        addHeader(headers, COMPARISON_HEADERS.DICE_COEFFICIENT);
        addHeaders(headers, COMPARISON_HEADERS.TOP_10_UNIQUE_TOKEN_DIFFS, thisExtension, thatExtension);
        addHeaders(headers, HEADERS.TOKEN_COUNT, thisExtension, thatExtension);
        addHeader(headers, COMPARISON_HEADERS.OVERLAP);
        addHeader(headers, COMPARISON_HEADERS.TOP_10_TOKEN_DIFFS);
        addHeaders(headers, HEADERS.TOP_N_WORDS, thisExtension, thatExtension);
        addHeaders(headers, HEADERS.NUM_EN_STOPS_TOP_N, thisExtension, thatExtension);
        addHeaders(headers, HEADERS.LANG_ID1, thisExtension, thatExtension);
        addHeaders(headers, HEADERS.LANG_ID_PROB1, thisExtension, thatExtension);

    }

    public BasicFileComparer(ArrayBlockingQueue<FileResource> queue) {
        super(queue);
    }

    private static void addHeaders(Map<String, ColInfo> headers, HEADERS header, String thisExtension, String thatExtension) {
        headers.put(header.name()+thisExtension, new ColInfo(headers.size()+1,
                header.getColInfo().getType(), header.getColInfo().getPrecision()));
        headers.put(header.name()+thatExtension, new ColInfo(headers.size()+1,
                header.getColInfo().getType(), header.getColInfo().getPrecision()));
    }

    private static void addHeaders(Map<String, ColInfo> headers, COMPARISON_HEADERS header, String thisExtension, String thatExtension) {
        headers.put(header.name()+thisExtension, new ColInfo(headers.size()+1,
                header.getColInfo().getType(), header.getColInfo().getPrecision()));
        headers.put(header.name()+thatExtension, new ColInfo(headers.size()+1,
                header.getColInfo().getType(), header.getColInfo().getPrecision()));
    }

    private static void addHeader(Map<String, ColInfo> headers, COMPARISON_HEADERS header) {
        headers.put(header.name(), new ColInfo(headers.size()+1,
                header.getColInfo().getType(), header.getColInfo().getPrecision()));
    }

    private static void addHeader(Map<String, ColInfo> headers, HEADERS header) {
        headers.put(header.name(), new ColInfo(headers.size()+1,
                header.getColInfo().getType(), header.getColInfo().getPrecision()));
    }


    public static void setLangModelDir(File langModelDir) {
        try {
            DetectorFactory.loadProfile(langModelDir);
        } catch (LangDetectException e) {
            throw new BatchNoRestartError(e);
        }
    }

    @Override
    public boolean processFileResource(FileResource fileResource) {
        Metadata metadata = fileResource.getMetadata();
        String relativePath = metadata.get(FSProperties.FS_REL_PATH);

        File thisFile = new File(thisRootDir, relativePath);
        File thatFile = new File(thatRootDir, relativePath);

        try {
            Map<String, String> output = compareFiles(relativePath, thisFile, thatFile);
            writer.writeRow(output);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    public static Map<String, ColInfo> getHeaders() {
        return headers;
    }

    protected Map<String, String> compareFiles(String relativePath, File thisFile, File thatFile) throws IOException {
        List<Metadata> thisMetadata = getMetadata(thisFile);
        List<Metadata> thatMetadata = getMetadata(thatFile);
        Map<String, String> output = new HashMap<String, String>();
        output.put(HEADERS.FILE_PATH.name(), relativePath);

        if (thisMetadata == null) {
            output.put(HEADERS.JSON_EX + thisExtension,
                    "Error with json parsing");
        }
        if (thatMetadata == null) {
            output.put(HEADERS.JSON_EX + thatExtension,
                    "Error with json parsing");
        }

        getExceptionStrings(thisMetadata, thisExtension, output);
        getExceptionStrings(thatMetadata, thatExtension, output);

        output.put(HEADERS.FILE_EXTENSION.name(),
                getOriginalFileExtension(thisFile.getName()));

        getFileTypes(thisMetadata, thisExtension, output);
        getFileTypes(thatMetadata, thatExtension, output);

        int thisNumAttachments = (thisMetadata == null) ? 0 : thisMetadata.size() - 1;
        int thatNumAttachments = (thatMetadata == null) ? 0 : thatMetadata.size() - 1;
        output.put(HEADERS.NUM_ATTACHMENTS + thisExtension,
                Integer.toString(thisNumAttachments));
        output.put(HEADERS.NUM_ATTACHMENTS + thatExtension,
                Integer.toString(thatNumAttachments));

        output.put(COMPARISON_HEADERS.DIFF_NUM_ATTACHMENTS.name(),
                Integer.toString(thatNumAttachments - thisNumAttachments));

        int thisNumMetadataValues = countMetadataValues(thisMetadata);
        int thatNumMetadataValues = countMetadataValues(thatMetadata);
        output.put(HEADERS.NUM_METADATA_VALUES + thisExtension,
                Integer.toString(thisNumMetadataValues));

        output.put(HEADERS.NUM_METADATA_VALUES + thatExtension,
                Integer.toString(thatNumMetadataValues));

        output.put(COMPARISON_HEADERS.DIFF_NUM_METADATA_VALUES.name(),
                Integer.toString(thatNumMetadataValues - thisNumMetadataValues));

        output.put(HEADERS.ELAPSED_TIME_MILLIS + thisExtension,
                getTime(thisMetadata));
        output.put(HEADERS.ELAPSED_TIME_MILLIS + thatExtension,
                getTime(thatMetadata));

        compareUnigramOverlap(thisMetadata, thatMetadata, output);
        return output;
    }

    private void getFileTypes(List<Metadata> metadata, String extension, Map<String, String> output) {
        if (metadata == null || metadata.size() == 0) {
            return;
        }
        String type = metadata.get(0).get(Metadata.CONTENT_TYPE);
        if (type == null) {
            return;
        }
        output.put(HEADERS.DETECTED_CONTENT_TYPE + extension, type);

        try {
            MimeTypes types = config.getMimeRepository();
            MimeType mime = types.forName(type);
            String ext = mime.getExtension();
            if (ext.startsWith(".")) {
                ext = ext.substring(1);
            }
            output.put(HEADERS.DETECTED_FILE_EXTENSION + extension, ext);
        } catch (MimeTypeException e) {
            //swallow
        }

    }


    private void compareUnigramOverlap(List<Metadata> thisMetadata,
                                       List<Metadata> thatMetadata,
                                       Map<String, String> data) throws IOException {

        String content = getContent(thisMetadata);
        langid(content, thisExtension, data);
        Map<String, MutableValueInt> theseTokens = getTokens(content);
        content = getContent(thatMetadata);
        langid(content, thatExtension, data);
        Map<String, MutableValueInt> thoseTokens = getTokens(content);


        int tokenCountThis = 0;
        int tokenCountThat = 0;
        int diceDenom = theseTokens.size() + thoseTokens.size();
        int diceNum = 0;

        int overlapNum = 0;
        Map<String, Integer> diffTokenCounts = new HashMap<String, Integer>();
        Map<String, Integer> thisUniqueTokens = new HashMap<String, Integer>();
        Map<String, Integer> thatUniqueTokens = new HashMap<String, Integer>();

        for (Map.Entry<String, MutableValueInt> e : theseTokens.entrySet()) {
            MutableValueInt thatCount = thoseTokens.get(e.getKey());
            if (thatCount != null) {
                diceNum += 2;
                overlapNum += 2 * Math.min(e.getValue().value, thatCount.value);
            }
            tokenCountThis += e.getValue().value;

            int localThatCount = (thatCount == null) ? 0 : thatCount.value;
            if (e.getValue().value != localThatCount) {
                diffTokenCounts.put(e.getKey(), localThatCount - e.getValue().value);
            }
            if (localThatCount == 0) {
                thisUniqueTokens.put(e.getKey(), e.getValue().value);
            }

        }

        for (Map.Entry<String, MutableValueInt> e : thoseTokens.entrySet()) {
            tokenCountThat += e.getValue().value;
            if (!theseTokens.containsKey(e.getKey())) {
                diffTokenCounts.put(e.getKey(), e.getValue().value);
                thatUniqueTokens.put(e.getKey(), e.getValue().value);
            }
        }

        float dice = (float) diceNum / (float) diceDenom;
        float overlap = (float) overlapNum / (float) (tokenCountThis + tokenCountThat);
        data.put(HEADERS.NUM_UNIQUE_TOKENS + thisExtension,
                Integer.toString(theseTokens.size()));
        data.put(HEADERS.NUM_UNIQUE_TOKENS + thatExtension,
                Integer.toString(thoseTokens.size()));
        data.put(COMPARISON_HEADERS.DICE_COEFFICIENT.name(),
                Float.toString(dice));
        data.put(COMPARISON_HEADERS.OVERLAP.name(), Float.toString(overlap));

        data.put(HEADERS.TOKEN_COUNT + thisExtension, Integer.toString(tokenCountThis));
        data.put(HEADERS.TOKEN_COUNT + thatExtension, Integer.toString(tokenCountThat));

        handleWordCounts(data, theseTokens, thisExtension);
        handleWordCounts(data, thoseTokens, thatExtension);

        handleUniques(data, thisUniqueTokens, thisExtension);
        handleUniques(data, thatUniqueTokens, thatExtension);

        handleDiffs(data, diffTokenCounts);
    }

    private void handleDiffs(Map<String, String> data, Map<String, Integer> diffTokenCounts) {
        if (diffTokenCounts.size() == 0) {
            return;
        }
        Comparator descValSorter = new DescendingAbsValSorter(diffTokenCounts);
        TreeMap<String, Integer> sorted = new TreeMap<String, Integer>(descValSorter);
        sorted.putAll(diffTokenCounts);
        int i = 0;
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Integer> e : sorted.entrySet()) {
            if (i > 0) {
                sb.append(" | ");
            }
            sb.append(e.getKey()).append(": ").append(e.getValue());
            i++;
            if (i >= 10) {
                break;
            }
        }
        data.put(COMPARISON_HEADERS.TOP_10_TOKEN_DIFFS.name(), sb.toString());
    }

    private void handleUniques(Map<String, String> data,
                               Map<String, Integer> uniqueTokens, String extension) {
        if (uniqueTokens.size() == 0) {
            return;
        }
        Comparator descValSorter = new DescendingValSorter(uniqueTokens);
        TreeMap<String, Integer> sorted = new TreeMap<String, Integer>(descValSorter);
        sorted.putAll(uniqueTokens);
        int i = 0;
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Integer> e : sorted.entrySet()) {
            if (i > 0) {
                sb.append(" | ");
            }
            sb.append(e.getKey()).append(": ").append(e.getValue());
            i++;
            if (i >= 10) {
                break;
            }
        }
        data.put(COMPARISON_HEADERS.TOP_10_UNIQUE_TOKEN_DIFFS + extension, sb.toString());
    }

    class DescendingValSorter implements Comparator<String> {

        Map<String, Integer> map;

        private DescendingValSorter(Map<String, Integer> base) {
            this.map = base;
        }

        public int compare(String a, String b) {
            if (map.get(a) > map.get(b)) {
                return -1;
            } else if (map.get(a) == map.get(b)) {
                return a.compareTo(b);
            } else {
                return 1;

            }
        }
    }


    class DescendingAbsValSorter implements Comparator<String> {

        Map<String, Integer> map;

        private DescendingAbsValSorter(Map<String, Integer> base) {
            this.map = base;
        }

        public int compare(String a, String b) {
            if (Math.abs(map.get(a)) > Math.abs(map.get(b))) {
                return -1;
            } else if (Math.abs(map.get(a)) == Math.abs(map.get(b))){
                return a.compareTo(b);
            } else {
                return 1;
            }
        }
    }
}