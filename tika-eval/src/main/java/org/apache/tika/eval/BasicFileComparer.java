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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import org.apache.lucene.util.PriorityQueue;
import org.apache.tika.batch.BatchNoRestartError;
import org.apache.tika.batch.FileResource;
import org.apache.tika.batch.fs.FSProperties;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.eval.db.ColInfo;
import org.apache.tika.eval.tokens.TokenCounter;
import org.apache.tika.eval.tokens.TokenIntPair;
import org.apache.tika.metadata.Metadata;

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
    private final static String thisExtension = "_A";
    private final static String thatExtension = "_B";
    private static int thisDirLen;

    public static void init(File thsRootDir, File thtRootDir) {
        thisRootDir = thsRootDir;
        thatRootDir = thtRootDir;
        thisDirLen = thsRootDir.getAbsolutePath().length() + 1;
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
        headers.put(header.name() + thisExtension, new ColInfo(headers.size() + 1,
                header.getColInfo().getType(), header.getColInfo().getPrecision()));
        headers.put(header.name() + thatExtension, new ColInfo(headers.size() + 1,
                header.getColInfo().getType(), header.getColInfo().getPrecision()));
    }

    private static void addHeaders(Map<String, ColInfo> headers, COMPARISON_HEADERS header, String thisExtension, String thatExtension) {
        headers.put(header.name() + thisExtension, new ColInfo(headers.size() + 1,
                header.getColInfo().getType(), header.getColInfo().getPrecision()));
        headers.put(header.name() + thatExtension, new ColInfo(headers.size() + 1,
                header.getColInfo().getType(), header.getColInfo().getPrecision()));
    }

    private static void addHeader(Map<String, ColInfo> headers, COMPARISON_HEADERS header) {
        headers.put(header.name(), new ColInfo(headers.size() + 1,
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
        } catch (Throwable e) {
            throw new RuntimeException("Exception while working on: " + relativePath, e);
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


    private void compareUnigramOverlap(List<Metadata> thisMetadata,
                                       List<Metadata> thatMetadata,
                                       Map<String, String> data) throws IOException {

        langid(thisMetadata, thisExtension, data);
        langid(thatMetadata, thatExtension, data);

        Map<String, PairCount> tokens = new HashMap<String, PairCount>();
        TokenCounter theseTokens = new CounterA(tokens);
        TokenCounter thoseTokens = new CounterB(tokens);
        countTokens(thisMetadata, theseTokens);
        countTokens(thatMetadata, thoseTokens);

        int diceDenom = theseTokens.getUniqueTokenCount() + thoseTokens.getUniqueTokenCount();
        int diceNum = 0;
        int overlapNum = 0;

        for (PairCount p : tokens.values()) {
            if (p.a > 0 && p.b > 0) {
                diceNum += 2;
                overlapNum += 2 * Math.min(p.a, p.b);
            }
        }

        float dice = (float) diceNum / (float) diceDenom;
        float overlap = (float) overlapNum / (float) (theseTokens.getTokenCount() + thoseTokens.getTokenCount());
        data.put(HEADERS.NUM_UNIQUE_TOKENS + thisExtension,
                Integer.toString(theseTokens.getUniqueTokenCount()));
        data.put(HEADERS.NUM_UNIQUE_TOKENS + thatExtension,
                Integer.toString(thoseTokens.getUniqueTokenCount()));
        data.put(COMPARISON_HEADERS.DICE_COEFFICIENT.name(),
                Float.toString(dice));
        data.put(COMPARISON_HEADERS.OVERLAP.name(), Float.toString(overlap));

        data.put(HEADERS.TOKEN_COUNT + thisExtension, Integer.toString(theseTokens.getTokenCount()));
        data.put(HEADERS.TOKEN_COUNT + thatExtension, Integer.toString(thoseTokens.getTokenCount()));

        handleWordCounts(data, theseTokens, thisExtension);
        handleWordCounts(data, thoseTokens, thatExtension);

        handleUniques(data, tokens, thisExtension, true);
        handleUniques(data, tokens, thatExtension, false);

        handleDiffs(data, tokens);
    }

    private void handleDiffs(Map<String, String> data, Map<String, PairCount> tokens) {
        if (tokens.size() == 0) {
            return;
        }
        int topNDiffs = 10;
        MutableValueAbsIntPriorityQueue queue = new MutableValueAbsIntPriorityQueue(topNDiffs);
        for (Map.Entry<String, PairCount> e : tokens.entrySet()) {
            int diff = e.getValue().b - e.getValue().a;
            if (diff == 0) {
                continue;
            }
            if (queue.top() == null || queue.size() < topNDiffs ||
                    Math.abs(diff) >= Math.abs(queue.top().getValue())) {
                queue.insertWithOverflow(new TokenIntPair(e.getKey(), diff));
            }
        }

        List<TokenIntPair> tokenDiffs = new ArrayList<TokenIntPair>();
        //now we reverse the queue
        TokenIntPair term = queue.pop();
        while (term != null) {
            tokenDiffs.add(0, term);
            term = queue.pop();
        }

        int i = 0;
        StringBuilder sb = new StringBuilder();
        for (TokenIntPair p : tokenDiffs) {
            if (i++ > 0) {
                sb.append(" | ");
            }
            sb.append(p.getToken()).append(": ").append(p.getValue());
        }
        data.put(COMPARISON_HEADERS.TOP_10_TOKEN_DIFFS.name(), sb.toString());
    }

    private void handleUniques(Map<String, String> data,
                               Map<String, PairCount> tokens, String extension, boolean counterA) {
        if (tokens.size() == 0) {
            return;
        }
        int topNUniques = 10;
        MutableValueIntPriorityQueue queue = new MutableValueIntPriorityQueue(topNUniques);

        if (counterA) {
            for (Map.Entry<String, PairCount> e : tokens.entrySet()) {
                if (e.getValue().b == 0) {
                    if (queue.top() == null || queue.size() < topNUniques ||
                            e.getValue().a >= queue.top().getValue()){
                        queue.insertWithOverflow(new TokenIntPair(e.getKey(), e.getValue().a));
                    }
                }
            }
        } else {
            for (Map.Entry<String, PairCount> e : tokens.entrySet()) {
                if (e.getValue().a == 0) {
                    if (queue.top() == null || queue.size() < topNUniques ||
                            e.getValue().a >= queue.top().getValue()){
                        queue.insertWithOverflow(new TokenIntPair(e.getKey(), e.getValue().b));
                    }
                }
            }
        }
        List<TokenIntPair> tokenCounts = new ArrayList<TokenIntPair>();
        //now we reverse the queue
        TokenIntPair term = queue.pop();
        while (term != null) {
            tokenCounts.add(0, term);
            term = queue.pop();
        }

        int i = 0;
        StringBuilder sb = new StringBuilder();
        for (TokenIntPair p : tokenCounts) {
            if (i++ > 0) {
                sb.append(" | ");
            }
            sb.append(p.getToken()).append(": ").append(p.getValue());
        }
        data.put(COMPARISON_HEADERS.TOP_10_UNIQUE_TOKEN_DIFFS + extension, sb.toString());
    }

    class MutableValueAbsIntPriorityQueue extends PriorityQueue<TokenIntPair> {

        MutableValueAbsIntPriorityQueue(int maxSize) {
            super(maxSize);
        }

        @Override
        protected boolean lessThan(TokenIntPair arg0, TokenIntPair arg1) {
            int v1 = Math.abs(arg0.getValue());
            int v2 = Math.abs(arg1.getValue());
            if (v1 < v2){
                return true;
            } else if (v1 == v2) {
                if (arg0.getValue() < arg1.getValue()) {
                    return true;
                } else if(arg0.getToken().compareTo(arg1.getToken()) > 0) {
                    return true;
                }
            }
            return false;
        }

    }
    class MutableValueIntPriorityQueue extends PriorityQueue<TokenIntPair> {

        MutableValueIntPriorityQueue(int maxSize) {
            super(maxSize);
        }

        @Override
        protected boolean lessThan(TokenIntPair arg0, TokenIntPair arg1) {
            if (arg0.getValue() < arg1.getValue()){
                return true;
            } else if (arg0.getValue() == arg1.getValue() &&
                    arg0.getToken().compareTo(arg1.getToken()) < 0) {
                return true;
            }
            return false;
        }
    }


    class CounterA extends TokenCounter {
        private final Map<String, PairCount> m;

        CounterA(Map<String, PairCount> m) {
            this.m = m;
        }

        @Override
        public void increment (String s){
            PairCount p = m.get(s);
            if (p == null) {
                p = new PairCount();
            }
            incrementOverallCounts(p.a);
            p.a++;
            m.put(s, p);
        }

        @Override
        public Collection<String> getTokens() {
            return m.keySet();
        }

        @Override
        public int getCount(String token) {
            PairCount p = m.get(token);
            if (p == null) {
                return 0;
            }
            return p.a;
        }
    }

    class CounterB extends TokenCounter {

        private final Map<String, PairCount> m;
        CounterB(Map<String, PairCount> m) {
            this.m = m;
        }

        @Override
        public void increment(String s){
            PairCount p = m.get(s);
            if (p == null) {
                p = new PairCount();
            }
            incrementOverallCounts(p.b);
            p.b++;
            m.put(s, p);
        }

        @Override
        public Collection<String> getTokens() {
            return m.keySet();
        }

        @Override
        public int getCount(String token) {
            PairCount p = m.get(token);
            if (p == null) {
                return 0;
            }
            return p.b;
        }
    }

    class PairCount {
        int a = 0;
        int b = 0;
    }


}