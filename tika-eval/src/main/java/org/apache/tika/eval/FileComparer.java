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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.util.PriorityQueue;
import org.apache.tika.batch.FileResource;
import org.apache.tika.batch.fs.FSProperties;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.eval.db.ColInfo;
import org.apache.tika.eval.io.IDBWriter;
import org.apache.tika.eval.tokens.TokenCounter;
import org.apache.tika.eval.tokens.TokenIntPair;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.RecursiveParserWrapper;

public class FileComparer extends AbstractProfiler {

    public static final String COMPARISONS_TABLE = "comparisons";

    public enum COMPARISON_HEADERS {
        DIFF_NUM_ATTACHMENTS(new ColInfo(-1, Types.INTEGER)),
        DIFF_NUM_METADATA_VALUES(new ColInfo(-1, Types.INTEGER)),
        TOP_10_UNIQUE_TOKEN_DIFFS(new ColInfo(-1, Types.VARCHAR, 1024)),
        TOP_10_MORE_IN_A(new ColInfo(-1, Types.VARCHAR, 1024)),
        TOP_10_MORE_IN_B(new ColInfo(-1, Types.VARCHAR, 1024)),
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

    //need to parameterize?
    private final TikaConfig config = TikaConfig.getDefaultConfig();

    public final static String thisExtension = "_A";
    public final static String thatExtension = "_B";

    private final File thisRootDir;
    private final File thatRootDir;

    private final long minJsonLength;
    private final long maxJsonLength;

    static  {
        headers = new HashMap<String, ColInfo>();
        addHeader(headers, HEADERS.ID);
        addHeader(headers, HEADERS.CONTAINER_ID);
        addHeader(headers, HEADERS.FILE_PATH);
        addHeader(headers, HEADERS.FILE_LENGTH);
        addHeader(headers, HEADERS.IS_EMBEDDED);
        addHeader(headers, HEADERS.EMBEDDED_FILE_PATH);
        addHeaders(headers, HEADERS.EMBEDDED_FILE_LENGTH, thisExtension, thatExtension);
        addHeaders(headers, HEADERS.JSON_FILE_LENGTH, thisExtension, thatExtension);
        addHeaders(headers, HEADERS.JSON_EX, thisExtension, thatExtension);
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
        addHeader(headers, COMPARISON_HEADERS.TOP_10_MORE_IN_A);
        addHeader(headers, COMPARISON_HEADERS.TOP_10_MORE_IN_B);
        addHeaders(headers, HEADERS.TOP_N_WORDS, thisExtension, thatExtension);
        addHeaders(headers, HEADERS.NUM_EN_STOPS_TOP_N, thisExtension, thatExtension);
        addHeaders(headers, HEADERS.LANG_ID1, thisExtension, thatExtension);
        addHeaders(headers, HEADERS.LANG_ID_PROB1, thisExtension, thatExtension);
        addHeaders(headers, HEADERS.TOKEN_ENTROPY_RATE, thisExtension, thatExtension);
        addHeaders(headers, HEADERS.TOKEN_LENGTH_SUM, thisExtension, thatExtension);
        addHeaders(headers, HEADERS.TOKEN_LENGTH_MEAN, thisExtension, thatExtension);
        addHeaders(headers, HEADERS.TOKEN_LENGTH_STD_DEV, thisExtension, thatExtension);

    }

    public FileComparer(ArrayBlockingQueue<FileResource> queue,
                        File thsRootDir, File thtRootDir,
                        boolean crawlingInputDir, IDBWriter writer, long minJsonLength,
                        long maxJsonLength) {
        super(queue, crawlingInputDir, writer);
        this.minJsonLength = minJsonLength;
        this.maxJsonLength = maxJsonLength;
        this.thisRootDir = thsRootDir;
        this.thatRootDir = thtRootDir;
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

    public static Map<String, ColInfo> getHeaders() {
        return headers;
    }

    @Override
    public boolean processFileResource(FileResource fileResource) {
        Metadata metadata = fileResource.getMetadata();
        String relativePath = metadata.get(FSProperties.FS_REL_PATH);

        File thisFile = new File(thisRootDir, relativePath);
        File thatFile = new File(thatRootDir, relativePath);
        if (minJsonLength > -1) {
            if (thisFile.length() < minJsonLength && thatFile.length() < minJsonLength) {
                return false;
            }
        }

        if (maxJsonLength > -1) {
            if (thisFile.length() > maxJsonLength || thatFile.length() > maxJsonLength) {
                return false;
            }
        }

        try {
            compareFiles(relativePath, thisFile, thatFile);
        } catch (Throwable e) {
            e.printStackTrace();
            //this should be cataclysmic...
            throw new RuntimeException("Exception while working on: " + relativePath, e);
        }
        return true;
    }



    //TODO: clean up
    //protected for testing, should find better way so that this can be private!
    protected void compareFiles(String relativePath, File thisFile, File thatFile) throws IOException {
        List<Metadata> thisMetadataList = getMetadata(thisFile);
        List<Metadata> thatMetadataList = getMetadata(thatFile);
        //array indices for those metadata items handled in
        //"that"
        Set<Integer> handledThat = new HashSet<Integer>();
        Map<String, String> output = new HashMap<String, String>();
        String thisJsonLength = Long.toString(thisFile.length());
        String thatJsonLength = Long.toString(thatFile.length());
        String containerID = Integer.toString(CONTAINER_ID.getAndIncrement());
        if (thisMetadataList == null && thatMetadataList == null) {
            output.put(HEADERS.FILE_PATH.name(), getInputFileName(relativePath));
            output.put(HEADERS.FILE_EXTENSION.name(),
                    FilenameUtils.getExtension(output.get(HEADERS.FILE_PATH.name())));
            output.put(HEADERS.JSON_FILE_LENGTH.name()+thisExtension, thisJsonLength);
            output.put(HEADERS.JSON_FILE_LENGTH.name()+thatExtension, thatJsonLength);
            output.put(HEADERS.FILE_PATH.name(), getInputFileName(relativePath));
            output.put(HEADERS.JSON_EX + thisExtension,
                    JSON_PARSE_EXCEPTION);
            output.put(HEADERS.JSON_EX + thatExtension,
                    JSON_PARSE_EXCEPTION);
            output.put(HEADERS.ID.name(), Integer.toString(ID.getAndIncrement()));
            output.put(HEADERS.CONTAINER_ID.name(), containerID);
            writer.writeRow(COMPARISONS_TABLE, output);
            return;
        }

        //now get that metadata
        if (thisMetadataList != null) {
            for (int i = 0; i < thisMetadataList.size(); i++) {
                output.clear();
                output.put(HEADERS.ID.name(), Integer.toString(ID.getAndIncrement()));
                output.put(HEADERS.CONTAINER_ID.name(), containerID);
                Metadata thisMetadata = thisMetadataList.get(i);
                Metadata thatMetadata = null;
                int matchIndex = getMatch(i, thisMetadataList, thatMetadataList);

                if (matchIndex > -1) {
                    thatMetadata = thatMetadataList.get(matchIndex);
                    handledThat.add(matchIndex);
                }
                output.put(HEADERS.JSON_FILE_LENGTH.name()+thisExtension, thisJsonLength);
                output.put(HEADERS.JSON_FILE_LENGTH.name()+thatExtension, thatJsonLength);
                output.put(HEADERS.FILE_PATH.name(), getInputFileName(relativePath));
                String lenString = thisMetadata.get(Metadata.CONTENT_LENGTH);
                if (lenString != null) {
                    output.put(HEADERS.FILE_LENGTH.name(), lenString);
                }
                if (thatMetadataList == null) {
                    output.put(HEADERS.JSON_EX + thatExtension,
                            JSON_PARSE_EXCEPTION);
                }
                //overall/container document
                if (i == 0) {
                    output.put(HEADERS.FILE_EXTENSION.name(),
                            getOriginalFileExtension(output.get(HEADERS.FILE_PATH.name())));
                    //num attachments
                    int thisNumAttachments = (thisMetadataList == null) ? 0 : thisMetadataList.size() - 1;
                    output.put(HEADERS.NUM_ATTACHMENTS + thisExtension,
                            Integer.toString(thisNumAttachments));

                    int thatNumAttachments = (thatMetadataList == null) ? 0 : thatMetadataList.size() - 1;
                    output.put(HEADERS.NUM_ATTACHMENTS + thatExtension,
                            Integer.toString(thatNumAttachments));
                    output.put(COMPARISON_HEADERS.DIFF_NUM_ATTACHMENTS.name(),
                            Integer.toString(thatNumAttachments - thisNumAttachments));
                    output.put(HEADERS.IS_EMBEDDED.name(), "false");
                } else { //embedded document
                    output.put(HEADERS.IS_EMBEDDED.name(), "true");
                    output.put(HEADERS.EMBEDDED_FILE_PATH.name(),
                            thisMetadata.get(RecursiveParserWrapper.EMBEDDED_RESOURCE_PATH));
                    output.put(HEADERS.FILE_EXTENSION.name(),
                            FilenameUtils.getExtension(output.get(HEADERS.EMBEDDED_FILE_PATH.name())));

                }

                //prep the token counting
                Map<String, PairCount> tokens = new HashMap<String, PairCount>();
                TokenCounter theseTokens = new CounterA(tokens);
                TokenCounter thoseTokens = new CounterB(tokens);
                countTokens(thisMetadata, theseTokens);
                countTokens(thatMetadata, thoseTokens);

                addSingleFileStats(thisMetadata, tokens.keySet(), theseTokens, thisExtension, output);
                addSingleFileStats(thatMetadata, tokens.keySet(), thoseTokens, thatExtension, output);

                //y, double counting metadata values...improve
                output.put(COMPARISON_HEADERS.DIFF_NUM_METADATA_VALUES.name(),
                        Integer.toString(countMetadataValues(thisMetadata) -
                                countMetadataValues(thatMetadata)));

                compareUnigramOverlap(tokens, theseTokens, thoseTokens, output);

                writer.writeRow(COMPARISONS_TABLE, output);

            }
        }
        //now try to get any Metadata objects in "that"
        //that haven't yet been handled.
        if (thatMetadataList != null) {
            for (int i = 0; i < thatMetadataList.size(); i++) {
                if (handledThat.contains(i)) {
                    continue;
                }
                output.clear();
                output.put(HEADERS.ID.name(), Integer.toString(ID.getAndIncrement()));
                output.put(HEADERS.CONTAINER_ID.name(), containerID);
                Metadata thatMetadata = thatMetadataList.get(i);
                if (thatMetadata == null) {
                    throw new RuntimeException("NULL metadata in list");
                }
                output.put(HEADERS.FILE_PATH.name(), getInputFileName(relativePath));
                output.put(HEADERS.JSON_FILE_LENGTH.name()+thisExtension, thisJsonLength);
                output.put(HEADERS.JSON_FILE_LENGTH.name()+thatExtension, thatJsonLength);
                String lenString = thatMetadata.get(Metadata.CONTENT_LENGTH);
                if (lenString != null) {
                    output.put(HEADERS.FILE_LENGTH.name(), lenString);
                }
                if (thisMetadataList == null) {
                    output.put(HEADERS.JSON_EX + thisExtension,
                            JSON_PARSE_EXCEPTION);
                }

                //overall/container document
                if (i == 0) {
                    output.put(HEADERS.FILE_EXTENSION.name(),
                            getOriginalFileExtension(output.get(HEADERS.FILE_PATH.name())));

                    //num attachments
                    output.put(HEADERS.NUM_ATTACHMENTS + thisExtension, "0");

                    int thatNumAttachments = thatMetadataList.size() - 1;
                    output.put(HEADERS.NUM_ATTACHMENTS + thatExtension,
                            Integer.toString(thatNumAttachments));
                    output.put(COMPARISON_HEADERS.DIFF_NUM_ATTACHMENTS.name(),
                            Integer.toString(thatNumAttachments));
                    output.put(HEADERS.IS_EMBEDDED.name(), "false");
                } else { //embedded document
                    output.put(HEADERS.IS_EMBEDDED.name(), "true");
                    output.put(HEADERS.EMBEDDED_FILE_PATH.name(),
                            thatMetadata.get(RecursiveParserWrapper.EMBEDDED_RESOURCE_PATH));
                    output.put(HEADERS.FILE_EXTENSION.name(),
                            FilenameUtils.getExtension(output.get(HEADERS.EMBEDDED_FILE_PATH.name())));
                }

                //prep the token counting
                Map<String, PairCount> tokens = new HashMap<String, PairCount>();
                TokenCounter theseTokens = new CounterA(tokens);
                TokenCounter thoseTokens = new CounterB(tokens);
                countTokens(thatMetadata, thoseTokens);

                addSingleFileStats(thatMetadata, tokens.keySet(), thoseTokens, thatExtension, output);
                addSingleFileStats(null, tokens.keySet(), theseTokens, thisExtension, output);
                compareUnigramOverlap(tokens, theseTokens, thoseTokens, output);

                //y, double counting metadata values...improve
                output.put(COMPARISON_HEADERS.DIFF_NUM_METADATA_VALUES.name(),
                        Integer.toString(0 -
                                countMetadataValues(thatMetadata)));
                writer.writeRow(COMPARISONS_TABLE, output);
            }
        }
    }

    /**
     * Try to find the matching metadata based on the RecursiveParserWrapper.EMBEDDED_RESOURCE_PATH
     * If you can't find it, return -1;
     *
     * @param i                index for match in thisMetadataList
     * @param thisMetadataList
     * @param thatMetadataList
     * @return
     */

    private int getMatch(int i,
                         List<Metadata> thisMetadataList,
                         List<Metadata> thatMetadataList) {
        if (thatMetadataList == null || thatMetadataList.size() == 0) {
            return -1;
        }
        if (i == 0) {
            return 0;
        }
        if (thisMetadataList.size() == thatMetadataList.size()) {
            //assume no rearrangments if lists are the same size
            return i;
        }

        Metadata thisMetadata = thisMetadataList.get(i);
        String embeddedPath = thisMetadata.get(RecursiveParserWrapper.EMBEDDED_RESOURCE_PATH);
        if (embeddedPath == null) {
            return -1;
        }
        if (i < thatMetadataList.size()) {
        }

        for (int j = 0; j < thatMetadataList.size(); j++) {
            String thatEmbeddedPath = thatMetadataList.get(j).get(
                    RecursiveParserWrapper.EMBEDDED_RESOURCE_PATH);
            if (embeddedPath.equals(thatEmbeddedPath)) {
                return j;
            }
        }
        return -1;
    }


    private void compareUnigramOverlap(Map<String, PairCount> tokens,
                                       TokenCounter theseTokens, TokenCounter thoseTokens,
                                       Map<String, String> data) throws IOException {

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


        handleUniques(data, tokens, thisExtension, true);
        handleUniques(data, tokens, thatExtension, false);
        handleDiffs(data, tokens);
    }

    private void handleDiffs(Map<String, String> data, Map<String, PairCount> tokens) {
        if (tokens.size() == 0) {
            return;
        }
        int topNDiffs = 10;
        MutableValueAbsIntPriorityQueue bQueue = new MutableValueAbsIntPriorityQueue(topNDiffs);
        MutableValueAbsIntPriorityQueue aQueue = new MutableValueAbsIntPriorityQueue(topNDiffs);
        for (Map.Entry<String, PairCount> e : tokens.entrySet()) {
            int diff = e.getValue().b - e.getValue().a;
            if (diff == 0) {
                continue;
            } else if (diff > 0) {
                if (bQueue.top() == null || bQueue.size() < topNDiffs ||
                        diff >= bQueue.top().getValue()) {
                    bQueue.insertWithOverflow(new TokenIntPair(e.getKey(), diff));
                }
            } else {
                diff = Math.abs(diff);
                if (aQueue.top() == null || aQueue.size() < topNDiffs ||
                        diff >= aQueue.top().getValue()) {
                    aQueue.insertWithOverflow(new TokenIntPair(e.getKey(), diff));
                }
            }
        }

        List<TokenIntPair> tokenDiffs = new ArrayList<TokenIntPair>();
        //now we reverse the queue
        TokenIntPair term = aQueue.pop();
        while (term != null) {
            tokenDiffs.add(0, term);
            term = aQueue.pop();
        }

        int i = 0;
        StringBuilder sb = new StringBuilder();
        for (TokenIntPair p : tokenDiffs) {
            if (i++ > 0) {
                sb.append(" | ");
            }
            sb.append(p.getToken()).append(": ").append(p.getValue());
        }
        data.put(COMPARISON_HEADERS.TOP_10_MORE_IN_A.name(), sb.toString());


        tokenDiffs.clear();
        //now we reverse the queue
        term = bQueue.pop();
        while (term != null) {
            tokenDiffs.add(0, term);
            term = bQueue.pop();
        }

        i = 0;
        sb.setLength(0);
        for (TokenIntPair p : tokenDiffs) {
            if (i++ > 0) {
                sb.append(" | ");
            }
            sb.append(p.getToken()).append(": ").append(p.getValue());
        }
        data.put(COMPARISON_HEADERS.TOP_10_MORE_IN_B.name(), sb.toString());
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
                            e.getValue().a >= queue.top().getValue()) {
                        queue.insertWithOverflow(new TokenIntPair(e.getKey(), e.getValue().a));
                    }
                }
            }
        } else {
            for (Map.Entry<String, PairCount> e : tokens.entrySet()) {
                if (e.getValue().a == 0) {
                    if (queue.top() == null || queue.size() < topNUniques ||
                            e.getValue().a >= queue.top().getValue()) {
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
            if (v1 < v2) {
                return true;
            } else if (v1 == v2) {
                if (arg0.getValue() < arg1.getValue()) {
                    return true;
                } else if (arg0.getToken().compareTo(arg1.getToken()) > 0) {
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
            if (arg0.getValue() < arg1.getValue()) {
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
        public void increment(String s) {
            PairCount p = m.get(s);
            if (p == null) {
                p = new PairCount();
                m.put(s, p);
            }
            incrementOverallCounts(p.a);
            p.a++;
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
        public void increment(String s) {
            PairCount p = m.get(s);
            if (p == null) {
                p = new PairCount();
                m.put(s, p);
            }
            incrementOverallCounts(p.b);
            p.b++;
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PairCount pairCount = (PairCount) o;

            if (a != pairCount.a) return false;
            if (b != pairCount.b) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = a;
            result = 31 * result + b;
            return result;
        }
    }


}