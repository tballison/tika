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
package org.apache.tika.eval;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.lucene.util.PriorityQueue;
import org.apache.tika.batch.FileResource;
import org.apache.tika.batch.fs.FSProperties;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.eval.db.ColInfo;
import org.apache.tika.eval.db.Cols;
import org.apache.tika.eval.db.TableInfo;
import org.apache.tika.eval.io.IDBWriter;
import org.apache.tika.eval.tokens.TokenCounter;
import org.apache.tika.eval.tokens.TokenIntPair;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.RecursiveParserWrapper;

public class FileComparer extends AbstractProfiler {

    public static TableInfo REF_PAIR_NAMES = new TableInfo("pair_names",
            new ColInfo(Cols.DIR_NAME_A, Types.VARCHAR, 128),
            new ColInfo(Cols.DIR_NAME_B, Types.VARCHAR, 128)
    );

    public static TableInfo COMPARISON_CONTAINERS = new TableInfo("containers",
            new ColInfo(Cols.CONTAINER_ID, Types.INTEGER, "PRIMARY KEY"),
            new ColInfo(Cols.FILE_PATH, Types.VARCHAR, FILE_PATH_MAX_LEN),
            new ColInfo(Cols.FILE_EXTENSION, Types.VARCHAR, 12),
            new ColInfo(Cols.LENGTH, Types.BIGINT),
            new ColInfo(Cols.EXTRACT_FILE_LENGTH_A, Types.BIGINT),
            new ColInfo(Cols.EXTRACT_FILE_LENGTH_B, Types.BIGINT)
    );

    public static TableInfo CONTENT_COMPARISONS = new TableInfo("content_comparisons",
            new ColInfo(Cols.ID, Types.INTEGER, "PRIMARY KEY"),
            new ColInfo(Cols.TOP_10_UNIQUE_TOKEN_DIFFS_A, Types.VARCHAR, 1024),
            new ColInfo(Cols.TOP_10_UNIQUE_TOKEN_DIFFS_B, Types.VARCHAR, 1024),
            new ColInfo(Cols.TOP_10_MORE_IN_A, Types.VARCHAR, 1024),
            new ColInfo(Cols.TOP_10_MORE_IN_B, Types.VARCHAR, 1024),
            new ColInfo(Cols.DICE_COEFFICIENT, Types.FLOAT),
            new ColInfo(Cols.OVERLAP, Types.FLOAT)
    );

    public static TableInfo PROFILES_A = new TableInfo( "profiles_a",
            SingleFileProfiler.PROFILE_TABLE.getColInfos());

    public static TableInfo PROFILES_B = new TableInfo( "profiles_b",
            SingleFileProfiler.PROFILE_TABLE.getColInfos());

    public static TableInfo EMBEDDED_FILE_PATH_TABLE_A = new TableInfo( "emb_path_a",
            SingleFileProfiler.EMBEDDED_FILE_PATH_TABLE.getColInfos());

    public static TableInfo EMBEDDED_FILE_PATH_TABLE_B = new TableInfo( "emb_path_b",
            SingleFileProfiler.EMBEDDED_FILE_PATH_TABLE.getColInfos());


    public static TableInfo CONTENTS_TABLE_A = new TableInfo( "contents_a",
            SingleFileProfiler.CONTENTS_TABLE.getColInfos());

    public static TableInfo CONTENTS_TABLE_B = new TableInfo( "contents_b",
            SingleFileProfiler.CONTENTS_TABLE.getColInfos());

    public static TableInfo EXCEPTION_TABLE_A = new TableInfo ("exceptions_a",
            SingleFileProfiler.EXCEPTION_TABLE.getColInfos());

    public static TableInfo EXCEPTION_TABLE_B = new TableInfo ("exceptions_b",
            SingleFileProfiler.EXCEPTION_TABLE.getColInfos());

    public static TableInfo ERROR_TABLE_A = new TableInfo("extract_errors_a",
            SingleFileProfiler.ERROR_TABLE.getColInfos());
    public static TableInfo ERROR_TABLE_B = new TableInfo("extract_errors_b",
            SingleFileProfiler.ERROR_TABLE.getColInfos());


    //need to parameterize?
    private final TikaConfig config = TikaConfig.getDefaultConfig();

    private final Path inputDir;
    private final Path extractDirA;
    private final Path extractDirB;

    private final long minJsonLength;
    private final long maxJsonLength;


    public FileComparer(ArrayBlockingQueue<FileResource> queue,
                        Path inputDir, Path extractDirA, Path extractDirB,
                        IDBWriter writer, long minJsonLength,
                        long maxJsonLength) {
        super(queue, writer);
        this.minJsonLength = minJsonLength;
        this.maxJsonLength = maxJsonLength;
        this.inputDir = inputDir;
        this.extractDirA = extractDirA;
        this.extractDirB = extractDirB;
    }

    @Override
    public boolean processFileResource(FileResource fileResource) {
        Metadata metadata = fileResource.getMetadata();
        EvalFilePaths fpsA = null;
        EvalFilePaths fpsB = null;

        if (inputDir != null && (inputDir.equals(extractDirA) ||
                inputDir.equals(extractDirB))) {
            //crawling an extract dir
            fpsA = getPathsFromExtractCrawl(metadata, extractDirA);
            fpsB = getPathsFromExtractCrawl(metadata, extractDirB);

        } else {
            fpsA = getPathsFromSrcCrawl(metadata, inputDir, extractDirA);
            fpsB = getPathsFromSrcCrawl(metadata, inputDir, extractDirB);
        }

            if (minJsonLength > -1) {
                //if both files exist and are < minJsonLength, skip em
                if (fpsA.getExtractFileLength() > NON_EXISTENT_FILE_LENGTH
                        && fpsA.getExtractFileLength() < minJsonLength
                        && fpsB.getExtractFileLength() > NON_EXISTENT_FILE_LENGTH
                        && fpsB.getExtractFileLength() < minJsonLength) {
                    return false;
                }
            }
            if (maxJsonLength > -1) {
                if ((fpsA.getExtractFileLength() > maxJsonLength) ||
                        (fpsB.getExtractFileLength() > maxJsonLength)) {
                    return false;
                }
            }


        try {
            compareFiles(fpsA, fpsB);
        } catch (Throwable e) {
            e.printStackTrace();
            //this should be cataclysmic...
            throw new RuntimeException("Exception while working on: " +
                    metadata.get(FSProperties.FS_REL_PATH), e);
        }
        return true;
    }

    //protected for testing, should find better way so that this can be private!
    protected void compareFiles(EvalFilePaths fpsA, EvalFilePaths fpsB) throws IOException {
        List<Metadata> metadataListA = getMetadata(fpsA.getExtractFile());
        List<Metadata> metadataListB = getMetadata(fpsB.getExtractFile());

        //array indices for those metadata items handled in
        //"that"
        Set<Integer> handledB = new HashSet<>();
        String containerID = Integer.toString(CONTAINER_ID.getAndIncrement());
        //container table
        Map<Cols, String> contData = new HashMap<>();
        contData.put(Cols.CONTAINER_ID, containerID);
        contData.put(Cols.FILE_PATH, fpsA.getRelativeSourceFilePath().toString());
        long srcFileLength = getSourceFileLength(metadataListA, metadataListB);
        contData.put(Cols.LENGTH,
                srcFileLength > NON_EXISTENT_FILE_LENGTH ?
                    Long.toString(srcFileLength) : "");
        contData.put(Cols.FILE_EXTENSION, fpsA.getRelativeSourceFilePath().getFileName().toString());
        long extractFileLengthA = getFileLength(fpsA.getExtractFile());
        contData.put(Cols.EXTRACT_FILE_LENGTH_A, extractFileLengthA > NON_EXISTENT_FILE_LENGTH ?
                Long.toString(extractFileLengthA) : "");

        long extractFileLengthB = getFileLength(fpsA.getExtractFile());
        contData.put(Cols.EXTRACT_FILE_LENGTH_B, extractFileLengthB > NON_EXISTENT_FILE_LENGTH ?
                Long.toString(extractFileLengthB) : "");

        writer.writeRow(COMPARISON_CONTAINERS, contData);

        if (metadataListA == null) {
            writeError(ERROR_TABLE_A, containerID, fpsA.getRelativeSourceFilePath().toString(),
                    fpsA.getExtractFile());
        }
        if (metadataListB == null) {
            writeError(ERROR_TABLE_B, containerID, fpsB.getRelativeSourceFilePath().toString(),
                    fpsB.getExtractFile());
        }

        if (metadataListA == null && metadataListB == null) {
            return;
        }
        List<Integer> numAttachmentsA = countAttachments(metadataListA);
        List<Integer> numAttachmentsB = countAttachments(metadataListB);

        //now get that metadata
        if (metadataListA != null) {
            for (int i = 0; i < metadataListA.size(); i++) {
                String fileId = Integer.toString(ID.getAndIncrement());
                Metadata metadataA = metadataListA.get(i);
                Metadata metadataB = null;
                //TODO: shouldn't be fileA!!!!
                writeProfileData(fpsA, i, metadataA, fileId, containerID, numAttachmentsA, PROFILES_A);
                writeExceptionData(fileId, metadataA, EXCEPTION_TABLE_A);
                int matchIndex = getMatch(i, metadataListA, metadataListB);

                if (matchIndex > -1) {
                    metadataB = metadataListB.get(matchIndex);
                    handledB.add(matchIndex);
                }
                if (metadataB != null) {
                    writeProfileData(fpsB, i, metadataB, fileId, containerID, numAttachmentsB, PROFILES_B);
                    writeExceptionData(fileId, metadataB, EXCEPTION_TABLE_B);
                }
                writeEmbeddedFilePathData(i, fileId, metadataA, metadataB);
                //prep the token counting
                Map<String, PairCount> tokens = new HashMap<>();
                TokenCounter tokenCounterA = new CounterA(tokens);
                TokenCounter tokenCounterB = new CounterB(tokens);
                //write content
                writeContentData(fileId, metadataA, tokenCounterA, CONTENTS_TABLE_A);
                writeContentData(fileId, metadataB, tokenCounterB, CONTENTS_TABLE_B);

                //now run comparisons
                if (tokenCounterA.getTokenCount() > 0 && tokenCounterB.getTokenCount() > 0) {
                    Map<Cols, String> data = new HashMap<>();
                    data.put(Cols.ID, fileId);
                    compareUnigramOverlap(tokens, tokenCounterA, tokenCounterB, data);
                    writer.writeRow(CONTENT_COMPARISONS, data);
                }
            }
        }
        //now try to get any Metadata objects in "that"
        //that haven't yet been handled.
        if (metadataListB != null) {
            for (int i = 0; i < metadataListB.size(); i++) {
                if (handledB.contains(i)) {
                    continue;
                }
                Metadata m = metadataListB.get(i);
                String fileId = Integer.toString(ID.getAndIncrement());
                writeProfileData(fpsB, i, m, fileId, containerID, numAttachmentsB, PROFILES_B);
                writeEmbeddedFilePathData(i, fileId, null, m);
                writeExceptionData(fileId, m, EXCEPTION_TABLE_B);
                //prep the token counting
                Map<String, PairCount> tokens = new HashMap<String, PairCount>();
                TokenCounter counter = new CounterB(tokens);
                countTokens(m, counter);
                writeContentData(fileId, m, counter, CONTENTS_TABLE_B);
            }
        }
    }

    private void writeEmbeddedFilePathData(int i, String fileId, Metadata mA, Metadata mB) {
        //container file, don't write anything
        if (i == 0) {
            return;
        }
        String pathA = null;
        String pathB = null;
        if (mA != null) {
            pathA = mA.get(RecursiveParserWrapper.EMBEDDED_RESOURCE_PATH);
        }
        if (mB != null) {
            pathB = mB.get(RecursiveParserWrapper.EMBEDDED_RESOURCE_PATH);
        }
        if (pathA != null) {
            Map<Cols, String> d = new HashMap<Cols, String>();
            d.put(Cols.ID, fileId);
            d.put(Cols.EMBEDDED_FILE_PATH, pathA);
            try {
                writer.writeRow(EMBEDDED_FILE_PATH_TABLE_A, d);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        if (pathB != null &&
                (pathA == null || ! pathA.equals(pathB))) {
            Map<Cols, String> d = new HashMap<>();
            d.put(Cols.ID, fileId);
            d.put(Cols.EMBEDDED_FILE_PATH, pathB);
            try {
                writer.writeRow(EMBEDDED_FILE_PATH_TABLE_B, d);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private long getSourceFileLength(List<Metadata> metadataListA, List<Metadata> metadataListB) {
        long len = getSourceFileLength(metadataListA);
        if (len > NON_EXISTENT_FILE_LENGTH) {
            return len;
        }
        return getSourceFileLength(metadataListB);
    }


    /**
     * Try to find the matching metadata based on the RecursiveParserWrapper.EMBEDDED_RESOURCE_PATH
     * If you can't find it, return -1;
     *
     * @param i                index for match in metadataListA
     * @param metadataListA
     * @param metadataListB
     * @return
     */

    private int getMatch(int i,
                         List<Metadata> metadataListA,
                         List<Metadata> metadataListB) {
        //TODO: could make this more robust
        if (metadataListB == null || metadataListB.size() == 0) {
            return -1;
        }
        if (i == 0) {
            return 0;
        }
        if (metadataListA.size() == metadataListB.size()) {
            //assume no rearrangments if lists are the same size
            return i;
        }

        Metadata thisMetadata = metadataListA.get(i);
        String embeddedPath = thisMetadata.get(RecursiveParserWrapper.EMBEDDED_RESOURCE_PATH);
        if (embeddedPath == null) {
            return -1;
        }
        if (i < metadataListB.size()) {
        }

        for (int j = 0; j < metadataListB.size(); j++) {
            String thatEmbeddedPath = metadataListB.get(j).get(
                    RecursiveParserWrapper.EMBEDDED_RESOURCE_PATH);
            if (embeddedPath.equals(thatEmbeddedPath)) {
                return j;
            }
        }
        return -1;
    }


    private void compareUnigramOverlap(Map<String, PairCount> tokens,
                                       TokenCounter theseTokens, TokenCounter thoseTokens,
                                       Map<Cols, String> data) throws IOException {

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

        data.put(Cols.DICE_COEFFICIENT,
                Float.toString(dice));
        data.put(Cols.OVERLAP, Float.toString(overlap));


        handleUniques(data, tokens, true);
        handleUniques(data, tokens, false);
        handleDiffs(data, tokens);
    }

    private void handleDiffs(Map<Cols, String> data, Map<String, PairCount> tokens) {
        if (tokens.size() == 0) {
            return;
        }
        int topNDiffs = 10;
        MutableValueAbsIntPriorityQueue bQueue = new MutableValueAbsIntPriorityQueue(topNDiffs);
        MutableValueAbsIntPriorityQueue aQueue = new MutableValueAbsIntPriorityQueue(topNDiffs);
        for (Map.Entry<String, PairCount> e : tokens.entrySet()) {
            int diff = e.getValue().b - e.getValue().a;
            if (diff > 0) {
                if (bQueue.top() == null || bQueue.size() < topNDiffs ||
                        diff >= bQueue.top().getValue()) {
                    bQueue.insertWithOverflow(new TokenIntPair(e.getKey(), diff));
                }
            } else if (diff < 0) {
                diff = Math.abs(diff);
                if (aQueue.top() == null || aQueue.size() < topNDiffs ||
                        diff >= aQueue.top().getValue()) {
                    aQueue.insertWithOverflow(new TokenIntPair(e.getKey(), diff));
                }
            }
        }

        List<TokenIntPair> tokenDiffs = new ArrayList<>();
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
        data.put(Cols.TOP_10_MORE_IN_A, sb.toString());


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
        data.put(Cols.TOP_10_MORE_IN_B, sb.toString());
    }

    private void handleUniques(Map<Cols, String> data,
                               Map<String, PairCount> tokens, boolean counterA) {
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
        List<TokenIntPair> tokenCounts = new ArrayList<>();
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
        if (counterA) {
            data.put(Cols.TOP_10_UNIQUE_TOKEN_DIFFS_A, sb.toString());
        } else {
            data.put(Cols.TOP_10_UNIQUE_TOKEN_DIFFS_B, sb.toString());
        }
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