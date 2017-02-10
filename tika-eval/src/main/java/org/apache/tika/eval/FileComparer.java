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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.commons.io.FilenameUtils;
import org.apache.tika.batch.FileResource;
import org.apache.tika.batch.fs.FSProperties;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.eval.db.ColInfo;
import org.apache.tika.eval.db.Cols;
import org.apache.tika.eval.db.TableInfo;
import org.apache.tika.eval.io.ExtractReader;
import org.apache.tika.eval.io.IDBWriter;
import org.apache.tika.eval.tokens.ContrastStatistics;
import org.apache.tika.eval.tokens.TokenContraster;
import org.apache.tika.eval.tokens.TokenIntPair;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.RecursiveParserWrapper;

public class FileComparer extends AbstractProfiler {

    private final static String FIELD_A = "fa";
    private final static String FIELD_B = "fb";

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
    private final ExtractReader.ALTER_METADATA_LIST alterMetadataList;

    private final TokenContraster tokenContraster = new TokenContraster();
    private final ExtractReader extractReader = new ExtractReader();

    public FileComparer(ArrayBlockingQueue<FileResource> queue,
                        Path inputDir, Path extractDirA, Path extractDirB,
                        IDBWriter writer, long minJsonLength,
                        long maxJsonLength, ExtractReader.ALTER_METADATA_LIST alterMetadataList) {
        super(queue, writer);
        this.minJsonLength = minJsonLength;
        this.maxJsonLength = maxJsonLength;
        this.inputDir = inputDir;
        this.extractDirA = extractDirA;
        this.extractDirB = extractDirB;
        this.alterMetadataList = alterMetadataList;
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

        List<Metadata> metadataListA =
                extractReader.loadExtract(fpsA.getExtractFile(), alterMetadataList);
        List<Metadata> metadataListB =
                extractReader.loadExtract(fpsB.getExtractFile(), alterMetadataList);

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
        contData.put(Cols.FILE_EXTENSION,
                FilenameUtils.getExtension(fpsA.getRelativeSourceFilePath().getFileName().toString()));

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
                tokenCounter.clear(FIELD_A);
                tokenCounter.clear(FIELD_B);
                //write content
                try {
                    writeContentData(fileId, metadataA, FIELD_A, CONTENTS_TABLE_A);
                    writeContentData(fileId, metadataB, FIELD_B, CONTENTS_TABLE_B);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                //now run comparisons
                if (tokenCounter.getTokenStatistics(FIELD_A).getTotalTokens() > 0
                        && tokenCounter.getTokenStatistics(FIELD_B).getTotalTokens() > 0) {
                    Map<Cols, String> data = new HashMap<>();
                    data.put(Cols.ID, fileId);

                    ContrastStatistics contrastStatistics =
                            tokenContraster.calculateContrastStatistics(
                            tokenCounter.getTokens(FIELD_A),
                            tokenCounter.getTokenStatistics(FIELD_A),
                            tokenCounter.getTokens(FIELD_B),
                            tokenCounter.getTokenStatistics(FIELD_B));

                    writeContrasts(data, contrastStatistics);
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
                Metadata metadataB = metadataListB.get(i);
                String fileId = Integer.toString(ID.getAndIncrement());
                writeProfileData(fpsB, i, metadataB, fileId, containerID, numAttachmentsB, PROFILES_B);
                writeEmbeddedFilePathData(i, fileId, null, metadataB);
                writeExceptionData(fileId, metadataB, EXCEPTION_TABLE_B);

                //prep the token counting
                tokenCounter.clear(FIELD_B);
                //write content
                try {
                    writeContentData(fileId, metadataB, FIELD_B, CONTENTS_TABLE_B);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
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
            Map<Cols, String> d = new HashMap<>();
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




    private void writeContrasts(Map<Cols, String> data, ContrastStatistics contrastStatistics) {
        writeContrastString(data, Cols.TOP_10_MORE_IN_A, contrastStatistics.getTopNMoreA());
        writeContrastString(data, Cols.TOP_10_MORE_IN_B, contrastStatistics.getTopNMoreB());
        writeContrastString(data, Cols.TOP_10_UNIQUE_TOKEN_DIFFS_A, contrastStatistics.getTopNUniqueA());
        writeContrastString(data, Cols.TOP_10_UNIQUE_TOKEN_DIFFS_B, contrastStatistics.getTopNUniqueB());
        data.put(Cols.OVERLAP, Double.toString(contrastStatistics.getOverlap()));
        data.put(Cols.DICE_COEFFICIENT, Double.toString(contrastStatistics.getDiceCoefficient()));

    }

    private void writeContrastString(Map<Cols, String> data, Cols col, TokenIntPair[] tokenIntPairs) {

        int i = 0;
        StringBuilder sb = new StringBuilder();
        for (TokenIntPair p : tokenIntPairs) {
            if (i++ > 0) {
                sb.append(" | ");
            }
            sb.append(p.getToken()).append(": ").append(p.getValue());
        }
        data.put(col, sb.toString());
    }
}