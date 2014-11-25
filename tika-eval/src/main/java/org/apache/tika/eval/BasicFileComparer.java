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

import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import org.apache.lucene.util.mutable.MutableValueInt;
import org.apache.tika.batch.BatchNoRestartError;
import org.apache.tika.batch.FileResource;
import org.apache.tika.batch.fs.FSProperties;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MimeType;
import org.apache.tika.mime.MimeTypeException;
import org.apache.tika.mime.MimeTypes;

public class BasicFileComparer extends AbstractProfiler {

    public enum COMPARISON_HEADERS  {
        DIFF_NUM_ATTACHMENTS,
        DIFF_NUM_METADATA_VALUES,
        DICE_COEFFICIENT,
        OVERLAP,
    };
    public final List<String> headers;


    //good enough? or do we need to parameterize?
    private final TikaConfig config = TikaConfig.getDefaultConfig();

    private final File thisRootDir;
    private final File thatRootDir;
    private final String thisExtension;
    private final String thatExtension;
    private final int thisDirLen;

    public BasicFileComparer(ArrayBlockingQueue<FileResource> queue, File thisRootDir, File thatRootDir) {
        super(queue);
        this.thisRootDir = thisRootDir;
        this.thatRootDir = thatRootDir;
        thisDirLen = thisRootDir.getAbsolutePath().length()+1;
        thisExtension = "_"+thisRootDir.getName();
        thatExtension = "_"+thatRootDir.getName();
        String[] headerArr = new String[]{
            HEADERS.FILE_PATH.name(),
            HEADERS.JSON_EX+thisExtension,
            HEADERS.JSON_EX+thatExtension,
            HEADERS.ORIG_STACK_TRACE+thisExtension,
            HEADERS.ORIG_STACK_TRACE+thatExtension,
            HEADERS.SORT_STACK_TRACE+thisExtension,
            HEADERS.SORT_STACK_TRACE+thatExtension,
            HEADERS.FILE_EXTENSION.name(),
            HEADERS.DETECTED_CONTENT_TYPE+thisExtension,
            HEADERS.DETECTED_CONTENT_TYPE+thatExtension,
            HEADERS.DETECTED_FILE_EXTENSION+thisExtension,
            HEADERS.DETECTED_FILE_EXTENSION+thatExtension,
            HEADERS.NUM_ATTACHMENTS+thisExtension,
            HEADERS.NUM_ATTACHMENTS+thatExtension,
            COMPARISON_HEADERS.DIFF_NUM_ATTACHMENTS.name(),
            HEADERS.NUM_METADATA_VALUES+thisExtension,
            HEADERS.NUM_METADATA_VALUES+thatExtension,
            COMPARISON_HEADERS.DIFF_NUM_METADATA_VALUES.name(),
            HEADERS.ELAPSED_TIME_MILLIS+thisExtension,
            HEADERS.ELAPSED_TIME_MILLIS+thatExtension,
            HEADERS.NUM_UNIQUE_TOKENS+thisExtension,
            HEADERS.NUM_UNIQUE_TOKENS+thatExtension,
            COMPARISON_HEADERS.DICE_COEFFICIENT.name(),
            HEADERS.TOKEN_COUNT+thisExtension,
            HEADERS.TOKEN_COUNT+thatExtension,
            COMPARISON_HEADERS.OVERLAP.name(),
            HEADERS.TOP_N_WORDS+thisExtension,
            HEADERS.TOP_N_WORDS+thatExtension,
            HEADERS.NUM_EN_STOPS_TOP_N+thisExtension,
            HEADERS.NUM_EN_STOPS_TOP_N+thatExtension,
            HEADERS.LANG_ID1+thisExtension,
            HEADERS.LANG_ID_PROB1+thisExtension,
            HEADERS.LANG_ID1+thatExtension,
            HEADERS.LANG_ID_PROB1+thatExtension,
        };
        headers = new ArrayList<String>();
        for (String s : headerArr) {
            headers.add(s);
        }
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
            compareFiles(relativePath, thisFile, thatFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    public Iterable<String> getHeaders() {
        return headers;
    }

    protected void compareFiles(String relativePath, File thisFile, File thatFile) throws IOException {
        List<Metadata> thisMetadata = getMetadata(thisFile);
        List<Metadata> thatMetadata = getMetadata(thatFile);
        Map<String, String> output = new HashMap<String, String>();
        output.put(HEADERS.FILE_PATH.name(), relativePath);

        if (thisMetadata == null) {
            output.put(HEADERS.JSON_EX+thisExtension,
                    "Error with json parsing");
        }
        if (thatMetadata == null) {
            output.put(HEADERS.JSON_EX+thatExtension,
                    "Error with json parsing");
        }

        getExceptionStrings(thisMetadata, thisExtension, output);
        getExceptionStrings(thatMetadata, thatExtension, output);

        output.put(HEADERS.FILE_EXTENSION.name(),
                getOriginalFileExtension(thisFile.getName()));

        getFileTypes(thisMetadata, thisExtension, output);
        getFileTypes(thatMetadata, thatExtension, output);

        int thisNumAttachments = (thisMetadata==null) ? 0 : thisMetadata.size()-1;
        int thatNumAttachments = (thatMetadata==null) ? 0: thatMetadata.size()-1;
        output.put(HEADERS.NUM_ATTACHMENTS+thisExtension,
            Integer.toString(thisNumAttachments));
        output.put(HEADERS.NUM_ATTACHMENTS+thatExtension,
            Integer.toString(thatNumAttachments));

        output.put(COMPARISON_HEADERS.DIFF_NUM_ATTACHMENTS.name(),
                Integer.toString(thatNumAttachments-thisNumAttachments));

        int thisNumMetadataValues = countMetadataValues(thisMetadata);
        int thatNumMetadataValues = countMetadataValues(thatMetadata);
        output.put(HEADERS.NUM_METADATA_VALUES+thisExtension,
                Integer.toString(thisNumMetadataValues));

        output.put(HEADERS.NUM_METADATA_VALUES+thatExtension,
                Integer.toString(thatNumMetadataValues));

        output.put(COMPARISON_HEADERS.DIFF_NUM_METADATA_VALUES.name(),
            Integer.toString(thatNumMetadataValues-thisNumMetadataValues));

        output.put(HEADERS.ELAPSED_TIME_MILLIS+thisExtension,
                getTime(thisMetadata));
        output.put(HEADERS.ELAPSED_TIME_MILLIS+thatExtension,
                getTime(thatMetadata));

        compareUnigramOverlap(thisMetadata, thatMetadata, output);
        writer.writeRow(output, getHeaders());
    }

    private void getFileTypes(List<Metadata> metadata, String extension, Map<String, String> output) {
        if (metadata == null || metadata.size() == 0) {
            return;
        }
        String type = metadata.get(0).get(Metadata.CONTENT_TYPE);
        if (type == null) {
            return;
        }
        output.put(HEADERS.DETECTED_CONTENT_TYPE+extension, type);

        try{
            MimeTypes types = config.getMimeRepository();
            MimeType mime = types.forName(type);
            String ext = mime.getExtension();
            if (ext.startsWith(".")){
                ext = ext.substring(1);
            }
            output.put(HEADERS.DETECTED_FILE_EXTENSION+extension, ext);
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
        for (Map.Entry<String, MutableValueInt> e : theseTokens.entrySet()) {
            MutableValueInt thatCount = thoseTokens.get(e.getKey());
            if (thatCount != null) {
                diceNum += 2;
                overlapNum += 2*Math.min(e.getValue().value, thatCount.value);
            }
            tokenCountThis += e.getValue().value;
        }

        for (MutableValueInt thatCount : thoseTokens.values()) {
            tokenCountThat += thatCount.value;
        }
        float dice = (float) diceNum / (float) diceDenom;
        float overlap = (float) overlapNum / (float)(tokenCountThis+tokenCountThat);
        data.put(HEADERS.NUM_UNIQUE_TOKENS+thisExtension,
                Integer.toString(theseTokens.size()));
        data.put(HEADERS.NUM_UNIQUE_TOKENS+thatExtension,
                Integer.toString(thoseTokens.size()));
        data.put(COMPARISON_HEADERS.DICE_COEFFICIENT.name(),
                Float.toString(dice));
        data.put(COMPARISON_HEADERS.OVERLAP.name(), Float.toString(overlap));

        data.put(HEADERS.TOKEN_COUNT+thisExtension, Integer.toString(tokenCountThis));
        data.put(HEADERS.TOKEN_COUNT+thatExtension, Integer.toString(tokenCountThat));

        handleWordCounts(data, theseTokens, thisExtension);
        handleWordCounts(data, thoseTokens, thatExtension);
    }
}
