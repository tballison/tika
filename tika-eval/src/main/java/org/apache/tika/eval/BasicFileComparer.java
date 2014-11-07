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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.cybozu.labs.langdetect.Language;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.z.ZCompressorInputStream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.icu.ICUFoldingFilter;
import org.apache.lucene.analysis.icu.segmentation.ICUTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.Version;
import org.apache.lucene.util.mutable.MutableValueInt;
import org.apache.tika.batch.BatchNoRestartError;
import org.apache.tika.batch.FileResource;
import org.apache.tika.batch.FileResourceConsumer;
import org.apache.tika.batch.fs.FSProperties;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.IOUtils;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.apache.tika.mime.MimeType;
import org.apache.tika.mime.MimeTypeException;
import org.apache.tika.mime.MimeTypes;
import org.apache.tika.parser.RecursiveParserWrapper;

public class BasicFileComparer extends FileResourceConsumer {

    public final String[] headers;

    //these remove runtime info from the stacktraces so
    //that actual causes can be counted.
    private final static Pattern CAUSED_BY_SNIPPER =
            Pattern.compile("(Caused by: [^:]+):[^\\r\\n]+");
    private final static Pattern OBJECT_ID_SNIPPER =
            Pattern.compile("(?s)^(org.apache.tika.exception.TikaException[^\\r\\n]+?)@[a-f0-9]+(\\s*[\\r\\n].*$)");

    private final static int MAX_STRING_LENGTH = 2000000;
    private final static int MAX_LEN_FOR_LANG_ID = 20000;
    private final static int TOP_N_WORDS = 10;

    private static AtomicInteger threadNum = new AtomicInteger(0);

    //good enough? or do we need to parameterize?
    private final TikaConfig config = TikaConfig.getDefaultConfig();
    private final JsonMetadataList serializer = new JsonMetadataList();
    private ThreadSafeCSVWrapper writer;

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
        thisExtension = thisRootDir.getName();
        thatExtension = thatRootDir.getName();
        headers = new String[]{
            "FILE_PATH",
            "JSON_EX_"+thisExtension,
            "JSON_EX_"+thatExtension,
            "ORIG_STACK_TRACE_"+thisExtension,
            "ORIG_STACK_TRACE_"+thatExtension,
            "SORT_STACK_TRACE_"+thisExtension,
            "SORT_STACK_TRACE_"+thatExtension,
            "FILE_EXTENSION",
            "DETECTED_CONTENT_TYPE_"+thisExtension,
            "DETECTED_CONTENT_TYPE_"+thatExtension,
            "DETECTED_FILE_EXTENSION_"+thisExtension,
            "DETECTED_FILE_EXTENSION_"+thatExtension,
            "NUM_ATTACHMENTS_"+thisExtension,
            "NUM_ATTACHMENTS_"+thatExtension,
            "NUM_META_VALUES_"+thisExtension,
            "NUM_META_VALUES_"+thatExtension,
            "MILLIS_"+thisExtension,
            "MILLIS_"+thatExtension,
            "NUM_UNIQUE_TOKENS_"+thisExtension,
            "NUM_UNIQUE_TOKENS_"+thatExtension,
            "DICE_COEFFICIENT",
            "TOKEN_COUNT_"+thisExtension,
            "TOKEN_COUNT_"+thatExtension,
            "OVERLAP",
            "TOP_N_WORDS_"+thisExtension,
            "TOP_N_WORDS_"+thatExtension,
            "NUM_EN_STOPS_TOP_N_"+thisExtension,
            "NUM_EN_STOPS_TOP_N_"+thatExtension,
            "LANG_ID_"+thisExtension,
            "LANG_ID_PROB_"+thisExtension,
            "LANG_ID_"+thatExtension,
            "LANG_ID_PROB_"+thatExtension,
        };

    }
    public void setWriter(ThreadSafeCSVWrapper writer) {
        this.writer = writer;
        //stinky
        if (threadNum.getAndIncrement() == 0) {
            List<String> headerList = Arrays.asList(headers);
            writer.printRecord(headerList);
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
            compareFiles(thisFile, thatFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    protected void compareFiles(File thisFile, File thatFile) throws IOException {
        List<Metadata> thisMetadata = getMetadata(thisFile, serializer);
        List<Metadata> thatMetadata = getMetadata(thatFile, serializer);
        Map<String, String> output = new HashMap<String, String>();
        output.put(
                "FILE_PATH",
                thisFile.getAbsolutePath().substring(thisDirLen));
        if (thisMetadata == null) {
            output.put("JSON_EX_"+thisExtension,
                    "Error with json parsing");
        }
        if (thatMetadata == null) {
            output.put("JSON_EX_"+thatExtension,
                    "Error with json parsing");
        }

        getExceptionStrings(thisMetadata, thisExtension, output);
        getExceptionStrings(thatMetadata, thatExtension, output);

        output.put("FILE_EXTENSION",
                getOriginalFileExtension(thisFile.getName()));

        getFileTypes(thisMetadata, thisExtension, output);
        getFileTypes(thatMetadata, thatExtension, output);

        if (thisMetadata != null) {
            output.put("NUM_ATTACHMENTS_"+thisExtension,
                Integer.toString(thisMetadata.size()-1));
        }
        if (thatMetadata != null) {
            output.put("NUM_ATTACHMENTS_"+thatExtension,
                Integer.toString(thatMetadata.size()-1));
        }
        output.put("NUM_META_VALUES_"+thisExtension,
                countMetadataValues(thisMetadata));

        output.put("NUM_META_VALUES_"+thatExtension,
                countMetadataValues(thatMetadata));
        output.put("MILLIS_"+thisExtension,
                getTime(thisMetadata));
        output.put("MILLIS_"+thatExtension,
                getTime(thatMetadata));

        compareUnigramOverlap(thisMetadata, thatMetadata, output);
        printRecord(output);
    }

    private void getFileTypes(List<Metadata> metadata, String extension, Map<String, String> output) {
        if (metadata == null || metadata.size() == 0) {
            return;
        }
        String type = metadata.get(0).get(Metadata.CONTENT_TYPE);
        if (type == null) {
            return;
        }
        output.put("DETECTED_CONTENT_TYPE_"+extension, type);

        try{
            MimeTypes types = config.getMimeRepository();
            MimeType mime = types.forName(type);
            String ext = mime.getExtension();
            if (ext.startsWith(".")){
                ext = ext.substring(1);
            }
            output.put("DETECTED_FILE_EXTENSION_"+extension, ext);
        } catch (MimeTypeException e) {
            //swallow
        }

    }

    public void printRecord(Map<String, String> data) {
        List<String> row = new ArrayList<String>();
        for (String h : headers) {
            String c = data.get(h);
            if (c == null) {
                c = "";
            }
            row.add(c);
        }
        writer.printRecord(row);
    }


    private void getExceptionStrings(List<Metadata> metadataList, String extension, Map<String, String> data) {

        if (metadataList == null) {
            return;
        }
        for (Metadata m : metadataList) {
            String exc = m.get(RecursiveParserWrapper.PARSE_EXCEPTION);
            if (exc != null) {
                data.put("ORIG_STACK_TRACE_"+extension, exc);
                //TikaExceptions can have object ids, as in the "@2b1ea6ee" in:
                //org.apache.tika.exception.TikaException: TIKA-198: Illegal
                // IOException from org.apache.tika.parser.microsoft.OfficeParser@2b1ea6ee
                //For reporting purposes, let's snip off the object id so that we can more
                //easily count exceptions.
                Matcher matcher = OBJECT_ID_SNIPPER.matcher(exc);
                if (matcher.matches()) {
                    exc = matcher.group(1) + matcher.group(2);
                }
                matcher = CAUSED_BY_SNIPPER.matcher(exc);
                exc = matcher.replaceAll("$1");
                data.put("SORT_STACK_TRACE_"+extension, exc);
            }
        }
    }

    private List<Metadata> getMetadata(File thisFile, JsonMetadataList serializer) {
        Reader reader = null;
        List<Metadata> metadataList = null;
        try {
            InputStream is = new FileInputStream(thisFile);
            if (thisFile.getName().endsWith("bz2")) {
                is = new BZip2CompressorInputStream(is);
            } else if (thisFile.getName().endsWith("gz")) {
                is = new GzipCompressorInputStream(is);
            } else if (thisFile.getName().endsWith("zip")) {
                is = new ZCompressorInputStream(is);
            }
            reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            metadataList = serializer.fromJson(reader);
        } catch (IOException e) {
            //log
        } catch (TikaException e) {
            //log
        } finally {
            IOUtils.closeQuietly(reader);
        }
        return metadataList;
    }
    private String getOriginalFileExtension(String fName) {
        Matcher m = Pattern.compile("\\.([^\\.]+)\\.json(?:\\.(?:bz2|gz(?:ip)?|zip))?$").matcher(fName);
        if (m.find()) {
            return m.group(1);
        }
        return "";
    }

    private String getTime(List<Metadata> metadatas) {
        if (metadatas == null) {
            return "";
        }
        String elapsed = "-1";

        for (Metadata m : metadatas) {
            String v = m.get(RecursiveParserWrapper.PARSE_TIME_MILLIS);
            if (v != null) {
                return v;
            }
        }
        return elapsed;
    }


    private String countMetadataValues(List<Metadata> metadatas) {
        if (metadatas == null) {
            return "";
        }

        int i = 0;
        for (Metadata m : metadatas) {
            for (String n : m.names()) {
                for (String v : m.getValues(n)) {
                    i++;
                }
            }
        }
        return Integer.toString(i);
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
        data.put("NUM_UNIQUE_TOKENS_"+thisExtension,
                Integer.toString(theseTokens.size()));
        data.put("NUM_UNIQUE_TOKENS_"+thatExtension,
                Integer.toString(thoseTokens.size()));
        data.put("DICE_COEFFICIENT",
                Float.toString(dice));
        data.put("OVERLAP", Float.toString(overlap));

        data.put("TOKEN_COUNT_"+thisExtension, Integer.toString(tokenCountThis));
        data.put("TOKEN_COUNT_"+thatExtension, Integer.toString(tokenCountThat));

        handleWordCounts(data, theseTokens, thisExtension);
        handleWordCounts(data, thoseTokens, thatExtension);
    }

    private void langid(String content, String extension, Map<String, String> data) {
        if (content.length() < 200) {
            return;
        }
        String s = content;
        if (content.length() > MAX_LEN_FOR_LANG_ID) {
            s = content.substring(0, MAX_LEN_FOR_LANG_ID);
        }
        Detector detector = null;
        try {
            detector = DetectorFactory.create();
        } catch (LangDetectException e) {
            throw new BatchNoRestartError(e);
        }

        detector.append(s);
        String lang = null;
        double prob = -1.0;
        try {
            List<Language> probabilities = detector.getProbabilities();
            if (probabilities.size() > 0) {
                lang = probabilities.get(0).lang;
                prob = probabilities.get(0).prob;
            }
        } catch (LangDetectException e) {
            //log
        }
        data.put("LANG_ID_"+extension, lang);
        data.put("LANG_ID_PROB_"+extension, Double.toString(prob));
    }

    private void handleWordCounts(Map<String, String> data,
                                  Map<String, MutableValueInt> tokens, String extension) {
        MutableValueIntPriorityQueue queue = new MutableValueIntPriorityQueue(TOP_N_WORDS);
        for (Map.Entry<String, MutableValueInt> e : tokens.entrySet()) {
            if (queue.top() == null || queue.size() < TOP_N_WORDS ||
                    e.getValue().value >= queue.top().value){
                queue.insertWithOverflow(new TermIntPair(e.getKey(), e.getValue().value));
            }
        }

        StringBuilder sb = new StringBuilder();
        TermIntPair term = queue.pop();
        List<TermIntPair> terms = new ArrayList<TermIntPair>();
        while (term != null) {
            terms.add(0, term);
            term = queue.pop();
        }
        CharArraySet en_stops = StandardAnalyzer.STOP_WORDS_SET;
        int stops = 0;
        int i = 0;
        for (TermIntPair t : terms) {
            if (i++ > 0) {
                sb.append(" | ");
            }
            sb.append(t.term + ": " + t.value);
            if (en_stops.contains(t.term)){
                stops++;
            }
            term = queue.pop();
        }

        data.put("TOP_N_WORDS_"+extension, sb.toString());
        data.put("NUM_EN_STOPS_TOP_N_"+extension, Integer.toString(stops));
    }

    private Map<String, MutableValueInt> getTokens(String s) throws IOException {
        if (s == null || s.equals("")) {
            return new HashMap<String, MutableValueInt>();
        }

        Analyzer analyzer = new ICUAnalyzer(Version.LUCENE_4_9, CharArraySet.EMPTY_SET);
        Map<String, MutableValueInt> m = new HashMap<String, MutableValueInt>();
        TokenStream stream = analyzer.tokenStream("", s);
        stream.reset();
        CharTermAttribute attr = stream.getAttribute(CharTermAttribute.class);
        while (stream.incrementToken()) {
            String t = attr.toString();
            MutableValueInt i = m.get(t);
            if (i == null) {
                i = new MutableValueInt();
                i.value = 0;
            }
            i.value++;
            m.put(t, i);
        }
        stream.end();
        stream.close();
        return m;
    }

    private String getContent(List<Metadata> metadataList) {
        if (metadataList == null) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        for (Metadata m : metadataList) {
            String c = m.get(RecursiveParserWrapper.TIKA_CONTENT);
            if (c != null) {
                sb.append(c);
                sb.append("\n\n");
                if (sb.length() > MAX_STRING_LENGTH) {
                    sb.setLength(MAX_STRING_LENGTH);
                    break;
                }
            }
        }
        return sb.toString();
    }


    private class ICUAnalyzer extends Analyzer {

        private CharArraySet stopWords = null;
        private final Version version;

        public ICUAnalyzer(Version version) {
            this.version = version;
        }

        private ICUAnalyzer(Version version, CharArraySet stopWords) {
            this(version);
            this.stopWords = stopWords;
        }

        @Override
        protected TokenStreamComponents createComponents(String field, Reader reader) {
            Tokenizer stream = new ICUTokenizer(reader);
            TokenFilter icu = new ICUFoldingFilter(stream);
            if (stopWords != null && stopWords.size() > 0) {
                TokenFilter stop = new StopFilter(version, icu, stopWords);
                return new TokenStreamComponents(stream, stop);
            }
            return new TokenStreamComponents(stream, icu);

        }
    }

    public class MutableValueIntPriorityQueue extends PriorityQueue<TermIntPair> {

        public MutableValueIntPriorityQueue(int maxSize) {
            super(maxSize);

        }

        @Override
        protected boolean lessThan(TermIntPair arg0, TermIntPair arg1) {
            if (arg0.value < arg1.value){
                return true;
            }
            return false;
        }
    }

    private class TermIntPair {
        private final String term;
        private final int value;

        private TermIntPair(String term, int value) {
            this.term = term;
            this.value = value;
        }
    }

}
