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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
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
import org.apache.tika.batch.BatchNoRestartException;
import org.apache.tika.batch.FileResource;
import org.apache.tika.batch.FileResourceConsumer;
import org.apache.tika.batch.fs.FSProperties;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.IOUtils;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.apache.tika.parser.RecursiveParserWrapper;

public class BasicFileComparer extends FileResourceConsumer {

    public final String[] headers;

    private final static int MAX_STRING_LENGTH = 2000000;
    private final static int MAX_LEN_FOR_LANG_ID = 20000;
    private final static int TOP_N_WORDS = 10;

    private final JsonMetadataList serializer = new JsonMetadataList();
    private ThreadSafeCSVWrapper writer;

    private final File thisRootDir;
    private final File thatRootDir;
    private final String thisSuffix;
    private final String thatSuffix;
    private final int thisDirLen;

    public BasicFileComparer(ArrayBlockingQueue<FileResource> queue, File thisRootDir, File thatRootDir) {
        super(queue);
        this.thisRootDir = thisRootDir;
        this.thatRootDir = thatRootDir;
        thisDirLen = thisRootDir.getAbsolutePath().length()+1;
        thisSuffix = thisRootDir.getName();
        thatSuffix = thatRootDir.getName();
        headers = new String[]{
            "FILE_PATH",
            "JSON_EX_"+thisSuffix,
            "JSON_EX_"+thatSuffix,
            "STACK_TRACE_"+thisSuffix,
            "STACK_TRACE_"+thatSuffix,
            "FILE_SUFFIX",
            "NUM_ATTACHMENTS_"+thisSuffix,
            "NUM_ATTACHMENTS_"+thatSuffix,
            "NUM_META_VALUES_"+thisSuffix,
            "NUM_META_VALUES_"+thatSuffix,
            "MILLIS_"+thisSuffix,
            "MILLIS_"+thatSuffix,
            "NUM_UNIQUE_WORDS_"+thisSuffix,
            "NUM_UNIQUE_WORDS_"+thatSuffix,
            "TOP_N_WORDS_"+thisSuffix,
            "TOP_N_WORDS_"+thatSuffix,
            "NUM_EN_STOPS_TOP_N_"+thisSuffix,
            "NUM_EN_STOPS_TOP_N_"+thatSuffix,
            "LANG_ID_"+thisSuffix,
            "LANG_ID_PROB"+thisSuffix,
            "LANG_ID_"+thatSuffix,
            "LANG_ID_PROB"+thatSuffix,
            "DICE_COEFFICIENT"
        };
    }
    public void setWriter(ThreadSafeCSVWrapper writer) {
        this.writer = writer;
    }

    public static void setLangModelDir(File langModelDir) {
        try {
            DetectorFactory.loadProfile(langModelDir);
        } catch (LangDetectException e) {
            throw new BatchNoRestartException(e);
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
            output.put("JSON_EX_"+thisSuffix,
                    "Error with json parsing");
        }
        if (thatMetadata == null) {
            output.put("JSON_EX_"+thatSuffix,
                    "Error with json parsing");
        }

        output.put("STACK_TRACE_"+thisSuffix,
                getExceptionString(thisMetadata));
        output.put("STACK_TRACE_"+thatSuffix,
                getExceptionString(thatMetadata));

        output.put("FILE_SUFFIX",
                getOriginalFileSuffix(thisFile.getName()));

        if (thisMetadata != null) {
            output.put("NUM_ATTACHMENTS_"+thisSuffix,
                Integer.toString(thisMetadata.size()-1));
        }
        if (thatMetadata != null) {
            output.put("NUM_ATTACHMENTS_"+thatSuffix,
                Integer.toString(thatMetadata.size()-1));
        }
        output.put("NUM_META_VALUES_"+thisSuffix,
                countMetadataValues(thisMetadata));

        output.put("NUM_META_VALUES_"+thatSuffix,
                countMetadataValues(thatMetadata));
        output.put("MILLIS_"+thisSuffix,
                getTime(thisMetadata));
        output.put("MILLIS_"+thatSuffix,
                getTime(thatMetadata));

        compareUnigramOverlap(thisMetadata, thatMetadata, output);
        printRecord(output);
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


    private String getExceptionString(List<Metadata> metadataList) {
        if (metadataList == null) {
            return "";
        }
        for (Metadata m : metadataList) {
            String exc = m.get(RecursiveParserWrapper.PARSE_EXCEPTION);
            if (exc != null) {
                //TikaExceptions can have object ids, as in the "@2b1ea6ee" in:
                //org.apache.tika.exception.TikaException: TIKA-198: Illegal
                // IOException from org.apache.tika.parser.microsoft.OfficeParser@2b1ea6ee
                //For reporting purposes, let's snip off the object id so that we can more
                //easily count exceptions.
                Matcher objectIdSnipper =
                        Pattern.compile("(?s)^(org.apache.tika.exception.TikaException[^\\r\\n]+?)@[a-f0-9]+(\\s*[\\r\\n].*$)").matcher(exc);
                if (objectIdSnipper.matches()) {
                    exc = objectIdSnipper.group(1) + objectIdSnipper.group(2);
                }
                return exc;
            }
        }
        return "";

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
    private String getOriginalFileSuffix(String fName) {
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
        langid(content, thisSuffix, data);
        Map<String, MutableValueInt> theseTokens = getTokens(content);
        content = getContent(thatMetadata);
        langid(content, thatSuffix, data);
        Map<String, MutableValueInt> thoseTokens = getTokens(content);

        int denom = theseTokens.size() + thoseTokens.size();
        int overlap = 0;
        for (String k : theseTokens.keySet()) {
            if (thoseTokens.containsKey(k)) {
                overlap += 2;
            }
        }

        float div = (float) overlap / (float) denom;
        data.put("NUM_UNIQUE_WORDS_"+thisSuffix,
                Integer.toString(theseTokens.size()));
        data.put("NUM_UNIQUE_WORDS_"+thatSuffix,
                Integer.toString(thoseTokens.size()));
        data.put("DICE_COEFFICIENT",
                Float.toString(div));
        handleWordCounts(data, theseTokens, thisSuffix);
        handleWordCounts(data, thoseTokens, thatSuffix);
    }

    private void langid(String content, String suffix, Map<String, String> data) {
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
            throw new BatchNoRestartException(e);
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
        data.put("LANG_ID_"+suffix, lang);
        data.put("LANG_ID_PROB_"+suffix, Double.toString(prob));
    }

    private void handleWordCounts(Map<String, String> data,
                                  Map<String, MutableValueInt> tokens, String suffix) {
        MutableValueIntPriorityQueue queue = new MutableValueIntPriorityQueue(TOP_N_WORDS);
        for (Map.Entry<String, MutableValueInt> e : tokens.entrySet()) {
            if (queue.top() == null || queue.size() < TOP_N_WORDS ||
                    e.getValue().value >= queue.top().value){
                queue.insertWithOverflow(new TermIntPair(e.getKey(), e.getValue().value));
            }
        }

        StringBuilder sb = new StringBuilder();
        TermIntPair term = queue.pop();
        int i = 0;
        CharArraySet en_stops = StandardAnalyzer.STOP_WORDS_SET;
        int stops = 0;
        while (term != null){
            if (i++ > 0) {
                sb.append(" | ");
            }
            sb.append(term.term+": "+term.value);
            if (en_stops.contains(term)){
                stops++;
            }
            term = queue.pop();
        }

        data.put("TOP_N_WORDS_"+suffix, sb.toString());
        data.put("NUM_EN_STOPS_TOP_N_"+suffix, Integer.toString(stops));

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
