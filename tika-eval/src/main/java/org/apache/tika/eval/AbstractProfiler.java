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
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.IOUtils;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.apache.tika.parser.RecursiveParserWrapper;

public abstract class AbstractProfiler extends FileResourceConsumer {

    enum HEADERS {
        FILE_PATH,
        FILE_EXTENSION,
        JSON_EX,
        ORIG_STACK_TRACE,
        SORT_STACK_TRACE,
        DETECTED_CONTENT_TYPE,
        DETECTED_FILE_EXTENSION,
        ELAPSED_TIME_MILLIS,
        NUM_METADATA_VALUES,
        NUM_ATTACHMENTS,
        TOKEN_COUNT,
        NUM_UNIQUE_TOKENS,
        TOP_N_WORDS,
        NUM_EN_STOPS_TOP_N,
        LANG_ID1,
        LANG_ID_PROB1,
        LANG_ID2,
        LANG_ID_PROB2,
        LANG_ID3,
        LANG_ID_PROB3,
    };

    private static AtomicInteger threadNum = new AtomicInteger(0);

    final static int MAX_STRING_LENGTH = 2000000;
    final static int MAX_LEN_FOR_LANG_ID = 20000;
    final static int TOP_N_WORDS = 10;

    //these remove runtime info from the stacktraces so
    //that actual causes can be counted.
    private final static Pattern CAUSED_BY_SNIPPER =
            Pattern.compile("(Caused by: [^:]+):[^\\r\\n]+");
    private final static Pattern OBJECT_ID_SNIPPER =
            Pattern.compile("(?s)^(org.apache.tika.exception.TikaException[^\\r\\n]+?)@[a-f0-9]+(\\s*[\\r\\n].*$)");

    private JsonMetadataList serializer = new JsonMetadataList();
    protected TableWriter writer;


    public AbstractProfiler(ArrayBlockingQueue<FileResource> fileQueue) {
        super(fileQueue);
    }

    List<Metadata> getMetadata(File thisFile) {
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

    public void setTableWriter(TableWriter writer) {
        this.writer = writer;
        if (threadNum.getAndIncrement() == 0) {
            writer.writeRow(getHeaders());
        }
    }

    public abstract Iterable<String> getHeaders();

    public static void setLangModelDir(File langModelDir) {
        try {
            DetectorFactory.loadProfile(langModelDir);
        } catch (LangDetectException e) {
            throw new BatchNoRestartError(e);
        }
    }

    String getContent(List<Metadata> metadataList) {
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


    String getOriginalFileExtension(String fName) {
        Matcher m = Pattern.compile("\\.([^\\.]+)\\.json(?:\\.(?:bz2|gz(?:ip)?|zip))?$").matcher(fName);
        if (m.find()) {
            return m.group(1);
        }
        return "";
    }

    String getTime(List<Metadata> metadatas) {
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


    int countMetadataValues(List<Metadata> metadatas) {
        if (metadatas == null) {
            return 0;
        }

        int i = 0;
        for (Metadata m : metadatas) {
            for (String n : m.names()) {
                i += m.getValues(n).length;
            }
        }
        return i;
    }

    void getExceptionStrings(List<Metadata> metadataList, String extension, Map<String, String> data) {

        if (metadataList == null) {
            return;
        }
        for (Metadata m : metadataList) {
            String exc = m.get(RecursiveParserWrapper.PARSE_EXCEPTION);
            if (exc != null) {
                data.put(HEADERS.ORIG_STACK_TRACE+extension, exc);
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
                data.put(HEADERS.SORT_STACK_TRACE+extension, exc);
            }
        }
    }

    void langid(String content, String extension, Map<String, String> data) {
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
                data.put(HEADERS.LANG_ID1+extension, probabilities.get(0).lang);
                data.put(HEADERS.LANG_ID_PROB1+extension,
                        Double.toString(probabilities.get(0).prob));
            }
            if (probabilities.size() > 1) {
                data.put(HEADERS.LANG_ID2+extension, probabilities.get(1).lang);
                data.put(HEADERS.LANG_ID_PROB2+extension,
                        Double.toString(probabilities.get(1).prob));
            }
            if (probabilities.size() > 2) {
                data.put(HEADERS.LANG_ID3+extension, probabilities.get(2).lang);
                data.put(HEADERS.LANG_ID_PROB3+extension,
                        Double.toString(probabilities.get(2).prob));
            }


        } catch (LangDetectException e) {
            //log
        }
    }

    void handleWordCounts(Map<String, String> data,
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
        }

        data.put(HEADERS.TOP_N_WORDS+extension, sb.toString());
        data.put(HEADERS.NUM_EN_STOPS_TOP_N+extension, Integer.toString(stops));
    }

    Map<String, MutableValueInt> getTokens(String s) throws IOException {
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

    class MutableValueIntPriorityQueue extends PriorityQueue<TermIntPair> {

        MutableValueIntPriorityQueue(int maxSize) {
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

    class TermIntPair {
        final String term;
        final int value;

        TermIntPair(String term, int value) {
            this.term = term;
            this.value = value;
        }
    }

}
