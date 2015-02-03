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
import java.sql.Types;
import java.util.ArrayList;
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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.PriorityQueue;
import org.apache.tika.batch.BatchNoRestartError;
import org.apache.tika.batch.FileResource;
import org.apache.tika.batch.FileResourceConsumer;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.eval.db.ColInfo;
import org.apache.tika.eval.io.TableWriter;
import org.apache.tika.eval.tokens.TokenCounter;
import org.apache.tika.eval.tokens.TokenIntPair;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.IOUtils;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.apache.tika.mime.MimeTypeException;
import org.apache.tika.eval.util.MimeUtil;
import org.apache.tika.parser.RecursiveParserWrapper;

public abstract class AbstractProfiler extends FileResourceConsumer {

    private final static String UNKNOWN_EXTENSION = "unk";

    public enum HEADERS {
        FILE_PATH(new ColInfo(-1, Types.VARCHAR, 1024)),
        FILE_LENGTH(new ColInfo(-1, Types.BIGINT)),
        FILE_EXTENSION(new ColInfo(-1, Types.VARCHAR, 12)),
        JSON_EX(new ColInfo(-1, Types.VARCHAR, 1024)),
        ORIG_STACK_TRACE(new ColInfo(-1, Types.VARCHAR, 1024)),
        SORT_STACK_TRACE(new ColInfo(-1, Types.VARCHAR, 1024)),
        DETECTED_CONTENT_TYPE(new ColInfo(-1, Types.VARCHAR, 128)),
        DETECTED_FILE_EXTENSION(new ColInfo(-1, Types.VARCHAR, 32)),
        ELAPSED_TIME_MILLIS(new ColInfo(-1, Types.INTEGER)),
        NUM_METADATA_VALUES(new ColInfo(-1, Types.INTEGER)),
        NUM_ATTACHMENTS(new ColInfo(-1, Types.INTEGER)),
        TOKEN_COUNT(new ColInfo(-1, Types.INTEGER)),
        NUM_UNIQUE_TOKENS(new ColInfo(-1, Types.INTEGER)),
        TOP_N_WORDS(new ColInfo(-1, Types.VARCHAR, 1024)),
        NUM_EN_STOPS_TOP_N(new ColInfo(-1, Types.INTEGER)),
        LANG_ID1(new ColInfo(-1, Types.VARCHAR, 12)),
        LANG_ID_PROB1(new ColInfo(-1, Types.FLOAT)),
        LANG_ID2(new ColInfo(-1, Types.VARCHAR, 12)),
        LANG_ID_PROB2(new ColInfo(-1, Types.FLOAT)),
        LANG_ID3(new ColInfo(-1, Types.VARCHAR, 12)),
        LANG_ID_PROB3(new ColInfo(-1, Types.FLOAT));

        private final ColInfo colInfo;

        HEADERS(ColInfo colInfo) {
            this.colInfo = colInfo;
        }

        protected ColInfo getColInfo() {
            return colInfo;
        }
    };


    final static int MAX_TOKENS = 100000;
    final static int MAX_STRING_LENGTH = 1000000;
    final static int MAX_LEN_FOR_LANG_ID = 20000;
    final static int TOP_N_WORDS = 10;

    //these remove runtime info from the stacktraces so
    //that actual causes can be counted.
    private final static Pattern CAUSED_BY_SNIPPER =
            Pattern.compile("(Caused by: [^:]+):[^\\r\\n]+");
    private final static Pattern OBJECT_ID_SNIPPER =
            Pattern.compile("(?s)^(org.apache.tika.exception.TikaException[^\\r\\n]+?)@[a-f0-9]+(\\s*[\\r\\n].*$)");

    private JsonMetadataList serializer = new JsonMetadataList();
    private TikaConfig config = TikaConfig.getDefaultConfig();//TODO: allow configuration
    protected TableWriter writer;


    public AbstractProfiler(ArrayBlockingQueue<FileResource> fileQueue) {
        super(fileQueue);
    }

    List<Metadata> getMetadata(File thisFile) {
        Reader reader = null;
        List<Metadata> metadataList = new ArrayList<Metadata>();
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
            e.printStackTrace();
        } catch (TikaException e) {
            //log
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(reader);
        }
        return metadataList;
    }

    public void setTableWriter(TableWriter writer) {
        this.writer = writer;
    }

    public static void setLangModelDir(File langModelDir) {
        try {
            DetectorFactory.loadProfile(langModelDir);
        } catch (LangDetectException e) {
            throw new BatchNoRestartError(e);
        }
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
            String exc = m.get(TikaCoreProperties.TIKA_META_EXCEPTION_PREFIX+"runtime");
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
                data.put(HEADERS.SORT_STACK_TRACE + extension, exc);
            }
        }
    }

    protected static String getContent(List<Metadata> metadataList, int maxLength) {
        StringBuilder content = new StringBuilder();
        if (metadataList == null) {
            return "";
        }
        for (Metadata m : metadataList) {
            String c = m.get(RecursiveParserWrapper.TIKA_CONTENT);
            if (c == null) {
                continue;
            }
            int localLen = c.length();
            if (content.length()+localLen > maxLength) {
                int allowedAmount = maxLength-content.length();
                if (allowedAmount <= 0) {
                    return content.toString();
                }
                String substr = c.substring(0,allowedAmount);
                content.append(substr);
                return content.toString();
            } else {
                content.append(c);
            }
        }
        return content.toString();
    }

    void langid(List<Metadata> metadataList, String extension, Map<String, String> data) {
        String content = getContent(metadataList, MAX_LEN_FOR_LANG_ID);
        langid(content, extension, data);
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
            //TODO: log
        }
    }

    void getFileTypes(List<Metadata> metadata, String extension, Map<String, String> output) {
        if (metadata == null || metadata.size() == 0) {
            return;
        }
        String type = metadata.get(0).get(Metadata.CONTENT_TYPE);
        if (type == null) {
            return;
        }
        output.put(HEADERS.DETECTED_CONTENT_TYPE + extension, type);

        try {
            String ext = MimeUtil.getExtension(type, config);
            if (ext == null || ext.length() == 0) {
                ext = UNKNOWN_EXTENSION;
            }
            output.put(HEADERS.DETECTED_FILE_EXTENSION + extension, ext);
        } catch (MimeTypeException e) {
            //swallow
        }

    }

    void handleWordCounts(Map<String, String> data,
                                  TokenCounter tokens, String extension) {
        MutableValueIntPriorityQueue queue = new MutableValueIntPriorityQueue(TOP_N_WORDS);
        for (String t : tokens.getTokens()) {
            int count = tokens.getCount(t);
            if (count == 0) {
                continue;
            }
            if (queue.top() == null || queue.size() < TOP_N_WORDS ||
                    count >= queue.top().getValue()){
                queue.insertWithOverflow(new TokenIntPair(t, count));
            }
        }

        StringBuilder sb = new StringBuilder();
        List<TokenIntPair> terms = new ArrayList<TokenIntPair>();
        //now we reverse the queue
        TokenIntPair term = queue.pop();
        while (term != null) {
            terms.add(0, term);
            term = queue.pop();
        }
        CharArraySet en_stops = StandardAnalyzer.STOP_WORDS_SET;
        int stops = 0;
        int i = 0;
        for (TokenIntPair t : terms) {
            if (i++ > 0) {
                sb.append(" | ");
            }
            sb.append(t.getToken() + ": " + t.getValue());
            if (en_stops.contains(t.getToken())){
                stops++;
            }
        }

        data.put(HEADERS.TOP_N_WORDS+extension, sb.toString());
        data.put(HEADERS.NUM_EN_STOPS_TOP_N+extension, Integer.toString(stops));
    }

    void countTokens(final List<Metadata> metadataList, final TokenCounter counter) throws IOException {
        Analyzer analyzer = new StandardAnalyzer(CharArraySet.EMPTY_SET);
        TokenCounter tokens;
        if (metadataList == null){
            return;
        }
        int contentLength = 0;
        for (Metadata m : metadataList) {
            String content = m.get(RecursiveParserWrapper.TIKA_CONTENT);
            if (content == null) {
                continue;
            }
            contentLength += content.length();
            addTokens(content, analyzer, counter);
            if (contentLength > MAX_STRING_LENGTH || counter.getUniqueTokenCount() > MAX_TOKENS) {
                //log this
                return;
            }
        }
        return;
    }

    void addTokens(String s, Analyzer analyzer,
                                           final TokenCounter counter) throws IOException {
        if (s == null || s.equals("")) {
            return;
        }

        TokenStream stream = analyzer.tokenStream("", s);
        stream.reset();
        CharTermAttribute attr = stream.getAttribute(CharTermAttribute.class);
        while (stream.incrementToken()) {
            String t = attr.toString();
            counter.increment(t);
        }
        stream.end();
        stream.close();
        return;
    }


/*    protected IndexReader index(List<Metadata> metadataList) {
        IndexReader reader = null;
        MemoryIndex index = new MemoryIndex();
        Analyzer analyzer = new ICUAnalyzer(LUCENE_VERSION, CharArraySet.EMPTY_SET);
        for (Metadata m : metadataList) {
            String content = m.get(RecursiveParserWrapper.TIKA_CONTENT);
            if (content != null) {
                index.addField(LUCENE_FIELD, content, analyzer);
            }
        }
        return index.createSearcher().getIndexReader();
    }*/
/*
//if ICU is desired
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
*/
    class MutableValueIntPriorityQueue extends PriorityQueue<TokenIntPair> {

        MutableValueIntPriorityQueue(int maxSize) {
            super(maxSize);

        }

        @Override
        protected boolean lessThan(TokenIntPair arg0, TokenIntPair arg1) {
            if (arg0.getValue() < arg1.getValue()){
                return true;
            }
            return false;
        }
    }

    protected static void addHeader(Map<String, ColInfo> headers, HEADERS header) {
        headers.put(header.name(), new ColInfo(headers.size() + 1,
                header.getColInfo().getType(), header.getColInfo().getPrecision()));
    }



}
