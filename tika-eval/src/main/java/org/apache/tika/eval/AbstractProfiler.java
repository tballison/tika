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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.commons.math3.util.FastMath;
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
import org.apache.tika.eval.io.IDBWriter;
import org.apache.tika.eval.tokens.TokenCounter;
import org.apache.tika.eval.tokens.TokenIntPair;
import org.apache.tika.eval.tokens.TokenStats;
import org.apache.tika.eval.util.MimeUtil;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.IOUtils;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.apache.tika.mime.MimeTypeException;
import org.apache.tika.parser.RecursiveParserWrapper;
import org.apache.tika.utils.ExceptionUtils;

public abstract class AbstractProfiler extends FileResourceConsumer {

    public static final String PAIR_NAMES_TABLE = "pair_names";
    public static final String EXCEPTIONS_TABLE = "exceptions";
    public static final String JSON_PARSE_EXCEPTION = "Json parse exception";

    protected static final AtomicInteger CONTAINER_ID = new AtomicInteger();
    protected static final AtomicInteger ID = new AtomicInteger();

    private final static String UNKNOWN_EXTENSION = "unk";

    public enum EXCEPTION_HEADERS {
        ID(new ColInfo(-1, Types.INTEGER, "PRIMARY KEY")),
        TIMEOUT_EXCEPTION(new ColInfo(-1, Types.BOOLEAN)),
        OOM_ERROR(new ColInfo(-1, Types.BOOLEAN)),
        ORIG_STACK_TRACE(new ColInfo(-1, Types.VARCHAR, 8192)),
        SORT_STACK_TRACE(new ColInfo(-1, Types.VARCHAR, 8192)),
        ENCRYPTED_EXCEPTION(new ColInfo(-1, Types.BOOLEAN)),
        ACCESS_PERMISSION_EXCEPTION(new ColInfo(-1, Types.BOOLEAN));

        private final ColInfo colInfo;

        EXCEPTION_HEADERS(ColInfo colInfo) {
            this.colInfo = colInfo;
        }

        public ColInfo getColInfo() {
            return colInfo;
        }
    }

    public enum HEADERS {
        ID(new ColInfo(-1, Types.INTEGER, "PRIMARY KEY")),
        CONTAINER_ID(new ColInfo(-1, Types.INTEGER)),
        FILE_PATH(new ColInfo(-1, Types.VARCHAR, 1024)),
        FILE_LENGTH(new ColInfo(-1, Types.BIGINT)),
        EMBEDDED_FILE_LENGTH(new ColInfo(-1, Types.BIGINT)),
        IS_EMBEDDED(new ColInfo(-1, Types.BOOLEAN)),
        EMBEDDED_FILE_PATH(new ColInfo(-1, Types.VARCHAR, 1024)),
        JSON_EX(new ColInfo(-1, Types.VARCHAR, 512)),
        JSON_FILE_LENGTH(new ColInfo(-1, Types.BIGINT)),
        FILE_EXTENSION(new ColInfo(-1, Types.VARCHAR, 12)),
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
        LANG_ID_PROB3(new ColInfo(-1, Types.FLOAT)),
        TOKEN_ENTROPY_RATE(new ColInfo(-1, Types.FLOAT)),
        TOKEN_LENGTH_SUM(new ColInfo(-1, Types.INTEGER)),
        TOKEN_LENGTH_MEAN(new ColInfo(-1, Types.FLOAT)),
        TOKEN_LENGTH_STD_DEV(new ColInfo(-1, Types.FLOAT));

        private final ColInfo colInfo;

        HEADERS(ColInfo colInfo) {
            this.colInfo = colInfo;
        }

        protected ColInfo getColInfo() {
            return colInfo;
        }
    }
    private static Pattern FILE_NAME_CLEANER = Pattern.compile("\\.json(\\.(bz2|gz|zip))?$");
    private static Map<String, ColInfo> EXCEPTION_HEADERS_MAP = new HashMap<String, ColInfo>();

    static {
        for (EXCEPTION_HEADERS header : EXCEPTION_HEADERS.values()) {
            EXCEPTION_HEADERS_MAP.put(header.name(), new ColInfo(EXCEPTION_HEADERS_MAP.size() + 1,
                    header.getColInfo().getType(), header.getColInfo().getPrecision()));

        }
        EXCEPTION_HEADERS_MAP = Collections.unmodifiableMap(EXCEPTION_HEADERS_MAP);
    }

    public static Map<String, ColInfo> getExceptionHeaders() {
        return EXCEPTION_HEADERS_MAP;
    }


    final static int MAX_STRING_LENGTH = 1000000;
    final static int MAX_LEN_FOR_LANG_ID = 20000;
    final static int TOP_N_WORDS = 10;

    //these remove runtime info from the stacktraces so
    //that actual causes can be counted.
    private final static Pattern CAUSED_BY_SNIPPER =
            Pattern.compile("(Caused by: [^:]+):[^\\r\\n]+");

    private final static Pattern ACCESS_PERMISSION_EXCEPTION =
            Pattern.compile("org\\.apache\\.tika\\.exception\\.AccessPermissionException");
    private final static Pattern ENCRYPTION_EXCEPTION =
            Pattern.compile("org\\.apache\\.tika.exception\\.EncryptedDocumentException");

    private TikaConfig config = TikaConfig.getDefaultConfig();//TODO: allow configuration
    protected IDBWriter writer;
    private final boolean crawlingInputDir;

    public AbstractProfiler(ArrayBlockingQueue<FileResource> fileQueue, boolean crawlingInputDir,
                            IDBWriter writer) {
        super(fileQueue);
        this.crawlingInputDir = crawlingInputDir;
        this.writer = writer;
    }

    public String getInputFileName(String fName) {
        if (crawlingInputDir) {
            return fName;
        }
        Matcher m = FILE_NAME_CLEANER.matcher(fName);
        return m.replaceAll("");
    }
/*
    public void setContentLength(Metadata metadata, Map<String, String> data) {
        if ("TRUE".equals(data.get(HEADERS.IS_EMBEDDED.name())) {
            String len = data.get(Metadata.CONTENT_LENGTH);
            if (len != null) {
                d
            }
        }
    }
*/
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
            metadataList = JsonMetadataList.fromJson(reader);
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

    /**
     * Initialize cybozu's DetectorFactory
     * @param langModelDir directory that contains the language models
     * @param seed any seed to assure consistent langid across runs
     */
    public static void initLangDetectorFactory(File langModelDir, Long seed) {
        try {
            DetectorFactory.loadProfile(langModelDir);
            if (seed != null) {
                DetectorFactory.setSeed(seed);
            }
        } catch (LangDetectException e) {
            throw new BatchNoRestartError(e);
        }
    }

    String getOriginalFileExtension(String fName) {
        if (fName == null) {
            return "";
        }
        Matcher m = Pattern.compile("\\.([^\\.]+)\\.json(?:\\.(?:bz2|gz(?:ip)?|zip))?$").matcher(fName);
        if (m.find()) {
            return m.group(1);
        }
        return "";
    }

    String getTime(Metadata m) {
        String elapsed = "-1";

        String v = m.get(RecursiveParserWrapper.PARSE_TIME_MILLIS);
        if (v != null) {
            return v;
        }
        return elapsed;
    }


    int countMetadataValues(Metadata m) {
        if (m == null) {
            return 0;
        }
        int i = 0;
        for (String n : m.names()) {
            i += m.getValues(n).length;
        }
        return i;
    }

    void getExceptionStrings(Metadata metadata, Map<String, String> data) {

        String fullTrace = metadata.get(TikaCoreProperties.TIKA_META_EXCEPTION_PREFIX + "runtime");

        if (fullTrace == null) {
            fullTrace = metadata.get(RecursiveParserWrapper.EMBEDDED_EXCEPTION);
        }

        if (fullTrace != null) {
            //check for "expected" exceptions...exceptions
            //that can't be fixed.
            //Do not store trace for "expected" exceptions

            Matcher matcher = ACCESS_PERMISSION_EXCEPTION.matcher(fullTrace);
            if (matcher.find()) {
                data.put(EXCEPTION_HEADERS.ACCESS_PERMISSION_EXCEPTION.name(), "true");
                return;
            }
            matcher = ENCRYPTION_EXCEPTION.matcher(fullTrace);
            if (matcher.find()) {
                data.put(EXCEPTION_HEADERS.ENCRYPTED_EXCEPTION.name(), "true");
                return;
            }

            data.put(EXCEPTION_HEADERS.ORIG_STACK_TRACE.name(), fullTrace);
            //TikaExceptions can have object ids, as in the "@2b1ea6ee" in:
            //org.apache.tika.exception.TikaException: TIKA-198: Illegal
            //IOException from org.apache.tika.parser.microsoft.OfficeParser@2b1ea6ee
            //For reporting purposes, let's snip off the object id so that we can more
            //easily count exceptions.
            String sortTrace = ExceptionUtils.trimMessage(fullTrace);

            matcher = CAUSED_BY_SNIPPER.matcher(sortTrace);
            sortTrace = matcher.replaceAll("$1");
            sortTrace = sortTrace.replaceAll("org.apache.tika.", "o.a.t.");
            data.put(EXCEPTION_HEADERS.SORT_STACK_TRACE.name(), sortTrace);
        }
    }

    protected static String getContent(Metadata metadata, int maxLength) {
        if (metadata == null) {
            return "";
        }
        StringBuilder content = new StringBuilder();
        String c = metadata.get(RecursiveParserWrapper.TIKA_CONTENT);
        if (c == null) {
            return "";
        }
        if (c.length() > maxLength) {
            c = c.substring(0, maxLength);
        }
        return c;
    }


    void langid(Metadata metadata, String extension, Map<String, String> data) {
        String content = getContent(metadata, MAX_LEN_FOR_LANG_ID);
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

    void getFileTypes(Metadata metadata, String extension, Map<String, String> output) {
        if (metadata == null) {
            return;
        }
        String type = metadata.get(Metadata.CONTENT_TYPE);
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
        data.put(HEADERS.NUM_EN_STOPS_TOP_N + extension, Integer.toString(stops));
    }

    void countTokens(final Metadata metadata, final TokenCounter counter) throws IOException {
        if (metadata == null){
            return;
        }
        Analyzer analyzer = new StandardAnalyzer(CharArraySet.EMPTY_SET);
        TokenCounter tokens;
        String content = getContent(metadata, MAX_STRING_LENGTH);
        addTokens(content, analyzer, counter);
        return;
    }

    void addTokens(String s, Analyzer analyzer,
                                           final TokenCounter counter) throws IOException {
        if (s == null || s.equals("")) {
            return;
        }

        TokenStream stream = analyzer.tokenStream("", s);
        stream.reset();
        //look into: http://lucene.apache.org/core/4_10_3/core/index.html
        //TermToBytesRefAttribute
        CharTermAttribute attr = stream.getAttribute(CharTermAttribute.class);

        while (stream.incrementToken()) {
            String t = attr.toString();
            counter.increment(t);
        }
        stream.end();
        stream.close();
        return;
    }

    protected void addSingleFileStats(Metadata metadata, Set<String> tokens,
                                      TokenCounter tokenCounter, String extension, Map<String, String> output) throws IOException {
        if (metadata == null) {
            return;
        }
        handleExceptionStrings(metadata, extension, output);
        getFileTypes(metadata, extension, output);

        int numMetadataValues = countMetadataValues(metadata);
        output.put(HEADERS.NUM_METADATA_VALUES + extension,
                Integer.toString(numMetadataValues));

        output.put(HEADERS.ELAPSED_TIME_MILLIS + extension,
                getTime(metadata));

        langid(metadata, extension, output);
        output.put(HEADERS.NUM_UNIQUE_TOKENS + extension,
                Integer.toString(tokenCounter.getUniqueTokenCount()));

        TokenStats tokenStats = calcTokenStats(tokens, tokenCounter);

        output.put(HEADERS.TOKEN_ENTROPY_RATE + extension,
                Double.toString(tokenStats.getEntropy()));
        SummaryStatistics summStats = tokenStats.getSummaryStatistics();
        output.put(HEADERS.TOKEN_LENGTH_SUM + extension,
                Integer.toString((int) summStats.getSum()));

        output.put(HEADERS.TOKEN_LENGTH_MEAN + extension,
                Double.toString(summStats.getMean()));

        output.put(HEADERS.TOKEN_LENGTH_STD_DEV + extension,
                Double.toString(summStats.getStandardDeviation()));

        output.put(HEADERS.TOKEN_COUNT + extension, Integer.toString(tokenCounter.getTokenCount()));

        handleWordCounts(output, tokenCounter, extension);

    }

    protected void handleExceptionStrings(Metadata metadata,
                                          String extension, Map<String, String> output) throws IOException {
        Map<String, String> excOutput = new HashMap<String, String>();
        getExceptionStrings(metadata, excOutput);
        if(excOutput.size() > 0) {
            excOutput.put(HEADERS.ID.name(), output.get(HEADERS.ID.name()));
            writer.writeRow(FileComparer.EXCEPTIONS_TABLE + extension, excOutput);
        }
    }

    private TokenStats calcTokenStats(Set<String> tokens, TokenCounter counter) {
        double ent = 0.0d;
        double p = 0.0d;
        double base = 2.0;
        int tokenCount = counter.getTokenCount();
        SummaryStatistics summStats = new SummaryStatistics();
        for (String token : tokens) {
            int a = counter.getCount(token);
            if (a < 1) {
                //this can happen because of the way that tokens are stored
                continue;
            }
            int len = token.length();
            for (int i = 0; i < a; i++) {
                summStats.addValue(len);
            }
            p = (double)a/(double)tokenCount;
            ent += p* FastMath.log(base, p);
        }

        if (tokenCount > 0) {
            ent = (-1.0d/(double)tokenCount)*ent;
        }

        return new TokenStats(ent, summStats);

    }

    public void closeWriter() throws IOException {
        writer.close();
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
