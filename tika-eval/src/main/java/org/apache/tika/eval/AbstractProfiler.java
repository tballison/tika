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


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Types;
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
import org.apache.tika.batch.fs.FSProperties;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.eval.db.ColInfo;
import org.apache.tika.eval.db.Cols;
import org.apache.tika.eval.db.TableInfo;
import org.apache.tika.eval.io.IDBWriter;
import org.apache.tika.eval.tokens.TokenCounter;
import org.apache.tika.eval.tokens.TokenIntPair;
import org.apache.tika.eval.tokens.TokenStats;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.FilenameUtils;
import org.apache.tika.io.IOUtils;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.apache.tika.parser.RecursiveParserWrapper;
import org.apache.tika.utils.ExceptionUtils;

public abstract class AbstractProfiler extends FileResourceConsumer {

    private static final String[] EXTRACT_EXTENSIONS = {
            ".json",
            ".txt",
            ""
    };

    private static final String[] COMPRESSION_EXTENSIONS = {
            "",
            ".bz2",
            ".gzip",
            ".zip",
    };
    static final String NON_EXISTENT_FILE_LENGTH = "-1";

    public static TableInfo REF_EXTRACT_ERROR_TYPES = new TableInfo("ref_extract_error_types",
            new ColInfo(Cols.EXTRACT_ERROR_TYPE_ID, Types.INTEGER),
            new ColInfo(Cols.EXTRACT_ERROR_DESCRIPTION, Types.VARCHAR, 128)
    );


    public static TableInfo REF_PARSE_ERROR_TYPES = new TableInfo("ref_parse_error_types",
            new ColInfo(Cols.PARSE_ERROR_TYPE_ID, Types.INTEGER),
            new ColInfo(Cols.PARSE_ERROR_DESCRIPTION, Types.VARCHAR, 128)
    );

    public static TableInfo REF_PARSE_EXCEPTION_TYPES = new TableInfo("ref_parse_exception_types",
            new ColInfo(Cols.PARSE_EXCEPTION_TYPE_ID, Types.INTEGER),
            new ColInfo(Cols.PARSE_EXCEPTION_DESCRIPTION, Types.VARCHAR, 128)
    );

    public static final String TRUE = Boolean.toString(true);
    public static final String FALSE = Boolean.toString(false);

    protected static final AtomicInteger CONTAINER_ID = new AtomicInteger();
    protected static final AtomicInteger ID = new AtomicInteger();

    private final static String UNKNOWN_EXTENSION = "unk";
    private final static String DIGEST_KEY = "X-TIKA:digest:MD5";
    private String lastExtractExtension = null;


    public enum EXTRACT_ERROR_TYPE {
        //what do you see when you look at the extract file
        NO_EXTRACT_FILE,
        ZERO_BYTE_EXTRACT_FILE,
        EXTRACT_PARSE_EXCEPTION
    }

    public enum EXCEPTION_TYPE {
        RUNTIME,
        ENCRYPTION,
        ACCESS_PERMISSION,
        UNSUPPORTED_VERSION,
    }

    public enum PARSE_ERROR_TYPE {
        //what was gathered from the log file during the batch run
        OOM,
        TIMEOUT
    }

    public static TableInfo MIME_TABLE = new TableInfo("mimes",
            new ColInfo(Cols.MIME_TYPE_ID, Types.INTEGER, "PRIMARY KEY"),
            new ColInfo(Cols.MIME_STRING, Types.VARCHAR, 256),
            new ColInfo(Cols.FILE_EXTENSION, Types.VARCHAR, 12)
    );



    private static Pattern FILE_NAME_CLEANER = Pattern.compile("\\.json(\\.(bz2|gz|zip))?$");


    final static int FILE_PATH_MAX_LEN = 512;//max len for varchar for file_path
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

    public AbstractProfiler(ArrayBlockingQueue<FileResource> fileQueue,
                            IDBWriter writer) {
        super(fileQueue);
        this.writer = writer;
    }


    protected void writeError(TableInfo extractErrorTable, String containerId,
                              String filePath, File extractA) throws IOException {
        Map<Cols, String> data = new HashMap<>();
        data.put(Cols.CONTAINER_ID, containerId);
        data.put(Cols.FILE_PATH, filePath);
        int errorCode = -1;
        if (extractA == null) {
            errorCode = EXTRACT_ERROR_TYPE.NO_EXTRACT_FILE.ordinal();
        } else if (extractA.length() == 0) {
            errorCode = EXTRACT_ERROR_TYPE.ZERO_BYTE_EXTRACT_FILE.ordinal();
        } else {
            errorCode = EXTRACT_ERROR_TYPE.EXTRACT_PARSE_EXCEPTION.ordinal();
        }
        data.put(Cols.EXTRACT_ERROR_TYPE_ID, Integer.toString(errorCode));
        writer.writeRow(extractErrorTable, data);

    }



    protected void writeProfileData(EvalFilePaths fps, int i, Metadata m,
                                    String fileId, String containerId,
                                    List<Integer> numAttachments, TableInfo profileTable) {

        Map<Cols, String> data = new HashMap<>();
        data.put(Cols.ID, fileId);
        data.put(Cols.CONTAINER_ID, containerId);
        data.put(Cols.MD5, m.get(DIGEST_KEY));

        if ( i < numAttachments.size()) {
            data.put(Cols.NUM_ATTACHMENTS, Integer.toString(numAttachments.get(i)));
        }
        data.put(Cols.ELAPSED_TIME_MILLIS, getTime(m));
        data.put(Cols.NUM_METADATA_VALUES,
                Integer.toString(countMetadataValues(m)));



        //if the outer wrapper document
        if (i == 0) {

            data.put(Cols.IS_EMBEDDED, FALSE);
            data.put(Cols.FILE_NAME, fps.sourceFileName);
            data.put(Cols.FILE_EXTENSION, FilenameUtils.getExtension(fps.sourceFileName));
        } else {
            data.put(Cols.IS_EMBEDDED, TRUE);
            data.put(Cols.FILE_NAME, FilenameUtils.getName(m.get(RecursiveParserWrapper.EMBEDDED_RESOURCE_PATH)));
            data.put(Cols.FILE_EXTENSION,
                    FilenameUtils.getExtension(data.get(Cols.FILE_NAME)));
        }
        data.put(Cols.LENGTH, getSourceFileLength(m));
        int numMetadataValues = countMetadataValues(m);
        data.put(Cols.NUM_METADATA_VALUES,
                Integer.toString(numMetadataValues));

        data.put(Cols.ELAPSED_TIME_MILLIS,
                getTime(m));

        String content = getContent(m, MAX_STRING_LENGTH);
        if (content == null || content.trim().length() == 0) {
            data.put(Cols.HAS_CONTENT, FALSE);
        } else {
            data.put(Cols.HAS_CONTENT, TRUE);
        }
        getFileTypes(m, data);
        try {
            writer.writeRow(profileTable, data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void writeExceptionData(String fileId, Metadata m, TableInfo exceptionTable) {
        Map<Cols, String> data = new HashMap<Cols, String>();
        getExceptionStrings(m, data);
        if (data.keySet().size() > 0) {
            try {
                data.put(Cols.ID, fileId);
                writer.writeRow(exceptionTable, data);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Checks to see if metadata is null or content is empty (null or only whitespace).
     * If any of these, then this does no processing, and the fileId is not
     * entered into the content table.
     *
     * @param fileId
     * @param m
     * @param counter
     * @param contentsTable
     */
    protected void writeContentData(String fileId, Metadata m, TokenCounter counter, TableInfo contentsTable) {
        if (m == null) {
            return;
        }

        String content = getContent(m, MAX_STRING_LENGTH);
        if (content == null || content.trim().length() == 0) {
            return;
        }

        Map<Cols, String> data = new HashMap<Cols, String>();
        data.put(Cols.ID, fileId);
        data.put(Cols.CONTENT_LENGTH, Integer.toString(content.length()));
        try {
            countTokens(m, counter);
            handleWordCounts(data, counter);
        } catch (IOException e) {
            //should log
            e.printStackTrace();
        }
        data.put(Cols.UNIQUE_TOKEN_COUNT,
                Integer.toString(counter.getUniqueTokenCount()));
        data.put(Cols.TOKEN_COUNT,
                Integer.toString(counter.getTokenCount()));

        TokenStats tokenStats = calcTokenStats(counter);

        data.put(Cols.TOKEN_ENTROPY_RATE,
                Double.toString(tokenStats.getEntropy()));
        SummaryStatistics summStats = tokenStats.getSummaryStatistics();
        data.put(Cols.TOKEN_LENGTH_SUM,
                Integer.toString((int) summStats.getSum()));

        data.put(Cols.TOKEN_LENGTH_MEAN,
                Double.toString(summStats.getMean()));

        data.put(Cols.TOKEN_LENGTH_STD_DEV,
                Double.toString(summStats.getStandardDeviation()));



        langid(m, data);
        try {
            writer.writeRow(contentsTable, data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    List<Metadata> getMetadata(File thisFile) {
        List<Metadata> metadataList = null;
        if (thisFile == null || ! thisFile.exists()) {
            return metadataList;
        }
        Reader reader = null;
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
     *
     * @param langModelDir directory that contains the language models
     * @param seed         any seed to assure consistent langid across runs
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

    void getExceptionStrings(Metadata metadata, Map<Cols, String> data) {

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
                data.put(Cols.PARSE_EXCEPTION_TYPE_ID,
                        Integer.toString(EXCEPTION_TYPE.ACCESS_PERMISSION.ordinal()));
                return;
            }
            matcher = ENCRYPTION_EXCEPTION.matcher(fullTrace);
            if (matcher.find()) {
                data.put(Cols.PARSE_EXCEPTION_TYPE_ID,
                        Integer.toString(EXCEPTION_TYPE.ENCRYPTION.ordinal()));
                return;
            }

            data.put(Cols.PARSE_EXCEPTION_TYPE_ID,
                    Integer.toString(EXCEPTION_TYPE.RUNTIME.ordinal()));

            data.put(Cols.ORIG_STACK_TRACE, fullTrace);
            //TikaExceptions can have object ids, as in the "@2b1ea6ee" in:
            //org.apache.tika.exception.TikaException: TIKA-198: Illegal
            //IOException from org.apache.tika.parser.microsoft.OfficeParser@2b1ea6ee
            //For reporting purposes, let's snip off the object id so that we can more
            //easily count exceptions.
            String sortTrace = ExceptionUtils.trimMessage(fullTrace);

            matcher = CAUSED_BY_SNIPPER.matcher(sortTrace);
            sortTrace = matcher.replaceAll("$1");
            sortTrace = sortTrace.replaceAll("org.apache.tika.", "o.a.t.");
            data.put(Cols.SORT_STACK_TRACE, sortTrace);
        }
    }

    protected static String getContent(Metadata metadata, int maxLength) {
        if (metadata == null) {
            return "";
        }
        String c = metadata.get(RecursiveParserWrapper.TIKA_CONTENT);
        if (c == null) {
            return "";
        }
        if (c.length() > maxLength) {
            c = c.substring(0, maxLength);
        }
        return c;
    }


    void langid(Metadata metadata, Map<Cols, String> data) {
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
                data.put(Cols.LANG_ID_1, probabilities.get(0).lang);
                data.put(Cols.LANG_ID_PROB_1,
                        Double.toString(probabilities.get(0).prob));
            }
            if (probabilities.size() > 1) {
                data.put(Cols.LANG_ID_2, probabilities.get(1).lang);
                data.put(Cols.LANG_ID_PROB_2,
                        Double.toString(probabilities.get(1).prob));
            }

        } catch (LangDetectException e) {
            //TODO: log
        }
    }

    void getFileTypes(Metadata metadata, Map<Cols, String> output) {
        if (metadata == null) {
            return;
        }
        String type = metadata.get(Metadata.CONTENT_TYPE);
        if (type == null) {
            return;
        }
        int mimeId = writer.getMimeId(type);
        output.put(Cols.MIME_TYPE_ID, Integer.toString(mimeId));
    }

    void handleWordCounts(Map<Cols, String> data,
                          TokenCounter tokens) {
        MutableValueIntPriorityQueue queue = new MutableValueIntPriorityQueue(TOP_N_WORDS);
        for (String t : tokens.getTokens()) {
            int count = tokens.getCount(t);
            if (count == 0) {
                continue;
            }
            if (queue.top() == null || queue.size() < TOP_N_WORDS ||
                    count >= queue.top().getValue()) {
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
            if (en_stops.contains(t.getToken())) {
                stops++;
            }
        }

        data.put(Cols.TOP_N_WORDS, sb.toString());
        data.put(Cols.NUM_EN_STOPS_TOP_N, Integer.toString(stops));
    }

    void countTokens(final Metadata metadata, final TokenCounter counter) throws IOException {
        if (metadata == null) {
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


    private TokenStats calcTokenStats(TokenCounter counter) {
        double ent = 0.0d;
        double p = 0.0d;
        double base = 2.0;
        int tokenCount = counter.getTokenCount();
        SummaryStatistics summStats = new SummaryStatistics();
        for (String token : counter.getTokens()) {
            int a = counter.getCount(token);
            if (a < 1) {
                //this can happen because of the way that tokens are stored
                continue;
            }
            int len = token.length();
            for (int i = 0; i < a; i++) {
                summStats.addValue(len);
            }
            p = (double) a / (double) tokenCount;
            ent += p * FastMath.log(base, p);
        }

        if (tokenCount > 0) {
            ent = (-1.0d / (double) tokenCount) * ent;
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
            if (arg0.getValue() < arg1.getValue()) {
                return true;
            }
            return false;
        }
    }

    /**
     *
     * @param metadata
     * @param extractDir
     * @return evalfilepaths for files if crawling an extract directory
     */
    protected EvalFilePaths getPathsFromExtractCrawl(Metadata metadata,
                                                     File extractDir) {
        EvalFilePaths fp = new EvalFilePaths();
        String relExtractFilePath = metadata.get(FSProperties.FS_REL_PATH);
        Matcher m = FILE_NAME_CLEANER.matcher(relExtractFilePath);
        fp.relativeSourceFilePath = m.replaceAll("");
        fp.sourceFileName = FilenameUtils.getName(fp.relativeSourceFilePath);
        //just try slapping the relextractfilepath on the extractdir
        fp.extractFile = new File(extractDir, relExtractFilePath);
        if (! fp.extractFile.exists()) {
            //if that doesn't work, try to find the right extract file.
            //This is necessary if crawling extractDirA and trying to find a file in
            //extractDirB that is not in the same format: json vs txt or compressed
            fp.extractFile = findFile(extractDir, fp.relativeSourceFilePath);
        }
        return fp;
    }
    //call this if the crawler is crawling through the src directory
    protected EvalFilePaths getPathsFromSrcCrawl(Metadata metadata, File srcDir,
                                                 File extractDir) {
        EvalFilePaths fp = new EvalFilePaths();
        fp.relativeSourceFilePath = metadata.get(FSProperties.FS_REL_PATH);
        fp.sourceFileName = FilenameUtils.getName(fp.relativeSourceFilePath);
        fp.extractFile = findFile(extractDir, fp.relativeSourceFilePath);
        File inputFile = new File(srcDir, fp.relativeSourceFilePath);
        fp.sourceFileLength = inputFile.length();
        return fp;
    }

    /**
     *
     * @param extractRootDir
     * @param relativeSourceFilePath
     * @return extractFile or null if couldn't find one.
     */
    private File findFile(File extractRootDir, String relativeSourceFilePath) {
        if (lastExtractExtension != null) {
            File candidate = new File(extractRootDir, relativeSourceFilePath+lastExtractExtension);
            if (candidate.exists() && candidate.isFile()) {
                return candidate;
            }
        }
        for (String ext : EXTRACT_EXTENSIONS) {
            for (String compress : COMPRESSION_EXTENSIONS) {
                File candidate = new File(extractRootDir, relativeSourceFilePath+ext+compress);
                if (candidate.exists() && candidate.isFile()) {
                    lastExtractExtension = ext+compress;
                    return candidate;
                }
            }
        }
        return null;
    }

    protected String getSourceFileLength(EvalFilePaths fps, List<Metadata> metadataList) {
        if (fps.sourceFileLength > -1) {
            return Long.toString(fps.sourceFileLength);
        }
        return getSourceFileLength(metadataList);
    }

    String getSourceFileLength(List<Metadata> metadataList) {
        if (metadataList == null || metadataList.size() < 1) {
            return NON_EXISTENT_FILE_LENGTH;
        }
        return getSourceFileLength(metadataList.get(0));
    }

    String getSourceFileLength(Metadata m) {
        String len = m.get(Metadata.CONTENT_LENGTH);
        return (len == null) ? NON_EXISTENT_FILE_LENGTH : len;
    }

    protected String getFileLength(File f) {
        if (f != null && f.exists()) {
            return Long.toString(f.length());
        }
        return NON_EXISTENT_FILE_LENGTH;
    }

    /**
     *
     * @param list
     * @return empty list if input list is empty or null
     */
    static List<Integer> countAttachments(List<Metadata> list) {
        List<Integer> ret = new ArrayList<Integer>();
        if (list == null || list.size() == 0) {
            return ret;
        }
        //container document attachment count = list.size()-1
        ret.add(list.size()-1);

        Map<String, Integer> counts = new HashMap<>();
        for (int i = 1; i < list.size(); i++) {
            String path = list.get(i).get(RecursiveParserWrapper.EMBEDDED_RESOURCE_PATH);
            if (path == null) {
                //shouldn't ever happen
                continue;
            }
            String[] parts = path.split("/");
            StringBuilder parent = new StringBuilder();
            for (int end = 1; end < parts.length-1; end++) {
                parent.setLength(0);
                join("/", parent, parts, 1, end);
                String parentPath = parent.toString();
                Integer count = counts.get(parentPath);
                if (count == null) {
                    count = 1;
                } else {
                    count++;
                }
                counts.put(parentPath, count);
            }
        }

        for (int i = 1; i < list.size(); i++) {
            Integer count = counts.get(list.get(i).get(RecursiveParserWrapper.EMBEDDED_RESOURCE_PATH));
            if (count == null) {
                count = 0;
            }
            ret.add(i, count);
        }
        return ret;


    }

    private static void join(String delimiter, StringBuilder sb, String[] parts, int start, int end) {
        for (int i = start; i <= end; i++) {
            sb.append(delimiter);
            sb.append(parts[i]);
        }
    }
}

