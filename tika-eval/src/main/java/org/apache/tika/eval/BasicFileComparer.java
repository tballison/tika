package org.apache.tika.eval;

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
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;
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
            "NUM_TOKENS_"+thisSuffix,
            "NUM_TOKENS_"+thatSuffix,
            "TOKEN_OVERLAP"
        };
    }
    public void setWriter(ThreadSafeCSVWrapper writer) {
        this.writer = writer;
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
        Map<String, Integer> theseTokens = getTokens(content);
        content = getContent(thatMetadata);
        Map<String, Integer> thoseTokens = getTokens(content);

        int denom = theseTokens.size() + thoseTokens.size();
        int overlap = 0;
        for (String k : theseTokens.keySet()) {
            if (thoseTokens.containsKey(k)) {
                overlap += 2;
            }
        }

        float div = (float) overlap / (float) denom;
        data.put("NUM_TOKENS_"+thisSuffix,
                Integer.toString(theseTokens.size()));
        data.put("NUM_TOKENS_"+thatSuffix,
                Integer.toString(thoseTokens.size()));
        data.put("TOKEN_OVERLAP",
                Float.toString(div));
    }

    private Map<String, Integer> getTokens(String s) throws IOException {
        if (s == null || s.equals("")) {
            return new HashMap<String, Integer>();
        }

        Analyzer analyzer = new ICUAnalyzer(Version.LUCENE_4_9, CharArraySet.EMPTY_SET);
        Map<String, Integer> m = new HashMap<String, Integer>();
        TokenStream stream = analyzer.tokenStream("", s);
        stream.reset();
        CharTermAttribute attr = stream.getAttribute(CharTermAttribute.class);
        while (stream.incrementToken()) {
            String t = attr.toString();
            Integer i = m.get(t);
            if (i == null) {
                i = 0;
            }
            m.put(t, ++i);
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
            sb.append(c);
            sb.append("\n\n");
            if (sb.length() > MAX_STRING_LENGTH) {
                sb.setLength(MAX_STRING_LENGTH);
                break;
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

}
