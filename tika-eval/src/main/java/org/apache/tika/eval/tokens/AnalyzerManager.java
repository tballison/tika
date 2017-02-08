package org.apache.tika.eval.tokens;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import org.apache.lucene.analysis.Analyzer;

public class AnalyzerManager {

    private final static String GENERAL = "general";
    private static final String COMMON = "common";

    private final Analyzer generalAnalyzer;
    private final Analyzer alphaAnalyzer;

    private AnalyzerManager(Analyzer generalAnalyzer, Analyzer commonAnalyzer) {
        this.generalAnalyzer = generalAnalyzer;
        this.alphaAnalyzer = commonAnalyzer;
    }

    public static AnalyzerManager newInstance() throws IOException {
        InputStream is = AnalyzerManager.class.getClassLoader().getResourceAsStream("analyzers.json");
        Reader reader = new InputStreamReader(is, "UTF-8");
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeHierarchyAdapter(Map.class, new AnalyzerDeserializer());
        Gson gson = builder.create();
        Map<String, Analyzer> map = gson.fromJson(reader, Map.class);
        Analyzer general = map.get(GENERAL);
        Analyzer common = map.get(COMMON);
        if (general == null) {
            throw new JsonParseException("Must specify "+GENERAL + " analyzer");
        }
        if (common == null) {
            throw new JsonParseException("Must specify "+COMMON + " analyzer");
        }

        return new AnalyzerManager(general, common);
    }

    public Analyzer getGeneralAnalyzer() {
        return generalAnalyzer;
    }

    public Analyzer getCommonWordAnalyzer() {
        return alphaAnalyzer;
    }
}
