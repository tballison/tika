package org.apache.tika.eval;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.tika.eval.tokens.AnalyzerManager;
import org.junit.Test;

/**
 * Created by TALLISON on 2/8/2017.
 */
public class AnalyzerManagerTest {

    @Test
    public void testGeneral() throws Exception {
        AnalyzerManager analyzerManager = AnalyzerManager.newInstance();
        Analyzer general = analyzerManager.getGeneralAnalyzer();
        TokenStream ts = general.tokenStream("f", "tHe quick aaaa aaa anD dirty dog");
        ts.reset();

        CharTermAttribute termAtt = ts.getAttribute(CharTermAttribute.class);
        Set<String> seen = new HashSet<>();
        while (ts.incrementToken()) {
            seen.add(termAtt.toString());
            System.out.println(termAtt.toString());
        }
        ts.end();
        ts.close();

        assertTrue(seen.contains("the"));
        assertTrue(seen.contains("and"));
        assertTrue(seen.contains("dog"));

    }

    @Test
    public void testCommon() throws Exception {
        AnalyzerManager analyzerManager = AnalyzerManager.newInstance();
        Analyzer common = analyzerManager.getCommonWordAnalyzer();
        TokenStream ts = common.tokenStream("f", "the 5,000.12 and dirty dog");
        ts.reset();
        CharTermAttribute termAtt = ts.getAttribute(CharTermAttribute.class);
        Set<String> seen = new HashSet<>();
        while (ts.incrementToken()) {
            if (termAtt.toString().contains("5")) {
                fail("Shouldn't have found a numeric");
            }
            System.out.println(termAtt.toString());
            seen.add(termAtt.toString());
        }
        ts.end();
        ts.close();

        assertTrue(seen.contains("the"));
        assertTrue(seen.contains("and"));
        assertTrue(seen.contains("dog"));


    }

}
