package org.apache.tika.eval.tokens;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * Filters tokens by length _unless_ the token
 * is in a Chinese/Japanese/Korean codepoint range.  This allows
 * one to limit the length of whitespace language tokens while
 * letting CJK bigrams pass through.
 */
public class CJKAwareLengthFilterFactory extends TokenFilterFactory {

    private static final String HAN_TYPE = StandardTokenizer.TOKEN_TYPES[StandardTokenizer.IDEOGRAPHIC];
    private static final String HIRAGANA_TYPE = StandardTokenizer.TOKEN_TYPES[StandardTokenizer.HIRAGANA];
    private static final String KATAKANA_TYPE = StandardTokenizer.TOKEN_TYPES[StandardTokenizer.KATAKANA];
    private static final String HANGUL_TYPE = StandardTokenizer.TOKEN_TYPES[StandardTokenizer.HANGUL];



    private final int min;
    private final int max;
    public CJKAwareLengthFilterFactory(Map<String, String> args) {
        super(args);
        min = Integer.parseInt(args.get("min"));
        max = Integer.parseInt(args.get("max"));
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new CJKAwareLengthFilter(tokenStream);
    }

    private class CJKAwareLengthFilter extends FilteringTokenFilter {
        private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
        private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);

        public CJKAwareLengthFilter(TokenStream in) {
            super(in);
        }

        @Override
        protected boolean accept() throws IOException {
            if ( termAtt.length() < min) {
                String type = typeAtt.type();
                if (type == HAN_TYPE || type == HIRAGANA_TYPE || type == KATAKANA_TYPE
                        || type == HANGUL_TYPE) {
                    return termAtt.length() >= max;
                }
            }
            return termAtt.length() >= min && termAtt.length() <= max;
        }
    }

    /*
    private static boolean isCJ(int codePoint) {
        if (
                (codePoint >= 0x4E00 && codePoint <= 0x9FFF) ||
                        ( codePoint >= 0x3400 && codePoint <= 0x4dbf) ||
                        ( codePoint >= 0x20000 && codePoint <= 0x2a6df) ||
                        ( codePoint >= 0x2A700 && codePoint <= 0x2b73f) ||
                        ( codePoint >= 0x2B740 && codePoint <= 0x2B81F) ||
                        ( codePoint >= 0x2B820 && codePoint <- 0x2CEAF) ||
                        ( codePoint >= 0xF900 && codePoint <= 0xFAFF) ||
                        ( codePoint >= 0x2F800 && codePoint <= 0x2Fa1F)
        ) {
            return true;
        }
        return false;
    }*/

}
