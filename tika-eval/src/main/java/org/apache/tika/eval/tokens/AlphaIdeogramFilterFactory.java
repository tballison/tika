package org.apache.tika.eval.tokens;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * Factory for filter that only allows tokens with characters that "isAlphabetic"  or "isIdeographic" through.
 */
public class AlphaIdeogramFilterFactory extends TokenFilterFactory {



    public AlphaIdeogramFilterFactory(Map<String, String> args) {
        super(args);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new AlphaFilter(tokenStream);
    }

    /**
     * Remove tokens tokens that do not contain an "
     */
    private class AlphaFilter extends FilteringTokenFilter {

        private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

        public AlphaFilter(TokenStream in) {
            super(in);
        }

        @Override
        protected boolean accept() throws IOException {
            char[] buff = termAtt.buffer();
            for (int i = 0; i < termAtt.length(); i++) {
                int cp = buff[i];
                if (Character.isHighSurrogate(buff[i])) {
                    if (i < termAtt.length()-1) {
                        cp = Character.toCodePoint(buff[i], buff[i + 1]);
                        i++;
                    }
                }

                if (Character.isAlphabetic(cp) ||
                        Character.isIdeographic(cp)) {
                    return true;
                }
            }
            return false;
        }
    }
}
