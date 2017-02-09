package org.apache.tika.eval.tokens;


public class CommonTokenResult {

    private final String langCode;
    private final int tokens;

    public CommonTokenResult(String langCode, int tokens) {
        this.langCode = langCode;
        this.tokens = tokens;
    }

    public String getLangCode() {
        return langCode;
    }

    public int getTokens() {
        return tokens;
    }
}
