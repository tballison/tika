package org.apache.tika.eval;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Optional;
import com.optimaize.langdetect.DetectedLanguage;
import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.CommonTextObjectFactories;
import com.optimaize.langdetect.text.TextObjectFactory;


public class LanguageIDWrapper {
    final LanguageDetector languageDetector;
    final TextObjectFactory textObjectFactory;

    public static LanguageIDWrapper build() throws IOException {
        List<LanguageProfile> languageProfiles = new LanguageProfileReader().readAllBuiltIn();
        LanguageDetector detector = LanguageDetectorBuilder.create(NgramExtractors.standard())
                .withProfiles(languageProfiles)
                .build();
        TextObjectFactory textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText();
        return new LanguageIDWrapper(detector, textObjectFactory);
    }

    private LanguageIDWrapper(LanguageDetector detector, TextObjectFactory textObjectFactory) {
        this.languageDetector = detector;
        this.textObjectFactory = textObjectFactory;
    }

    public Optional<LdLocale> detect(String s) {
        return languageDetector.detect(textObjectFactory.forText(s));
    }

    public List<DetectedLanguage> getProbabilities(String s) {
        return languageDetector.getProbabilities(textObjectFactory.forText(s));
    }

}
