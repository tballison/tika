package org.apache.tika.eval;

import java.io.File;
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
    static List<LanguageProfile> languageProfiles;
    static LanguageDetector detector;
    static TextObjectFactory textObjectFactory;

    public static void loadBuiltInModels() throws IOException {

        languageProfiles = new LanguageProfileReader().readAllBuiltIn();
        detector = LanguageDetectorBuilder.create(NgramExtractors.standard())
                .withProfiles(languageProfiles)
                .build();
        textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText();
    }

    public static void loadModels(File path) throws IOException {

        languageProfiles = new LanguageProfileReader().readAll(path);
        detector = LanguageDetectorBuilder.create(NgramExtractors.standard())
                .withProfiles(languageProfiles)
                .build();
        textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText();
    }



    public static Optional<LdLocale> detect(String s) {
        return detector.detect(textObjectFactory.forText(s));
    }

    public static List<DetectedLanguage> getProbabilities(String s) {
        return detector.getProbabilities(textObjectFactory.forText(s));
    }

}
