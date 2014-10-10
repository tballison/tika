package org.apache.tika.parser.evil;

import org.apache.tika.batch.ParserFactory;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.parser.Parser;

public class EvilParserFactory implements ParserFactory {
    @Override
    public Parser getParser(TikaConfig config) {
        return new EvilParser();
    }
}
